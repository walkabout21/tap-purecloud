#!/usr/bin/env python3

import argparse
import requests
import logging
import base64
import datetime
import time
import json
import backoff
import hashlib
import collections
import os

from typing import Any, Dict, Tuple
from dateutil.parser import parse as parse_datetime

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from PureCloudPlatformClientV2 import (
    UsersApi, UserDetailsQuery,
    GroupsApi,
    LocationsApi, LocationSearchRequest,
    PresenceApi,
    RoutingApi,
    WorkforceManagementApi, WfmHistoricalAdherenceQuery, UserListScheduleRequestBody,
    ConversationsApi, ConversationQuery,
)
from PureCloudPlatformClientV2.api_client import ApiClient
from PureCloudPlatformClientV2.rest import ApiException as PureCloudApiException

import tap_purecloud.schemas as schemas
from tap_purecloud.websocket_helper import get_historical_adherence
from tap_purecloud.util import safe_json_serialize_deserialize, handle_and_filter_page

logger = singer.get_logger()

HTTP_SUCCESS = 200
HTTP_RATE_LIMIT_ERROR = 429
API_RETRY_INTERVAL_SECONDS = 30
API_RETRY_COUNT = 5
BASE_PURECLOUD_AUTH_HOST = 'https://login.{domain}'
BASE_PURECLOUD_API_HOST = 'https://api.{domain}'
DEFAULT_SCHEDULE_LOOKAHEAD_WEEKS = 5

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'domain',
    'client_id',
    'client_secret',
]

def giveup(error):
    logger.warning("Encountered an error while syncing")
    logger.error(error)

    is_api_error = hasattr(error, 'status')
    is_rate_limit_error = error.status == HTTP_RATE_LIMIT_ERROR

    logger.debug("Is API Error? {}. Is Rate Limit Error? {}.".format(is_api_error, is_rate_limit_error))

    #  return true if we should *not* retry
    should_retry = is_api_error and is_rate_limit_error
    return not should_retry


class FakeBody(object):
    def __init__(self, page_number=1, page_size=100):
        self.page_number = page_number
        self.page_size = page_size


@backoff.on_exception(backoff.constant,
                      (PureCloudApiException),
                      jitter=backoff.random_jitter,
                      max_tries=API_RETRY_COUNT,
                      giveup=giveup,
                      interval=API_RETRY_INTERVAL_SECONDS)

def fetch_one_page(get_records, body, entity_names: tuple, api_function_params) -> Tuple[Any, Dict[str, list]]:
    if isinstance(body, FakeBody):
        # logger.info("Fetching {} records from page {}".format(body.page_size, body.page_number))
        response = get_records(page_size=body.page_size, page_number=body.page_number, **api_function_params)
    elif hasattr(body, 'page_size'):
        # logger.info("Fetching {} records from page {}".format(body.page_size, body.page_number))
        response = get_records(body, **api_function_params)
    elif hasattr(body, 'paging'):
        # logger.info("Fetching {} records from page {}".format(body.paging['pageSize'], body.paging['pageNumber']))
        response = get_records(body, **api_function_params)
    else:
        raise RuntimeError("Unknown body passed to request: {}".format(body))

    results = {}
    for entity_name in entity_names:
        if hasattr(response, entity_name):
            results[entity_name] = getattr(response, entity_name) or []
        elif entity_name in response:
            results[entity_name] = response[entity_name] or []
        else:
            results[entity_name] = []

    return response, results


def should_continue(api_response, body: FakeBody, entity_names: tuple) -> bool:
    has_data_for_entities = []
    for entity_name in entity_names:
        records = getattr(api_response, entity_name, [])
        if not records:
            has_data_for_entities.append(False)
        elif hasattr(api_response, 'page_count') and body.page_number >= api_response.page_count:
            has_data_for_entities.append(False)
        else:
            has_data_for_entities.append(True)
    # Some entities might not have data, but others might so check that atleast one entity
    # has data so that we can continue to fetch more
    return any(has_data_for_entities)


def fetch_all_records(get_records, entity_names: tuple, body, api_function_params=None, max_pages=None) -> Dict[str, list]:
    if api_function_params is None:
        api_function_params = {}

    body.page_size = 100
    body.page_number = 1

    api_response, results = fetch_one_page(get_records, body, entity_names, api_function_params)
    yield results

    while should_continue(api_response, body, entity_names) and body.page_number != max_pages:
        body.page_number += 1
        api_response, results = fetch_one_page(get_records, body, entity_names, api_function_params)
        yield results


def fetch_all_analytics_records(get_records, body, entity_names: tuple, max_pages=None) -> Dict[str, list]:
    api_function_params = {}

    body.paging = {
        "pageSize": 100, # Limit to 100 as the page size can't be larger
        "pageNumber": 1
    }

    api_response, results = fetch_one_page(get_records, body, entity_names, api_function_params)
    yield results

    # Also check that there is some entity that has data to continue
    while any(len(results[entity_name]) > 0 for entity_name in entity_names) and body.paging['pageNumber'] != max_pages:
        body.paging['pageNumber'] += 1
        api_response, results = fetch_one_page(get_records, body, entity_names, api_function_params)
        yield results


def parse_dates(record: dict) -> dict:
    parsed = record.copy()
    for (k,v) in record.items():
        if isinstance(v, datetime.datetime):
            parsed[k] = v.isoformat()
    return parsed


def handle_object(obj: dict) -> dict:
    return parse_dates(obj.to_dict())


def stream_results(generator, entity_names: tuple, transform_record, record_name, schema, primary_key, write_schema):
    all_records = []
    if write_schema:
        singer.write_schema(record_name, schema, primary_key)
    for page in generator:
        for entity_name in entity_names:
            if not entity_name:
                continue
            entity_page = page[entity_name]
            if isinstance(entity_page, dict):
                records = [transform_record(k, v) for (k,v) in entity_page.items()]
            else:
                records = [transform_record(record) for record in entity_page]
            valid_records = [r for r in records if r is not None]
            singer.write_records(record_name, valid_records)
            all_records.extend(valid_records)
    return all_records


def stream_results_list(generator, entity_names: tuple, transform_record, record_name, schema, primary_key, write_schema):
    if write_schema:
        singer.write_schema(record_name, schema, primary_key)

    for page in generator:
        for entity_name in entity_names:
            if not entity_name:
                continue
            entity_page = page[entity_name]
            records_list = [transform_record(record) for record in entity_page]
            for records in records_list:
                singer.write_records(record_name, records)


def sync_users(api_client: ApiClient):
    logger.info("Fetching users")
    api_instance = UsersApi(api_client)
    body = FakeBody()
    entity_names = ('entities', )
    gen_users = fetch_all_records(api_instance.get_users, entity_names, body, {'expand': ['locations']})
    stream_results(gen_users, entity_names, handle_object, 'users', schemas.user, ['id'], True)


def sync_groups(api_client: ApiClient):
    logger.info("Fetching groups")
    api_instance = GroupsApi(api_client)
    body = FakeBody()
    entity_names = ('entities', )
    gen_groups = fetch_all_records(api_instance.get_groups, entity_names, body)
    stream_results(gen_groups, entity_names, handle_object, 'groups', schemas.group, ['id'], True)


def sync_locations(api_client: ApiClient):
    logger.info("Fetching locations")
    api_instance = LocationsApi(api_client)
    body = LocationSearchRequest()
    entity_names = ('results', )
    gen_locations = fetch_all_records(api_instance.post_locations_search, entity_names, body)
    stream_results(gen_locations, entity_names, handle_object, 'location', schemas.location, ['id'], True)


def sync_presence_definitions(api_client: ApiClient):
    logger.info("Fetching presence definitions")
    api_instance = PresenceApi(api_client)
    body = FakeBody()
    entity_names = ('entities', )
    gen_presences = fetch_all_records(api_instance.get_presencedefinitions, entity_names, body)
    stream_results(gen_presences, entity_names, handle_object, 'presence', schemas.presence, ['id'], True)


def sync_queues(api_client: ApiClient):
    logger.info("Fetching queues")
    api_instance = RoutingApi(api_client)
    body = FakeBody()
    entity_names = ('entities', )
    gen_queues = fetch_all_records(api_instance.get_routing_queues, entity_names, body)

    queues = stream_results(gen_queues, entity_names, handle_object, 'queues', schemas.queue, ['id'], True)

    for i, queue in enumerate(queues):
        first_page = (i == 0)
        queue_id = queue['id']

        getter = lambda *args, **kwargs: api_instance.get_routing_queue_users(queue_id)
        gen_queue_membership = fetch_all_records(getter, entity_names, FakeBody())
        stream_results(gen_queue_membership, entity_names, handle_queue_user_membership(queue_id), 'queue_membership', schemas.queue_membership, ['id'], first_page)

        getter = lambda *args, **kwargs: api_instance.get_routing_queue_wrapupcodes(queue_id)
        gen_queue_wrapup_codes = fetch_all_records(getter, entity_names, FakeBody())
        stream_results(gen_queue_wrapup_codes, entity_names, handle_queue_wrapup_code(queue_id), 'queue_wrapup_code', schemas.queue_wrapup, ['id'], first_page)



def get_wfm_units_for_broken_sdk(api_instance: WorkforceManagementApi):
    def wrap(*args, **kwargs):
        _ = api_instance.get_workforcemanagement_managementunits(*args, **kwargs)
        return json.loads(api_instance.api_client.last_response.data)
    return wrap


def handle_activity_codes(unit_id):
    def wrap(activity_code_id, activity_code):
        activity_code = activity_code.to_dict()
        activity_code['id'] = activity_code_id
        activity_code['management_unit_id'] = unit_id
        return safe_json_serialize_deserialize(activity_code)
    return wrap


def handle_mgmt_users(unit_id):
    def wrap(user):
        return {
            'user_id': user.id,
            'management_unit_id': unit_id,
        }
    return wrap

def handle_queue_user_membership(queue_id):
    def wrap(queue_membership):
        membership_info = handle_object(queue_membership)
        user = membership_info.pop('user')

        membership_info['user_id'] = user['id']
        membership_info['queue_id'] = queue_id

        return membership_info
    return wrap

def handle_queue_wrapup_code(queue_id):
    def wrap(queue_wrapup_code):
        wrapup_code = handle_object(queue_wrapup_code)
        wrapup_code['queue_id'] = queue_id

        return wrapup_code
    return wrap

def handle_schedule(start_date):
    def wrap(user_id, user_record):
        schedule = {
            'start_date': start_date,
            'user_id': user_id
        }

        if len(user_record.shifts) == 0:
            return None

        shifts = [parse_dates(shift.to_dict()) for shift in user_record.shifts]
        for shift in shifts:
            parsed_activities = [parse_dates(activity) for activity in shift['activities']]
            shift['activities'] = parsed_activities

        schedule['shifts'] = shifts
        return schedule

    return wrap

def sync_user_schedules(api_instance: WorkforceManagementApi, config, unit_id, user_ids, first_page):
    logger.info("Fetching user schedules")

    sync_date: 'datetime.date' = config['_sync_date']
    lookahead_weeks = config.get('schedule_lookahead_weeks', DEFAULT_SCHEDULE_LOOKAHEAD_WEEKS)
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(weeks=lookahead_weeks)
    incr = datetime.timedelta(days=1)
    entity_names = ('user_schedules', )

    while sync_date < end_date:
        next_date = sync_date + incr
        logger.info(f"Syncing user schedules between {sync_date} and {next_date}")

        start_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')

        body = UserListScheduleRequestBody()
        body.user_ids = user_ids
        body.start_date = start_date_s
        body.end_date = end_date_s

        getter = lambda *args, **kwargs: api_instance.post_workforcemanagement_managementunit_schedules_search(unit_id, body=body)
        gen_schedules = fetch_all_analytics_records(getter, body, entity_names, max_pages=1)

        stream_results(gen_schedules, entity_names, handle_schedule(start_date_s), 'user_schedule', schemas.user_schedule, ['start_date', 'user_id'], first_page)

        sync_date = next_date
        first_page = False


def sync_wfm_historical_adherence(api_instance: WorkforceManagementApi, config, unit_id, users, body):
    # The ol' Python pass-by-reference
    result_reference = {}
    api_client = api_instance.api_client
    wfm_notifcation_thread = get_historical_adherence(api_client, config, result_reference)

    # give the webhook a chance to get settled
    logger.info("Waiting for websocket to settle")
    time.sleep(3)

    logger.info("POSTING adherence request")
    wfm_response = api_instance.post_workforcemanagement_managementunit_historicaladherencequery(
            unit_id, body=body)

    logger.info("Waiting for notification")
    wfm_notifcation_thread.join()

    # downloadUrl is deprecated, use downloadUrls
    # https://developer.genesys.cloud/devapps/sdk/docexplorer/purecloudpython/WfmHistoricalAdherenceResponse
    download_urls = result_reference.get('downloadUrls', [])
        
    for download_url in download_urls:
        resp = requests.get(download_url, timeout=15)
        json_resp = resp.json()
        yield json_resp


def handle_adherence(unit_id):
    def handle(record):
        record['management_unit_id'] = unit_id
        return parse_dates(record)
    return handle

def get_user_unit_mapping(users):
    unit_users = collections.defaultdict(list)
    for item in users:
        user_id = item['user_id']
        management_unit_id = item['management_unit_id']
        unit_users[management_unit_id].append(user_id)
    return unit_users


def sync_historical_adherence(api_instance: WorkforceManagementApi, config, unit_id, users, first_page):

    sync_date: 'datetime.date' = config['_sync_date']

    end_date: 'datetime.date' = datetime.date.today()
    incr = datetime.timedelta(days=1)

    sync_date = sync_date - incr

    while sync_date < end_date:
        logger.info("Syncing historical adherence for {}".format(sync_date))
        next_date = sync_date + incr

        start_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')

        body = WfmHistoricalAdherenceQuery()
        body.start_date = start_date_s
        body.end_date = end_date_s
        body.user_ids = users
        body.include_exceptions = True
        body.time_zone = "UTC"

        gen_adherence = sync_wfm_historical_adherence(api_instance, config, unit_id, users, body)
        stream_results(
            gen_adherence, ('data', ), handle_adherence(unit_id), 'historical_adherence',
            schemas.historical_adherence, ['userId', 'management_unit_id', 'startDate'], first_page
        )

        sync_date = next_date
        first_page = False

def sync_management_units(api_client: ApiClient, config):
    logger.info("Fetching management units")
    api_instance = WorkforceManagementApi(api_client)
    body = FakeBody()
    getter = get_wfm_units_for_broken_sdk(api_instance)
    gen_units = fetch_all_records(getter, ('entities', ), body)

    # first, write out the units
    mgmt_units = stream_results(gen_units, ('entities', ), lambda x: x, 'management_unit', schemas.management_unit, ['id'], True)

    for i, unit in enumerate(mgmt_units):
        logger.info("Syncing mgmt unit {} of {}".format(i + 1, len(mgmt_units)))
        first_page = (i == 0)
        unit_id = unit['id']

        # don't allow args here
        getter = lambda *args, **kwargs: api_instance.get_workforcemanagement_managementunit_activitycodes(unit_id)
        gen_activitycodes = fetch_all_records(getter, ('activity_codes', ), FakeBody(), max_pages=1)
        stream_results(gen_activitycodes, ('activity_codes', ), handle_activity_codes(unit_id), 'activity_code', schemas.activity_code, ['id', 'management_unit_id'], first_page)

        # don't allow args here
        getter = lambda *args, **kwargs: api_instance.get_workforcemanagement_managementunit_users(unit_id)
        gen_users = fetch_all_records(getter, ('entities', ), FakeBody(page_size=1000), max_pages=1)
        users = stream_results(gen_users, ('entities', ), handle_mgmt_users(unit_id), 'management_unit_users', schemas.management_unit_users, ['user_id', 'management_unit_id'], first_page)

        if not users:
            # Without any users, we can't sync any schedules or get any historical adherence data
            # https://developer.genesys.cloud/forum/t/missing-download-url-from-api-response/12656/3
            logger.info(f"Cannot sync user_schedules or historical_adherence, skipping: no users for management_unit_id {unit_id}")
            continue

        user_ids = [user['user_id'] for user in users]
        sync_user_schedules(api_instance, config, unit_id, user_ids, first_page)

        unit_users = get_user_unit_mapping(users)
        sync_historical_adherence(api_instance, config, unit_id, unit_users[unit_id], first_page)


def sync_conversations(api_client: ApiClient, config):
    logger.info("Fetching conversations")
    api_instance = ConversationsApi(api_client)

    sync_date: 'datetime.date' = config['_sync_date']
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(days=1)
    incr = datetime.timedelta(days=1)

    first_page = True
    while sync_date < end_date:
        next_date = sync_date + incr
        logger.info(f"Syncing conversations between {sync_date} and {next_date}")
        sync_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        next_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')
        interval = '{}/{}'.format(sync_date_s, next_date_s)

        body = ConversationQuery()
        body.interval = interval
        body.order = "asc"
        body.orderBy = "conversationStart"

        gen_conversations = fetch_all_analytics_records(
            api_instance.post_analytics_conversations_details_query, body, ('aggregations', 'conversations')
        )

        for page_num, page in enumerate(gen_conversations, start=1):
            aggregations = handle_and_filter_page(page['aggregations'], handle_object) # Aggregations is always empty?
            conversations = handle_and_filter_page(page['conversations'], handle_object)

            # Handle deeply nested objects by separating them into different tables, keyed by the parent identifier
            for i, conversation in enumerate(conversations):
                conversation_id = conversation['conversation_id']
                if first_page and i == 0:
                    singer.write_schema('conversation', schemas.conversation, ['conversation_id'])

                participants = conversation.pop('participants', [])
                for j, participant in enumerate(participants):
                    participant_id = participant['participant_id']
                    participant['conversation_id'] = conversation_id
                    write_conversation_participant_schema = first_page and i == 0 and j == 0
                    if write_conversation_participant_schema:
                        singer.write_schema(
                            'conversation_participant',
                            schemas.conversation_participant,
                            ['conversation_id', 'participant_id']
                        )

                    sessions = participant.pop('sessions', [])
                    singer.write_record('conversation_participant', participant)
                    for k, session in enumerate(sessions):
                        session_id = session['session_id']
                        session['conversation_id'] = conversation_id
                        session['participant_id'] = participant_id
                        write_conversation_participant_session_schema = (
                            first_page and i == 0 and j == 0 and k == 0
                        )
                        if write_conversation_participant_schema:
                            singer.write_schema(
                                'conversation_participant_session',
                                schemas.conversation_participant_session,
                                ['conversation_id', 'participant_id', 'session_id']
                            )

                        segments = session.pop('segments', [])
                        singer.write_record('conversation_participant_session', session)
                        for l, segment in enumerate(segments):
                            segment['conversation_id'] = conversation_id
                            segment['participant_id'] = participant_id
                            segment['session_id'] = session_id

                            write_conversation_participant_session_segment_schema = (
                                first_page and i == 0 and j == 0 and k == 0 and l == 0
                            )
                            if write_conversation_participant_session_segment_schema:
                                singer.write_schema(
                                    'conversation_participant_session_segment',
                                    schemas.conversation_participant_session_segment,
                                    ['conversation_id', 'participant_id', 'session_id', 'segment_end']
                                )
                            singer.write_record('conversation_participant_session_segment', segment)

                evaluations = conversation.pop('evaluations', []) or []
                for j, evaluation in enumerate(evaluations):
                    logger.info(f"has evaluations {conversation_id}")
                    pass # TODO https://developer.genesys.cloud/devapps/sdk/docexplorer/purecloudpython/AnalyticsConversationWithoutAttributes

                resolutions = conversation.pop('resolutions', []) or []
                for j, resolution in enumerate(resolutions):
                    logger.info(f"has resolutions {resolution}")
                    pass # TODO
                singer.write_record('conversation', conversation)
                
            first_page = False

        sync_date = next_date

def md5(s):
    hasher = hashlib.md5()
    hasher.update(s.encode('utf-8'))
    return hasher.hexdigest()


def handle_user_presences(user_details_record):
    presences = []

    if not user_details_record.primary_presence:
        return presences

    for presence in user_details_record.primary_presence:
        pres_dict = parse_dates(presence.to_dict())
        pres_dict['user_id'] = user_details_record.user_id
        pres_dict['id'] = md5("{}-{}".format(pres_dict['start_time'], pres_dict['user_id']))
        presences.append({
            'id': pres_dict['id'],
            'user_id': pres_dict['user_id'],
            'start_time': pres_dict['start_time'],
            'end_time': pres_dict['end_time'],
            'state': pres_dict['system_presence'],
            'state_id': pres_dict['organization_presence_id'],
            'type': 'presence'
        })

    return presences


def handle_user_routing_statuses(user_details_record):
    statuses = []

    if not user_details_record.routing_status:
        return statuses

    for status in user_details_record.routing_status:
        status_dict = parse_dates(status.to_dict())
        status_dict['user_id'] = user_details_record.user_id
        status_dict['id'] = md5("{}-{}".format(status_dict['start_time'], status_dict['user_id']))
        statuses.append({
            'id': status_dict['id'],
            'user_id': status_dict['user_id'],
            'start_time': status_dict['start_time'],
            'end_time': status_dict['end_time'],
            'state': status_dict['routing_status'],
            'type': 'routing_status'
        })

    return statuses


def handle_user_details(user_details_record):
    user_details = user_details_record.to_dict()

    presences = handle_user_presences(user_details_record)
    statuses = handle_user_routing_statuses(user_details_record)

    return presences + statuses


def sync_user_details(api_client: ApiClient, config):
    logger.info("Fetching user details")
    api_instance = UsersApi(api_client)

    sync_date: 'datetime.date' = config['_sync_date']
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(days=1)
    incr = datetime.timedelta(days=1)

    first_page = True
    entity_names = ('user_details', )
    while sync_date < end_date:
        next_date = sync_date + incr
        logger.info(f"Syncing user details between {sync_date} and {next_date}")
        sync_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        next_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')
        interval = '{}/{}'.format(sync_date_s, next_date_s)

        body = UserDetailsQuery()
        body.interval = interval
        body.order = "asc"


        gen_user_details = fetch_all_analytics_records(api_instance.post_analytics_users_details_query, body, entity_names)
        stream_results_list(gen_user_details, entity_names, handle_user_details, 'user_state', schemas.user_state, ['id'], first_page)
        sync_date = next_date

        first_page = False

def parse_to_date(date_string: str) -> 'datetime.date':
    """
    Tries to parse the date_string to date
    Otherwise raises a TypeError or ValueError
    """
    try:
        # Arbitrarily chosen, just needs to be type datetime.date
        # as successful conversion will replace these
        temp_date = datetime.date(1, 1, 1)
        return parse_datetime(date_string, default=temp_date)
    except (TypeError, ValueError):
        pass
    # Failing the conversion to date means it could be datetime instead
    as_datetime = parse_datetime(date_string)
    return as_datetime.date()

def do_sync(args):    
    config: dict = args.config
    state: dict = args.state

    # grab start date from state file. If not found
    # default to value in config file

    if 'start_date' in state:
        start_date = parse_to_date(state['start_date'])
    else:
        start_date = parse_to_date(config['start_date'])
    
    # Use a different key since mutating values can be hard to track
    config['_sync_date'] = start_date

    api_host = f"https://api.{config['domain']}"
    api_client = ApiClient(host=api_host)

    # Sets the access_token in the api_client
    logger.info("Getting access token")
    api_client = api_client.get_client_credentials_token(
        client_id=config['client_id'],
        client_secret=config['client_secret']
    )

    logger.info(f"Successfully got access token. Starting sync from {start_date}")
    
    # https://developer.genesys.cloud/devapps/sdk/docexplorer/purecloudpython/
    sync_users(api_client)
    sync_groups(api_client)
    sync_locations(api_client)
    sync_presence_definitions(api_client)
    sync_queues(api_client)

    sync_management_units(api_client, config)
    sync_conversations(api_client, config)
    sync_user_details(api_client, config)

    new_state = {
        'start_date': datetime.date.today().strftime('%Y-%m-%d')
    }

    singer.write_state(new_state)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> dict:
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = os.path.join(get_abs_path('schemas'), filename)
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        key_properties = ['id']
        valid_replication_keys = None
        stream_metadata = metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=key_properties,
            valid_replication_keys=valid_replication_keys,
            replication_method=None
        )
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

@utils.handle_top_exception(logger)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if parsed_args.discover:
        catalog = discover()
        catalog.dump()
    else:
        do_sync(parsed_args)

if __name__ == '__main__':
    main()
