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

from dateutil.parser import parse as parse_datetime

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import PureCloudPlatformApiSdk
import PureCloudPlatformClientV2

import tap_purecloud.schemas as schemas
import tap_purecloud.websocket_helper
import time


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


def get_access_token(config):
    "Returns an access_token for the client credentials, or raises if unauthorized"

    client_id = config['client_id']
    client_secret = config['client_secret']
    client_domain = config['domain']

    auth_host = BASE_PURECLOUD_AUTH_HOST.format(domain=client_domain)
    auth_endpoint = '{}/oauth/token'.format(auth_host)

    authorization = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")
    body = {'grant_type': 'client_credentials'}

    headers = {
        'Authorization': f'Basic {authorization}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(auth_endpoint, data=body, headers=headers)

    if response.status_code == HTTP_SUCCESS:
        response_json = response.json()
        return response_json['access_token']
    else:
        logger.fatal(response.json())
        raise RuntimeError("Unauthorized")


class FakeBody(object):
    def __init__(self, page_number=1, page_size=100):
        self.page_number = page_number
        self.page_size = page_size


@backoff.on_exception(backoff.constant,
                      (PureCloudPlatformApiSdk.rest.ApiException),
                      jitter=backoff.random_jitter,
                      max_tries=API_RETRY_COUNT,
                      giveup=giveup,
                      interval=API_RETRY_INTERVAL_SECONDS)

def fetch_one_page(get_records, body, entity_name, api_function_params):
    if isinstance(body, FakeBody):
        logger.info("Fetching {} records from page {}".format(body.page_size, body.page_number))
        response = get_records(page_size=body.page_size, page_number=body.page_number, **api_function_params)
    elif hasattr(body, 'page_size'):
        logger.info("Fetching {} records from page {}".format(body.page_size, body.page_number))
        response = get_records(body, **api_function_params)
    elif hasattr(body, 'paging'):
        logger.info("Fetching {} records from page {}".format(body.paging['pageSize'], body.paging['pageNumber']))
        response = get_records(body, **api_function_params)
    else:
        raise RuntimeError("Unknown body passed to request: {}".format(body))

    if hasattr(response, entity_name):
        results = getattr(response, entity_name)
    elif entity_name in response:
        results = response[entity_name]
    else:
        results = []

    if results is None:
        return response, []
    else:
        return response, results


def should_continue(api_response, body, entity_name):
    records = getattr(api_response, entity_name, [])

    if records is None or len(records) == 0:
        return False
    elif hasattr(api_response, 'page_count') and body.page_number >= api_response.page_count:
        return False
    else:
        return True


def fetch_all_records(get_records, entity_name, body, api_function_params=None, max_pages=None):
    if api_function_params is None:
        api_function_params = {}

    body.page_size = 100
    body.page_number = 1

    api_response, results = fetch_one_page(get_records, body, entity_name, api_function_params)
    yield results

    while should_continue(api_response, body, entity_name) and body.page_number != max_pages:
        body.page_number += 1

        api_response, results = fetch_one_page(get_records, body, entity_name, api_function_params)
        yield results


def fetch_all_analytics_records(get_records, body, entity_name, max_pages=None):
    api_function_params = {}

    body.paging = {
        "pageSize": 100,
        "pageNumber": 1
    }

    api_response, results = fetch_one_page(get_records, body, entity_name, api_function_params)
    yield results

    while results is not None and len(results) > 0 and body.paging['pageNumber'] != max_pages:
        body.paging['pageNumber'] += 1
        api_response, results = fetch_one_page(get_records, body, entity_name, api_function_params)
        yield results


def parse_dates(record):
    parsed = record.copy()
    for (k,v) in record.items():
        if isinstance(v, datetime.datetime):
            parsed[k] = v.isoformat()
    return parsed


def handle_object(obj):
    return parse_dates(obj.to_dict())


def stream_results(generator, transform_record, record_name, schema, primary_key, write_schema):
    all_records = []
    if write_schema:
        singer.write_schema(record_name, schema, primary_key)
    for page in generator:
        if isinstance(page, dict):
            records = [transform_record(k, v) for (k,v) in page.items()]
        else:
            records = [transform_record(record) for record in page]
        valid_records = [r for r in records if r is not None]
        singer.write_records(record_name, valid_records)
        all_records.extend(valid_records)
    return all_records


def stream_results_list(generator, transform_record, record_name, schema, primary_key, write_schema):
    if write_schema:
        singer.write_schema(record_name, schema, primary_key)

    for page in generator:
        records_list = [transform_record(record) for record in page]
        for records in records_list:
            singer.write_records(record_name, records)


def sync_users(config):
    logger.info("Fetching users")
    api_instance = PureCloudPlatformApiSdk.UsersApi()
    body = FakeBody()
    gen_users = fetch_all_records(api_instance.get_users, 'entities', body, {'expand': ['locations']})
    stream_results(gen_users, handle_object, 'users', schemas.user, ['id'], True)


def sync_groups(config):
    logger.info("Fetching groups")
    api_instance = PureCloudPlatformApiSdk.GroupsApi()
    body = FakeBody()
    gen_groups = fetch_all_records(api_instance.get_groups, 'entities', body)
    stream_results(gen_groups, handle_object, 'groups', schemas.group, ['id'], True)


def sync_locations(config):
    logger.info("Fetching locations")
    api_instance = PureCloudPlatformApiSdk.LocationsApi()
    body = PureCloudPlatformApiSdk.LocationSearchRequest()
    gen_locations = fetch_all_records(api_instance.post_search, 'results', body)
    stream_results(gen_locations, handle_object, 'location', schemas.location, ['id'], True)


def sync_presence_definitions(config):
    logger.info("Fetching presence definitions")
    api_instance = PureCloudPlatformApiSdk.PresenceApi()
    body = FakeBody()
    gen_presences = fetch_all_records(api_instance.get_presencedefinitions, 'entities', body)
    stream_results(gen_presences, handle_object, 'presence', schemas.presence, ['id'], True)


def sync_queues(config):
    logger.info("Fetching queues")
    api_instance = PureCloudPlatformApiSdk.RoutingApi()
    body = FakeBody()
    gen_queues = fetch_all_records(api_instance.get_queues, 'entities', body)

    queues = stream_results(gen_queues, handle_object, 'queues', schemas.queue, ['id'], True)

    for i, queue in enumerate(queues):
        first_page = (i == 0)
        queue_id = queue['id']

        getter = lambda *args, **kwargs: api_instance.get_queues_queue_id_users(queue_id)
        gen_queue_membership = fetch_all_records(getter, 'entities', FakeBody())
        stream_results(gen_queue_membership, handle_queue_user_membership(queue_id), 'queue_membership', schemas.queue_membership, ['id'], first_page)

        getter = lambda *args, **kwargs: api_instance.get_queues_queue_id_wrapupcodes(queue_id)
        gen_queue_wrapup_codes = fetch_all_records(getter, 'entities', FakeBody())
        stream_results(gen_queue_wrapup_codes, handle_queue_wrapup_code(queue_id), 'queue_wrapup_code', schemas.queue_wrapup, ['id'], first_page)



def get_wfm_units_for_broken_sdk(api_instance):
    def wrap(*args, **kwargs):
        _ = api_instance.get_managementunits(*args, **kwargs)
        return json.loads(api_instance.api_client.last_response.data)
    return wrap


def handle_activity_codes(unit_id):
    def wrap(activity_code_id, activity_code):
        activity_code = activity_code.to_dict()
        activity_code['id'] = activity_code_id
        activity_code['management_unit_id'] = unit_id
        return activity_code
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

def sync_user_schedules(config, unit_id, user_ids, first_page):
    logger.info("Fetching user schedules")
    api_instance = PureCloudPlatformApiSdk.WorkforceManagementApi()

    sync_date: 'datetime.date' = config['start_date']
    lookahead_weeks = config.get('schedule_lookahead_weeks', DEFAULT_SCHEDULE_LOOKAHEAD_WEEKS)
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(weeks=lookahead_weeks)
    incr = datetime.timedelta(days=1)

    while sync_date < end_date:
        logger.info("Syncing for {}".format(sync_date))
        next_date = sync_date + incr

        start_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')

        body = PureCloudPlatformApiSdk.UserListScheduleRequestBody()
        body.user_ids = user_ids
        body.start_date = start_date_s
        body.end_date = end_date_s

        getter = lambda *args, **kwargs: api_instance.post_managementunits_mu_id_schedules_search(unit_id, body=body)
        gen_schedules = fetch_all_analytics_records(getter, body, 'user_schedules', max_pages=1)

        stream_results(gen_schedules, handle_schedule(start_date_s), 'user_schedule', schemas.user_schedule, ['start_date', 'user_id'], first_page)

        sync_date = next_date
        first_page = False


def sync_wfm_historical_adherence(config, unit_id, users, body):
    # The ol' Python pass-by-reference
    result_reference = {}
    wfm_notifcation_thread = tap_purecloud.websocket_helper.get_historical_adherence(config, result_reference)

    # give the webhook a chance to get settled
    logger.info("Waiting for websocket to settle")
    time.sleep(3)

    logger.info("POSTING adherence request")
    api_instance = PureCloudPlatformClientV2.WorkforceManagementApi()
    wfm_response = api_instance.post_workforcemanagement_managementunit_historicaladherencequery(
            unit_id, body=body)

    logger.info("Waiting for notification")
    wfm_notifcation_thread.join()

    url = result_reference['downloadUrl']
    response = requests.get(url).json()
    yield response['data']


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


def sync_historical_adherence(config, unit_id, users, first_page):

    sync_date: 'datetime.date' = config['start_date']

    end_date: 'datetime.date' = datetime.date.today()
    incr = datetime.timedelta(days=1)

    sync_date = sync_date - incr

    while sync_date < end_date:
        logger.info("Syncing historical adherence for {}".format(sync_date))
        next_date = sync_date + incr

        start_date_s = sync_date.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date_s = next_date.strftime('%Y-%m-%dT00:00:00.000Z')

        body = PureCloudPlatformClientV2.WfmHistoricalAdherenceQuery()
        body.start_date = start_date_s
        body.end_date = end_date_s
        body.user_ids = users
        body.include_exceptions = True
        body.time_zone = "UTC"

        gen_adherence = sync_wfm_historical_adherence(config, unit_id, users, body)
        stream_results(gen_adherence, handle_adherence(unit_id), 'historical_adherence', schemas.historical_adherence, ['userId', 'management_unit_id', 'startDate'], first_page)

        sync_date = next_date
        first_page = False

def sync_management_units(config):
    logger.info("Fetching management units")
    api_instance = PureCloudPlatformApiSdk.WorkforceManagementApi()
    body = FakeBody()
    getter = get_wfm_units_for_broken_sdk(api_instance)
    gen_units = fetch_all_records(getter, 'entities', body)

    # first, write out the units
    mgmt_units = stream_results(gen_units, lambda x: x, 'management_unit', schemas.management_unit, ['id'], True)

    for i, unit in enumerate(mgmt_units):
        logger.info("Syncing mgmt unit {} of {}".format(i + 1, len(mgmt_units)))
        first_page = (i == 0)
        unit_id = unit['id']

        # don't allow args here
        getter = lambda *args, **kwargs: api_instance.get_managementunits_mu_id_activitycodes(unit_id)
        gen_activitycodes = fetch_all_records(getter, 'activity_codes', FakeBody(), max_pages=1)
        stream_results(gen_activitycodes, handle_activity_codes(unit_id), 'activity_code', schemas.activity_code, ['id', 'management_unit_id'], first_page)

        # don't allow args here
        getter = lambda *args, **kwargs: api_instance.get_managementunits_mu_id_users(unit_id)
        gen_users = fetch_all_records(getter, 'entities', FakeBody(), max_pages=1)
        users = stream_results(gen_users, handle_mgmt_users(unit_id), 'management_unit_users', schemas.management_unit_users, ['user_id', 'management_unit_id'], first_page)

        user_ids = [user['user_id'] for user in users]
        sync_user_schedules(config, unit_id, user_ids, first_page)

        unit_users = get_user_unit_mapping(users)
        sync_historical_adherence(config, unit_id, unit_users[unit_id], first_page)


def handle_conversation(conversation_record):
    conversation = handle_object(conversation_record)

    participants = []
    for participant_record in conversation_record.participants:
        participants.append(handle_object(participant_record))

        sessions = []
        for session_record in participant_record.sessions:
            sessions.append(handle_object(session_record))

            segments = []
            for segment_record in session_record.segments:
                segments.append(handle_object(segment_record))

            sessions[-1]['segments'] = segments
        participants[-1]['sessions'] = sessions
    conversation['participants'] = participants

    return conversation


def sync_conversations(config):
    logger.info("Fetching conversations")
    api_instance = PureCloudPlatformApiSdk.ConversationsApi()

    sync_date: 'datetime.date' = config['start_date']
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(days=1)
    incr = datetime.timedelta(days=1)

    first_page = True
    while sync_date < end_date:
        logger.info("Syncing for {}".format(sync_date))
        next_date = sync_date + incr
        interval = '{}/{}'.format(
            sync_date.strftime('%Y-%m-%dT00:00:00.000Z'),
            next_date.strftime('%Y-%m-%dT00:00:00.000Z')
        )

        body = PureCloudPlatformApiSdk.ConversationQuery()
        body.interval = interval
        body.order = "asc"
        body.orderBy = "conversationStart"

        gen_conversations = fetch_all_analytics_records(api_instance.post_conversations_details_query, body, 'conversations')
        stream_results(gen_conversations, handle_conversation, 'conversation', schemas.conversation, ['conversation_id'], first_page)

        sync_date = next_date
        first_page = False


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


def sync_user_details(config):
    logger.info("Fetching user details")
    api_instance = PureCloudPlatformApiSdk.UsersApi()

    sync_date: 'datetime.date' = config['start_date']
    end_date: 'datetime.date' = datetime.date.today() + datetime.timedelta(days=1)
    incr = datetime.timedelta(days=1)

    first_page = True
    while sync_date < end_date:
        logger.info("Syncing for {}".format(sync_date))
        next_date = sync_date + incr
        interval = '{}/{}'.format(
            sync_date.strftime('%Y-%m-%dT00:00:00.000Z'),
            next_date.strftime('%Y-%m-%dT00:00:00.000Z')
        )

        body = PureCloudPlatformApiSdk.UserDetailsQuery()
        body.interval = interval
        body.order = "asc"


        gen_user_details = fetch_all_analytics_records(api_instance.post_users_details_query, body, 'user_details')
        stream_results_list(gen_user_details, handle_user_details, 'user_state', schemas.user_state, ['id'], first_page)
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
    logger.info("Starting sync.")

    config: dict = args.config
    state: dict = args.state

    # grab start date from state file. If not found
    # default to value in config file

    if 'start_date' in state:
        start_date = parse_to_date(state['start_date'])
    else:
        start_date = parse_to_date(config['start_date'])

    logger.info("Syncing data from: {}".format(start_date))

    config['start_date'] = start_date

    logger.info("Getting access token")
    access_token = get_access_token(config)

    api_host = 'https://api.{domain}'.format(domain=config['domain'])
    PureCloudPlatformApiSdk.configuration.host = api_host
    PureCloudPlatformApiSdk.configuration.access_token = access_token

    PureCloudPlatformClientV2.configuration.host = api_host
    PureCloudPlatformClientV2.configuration.access_token = access_token

    sync_users(config)
    sync_groups(config)
    sync_locations(config)
    sync_presence_definitions(config)
    sync_queues(config)

    sync_management_units(config)
    sync_conversations(config)
    sync_user_details(config)

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
