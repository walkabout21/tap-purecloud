# For fetching historical adherence
import asyncio
import websockets
import threading
import json

from PureCloudPlatformClientV2 import (
    NotificationsApi, ChannelTopic,
)

import singer
logger = singer.get_logger()

MAX_TRIES = 12
WEBHOOK_WAIT = 5
ADHERENCE_CHANNEL = 'v2.users.{}.workforcemanagement.historicaladherencequery'


async def get_websocket_msg(uri):
    async with websockets.connect(uri) as websocket:
        for i in range(MAX_TRIES):
            resp = await websocket.recv()
            data = json.loads(resp)
            body = data.get('eventBody', {})

            logger.info("Got websocket data")

            if body.get('id'):
                return body

        raise RuntimeError("Did not find expected message")


def get_historical_adherence(api_client, config, result_reference):
    notif_api_instance = NotificationsApi(api_client)
    api_response = notif_api_instance.post_notifications_channels()

    client_id = config['client_id']
    channel_id = ADHERENCE_CHANNEL.format(client_id)

    topic = ChannelTopic()
    topic.id = channel_id

    notif_api_instance.post_notifications_channel_subscriptions(api_response.id, [topic])

    def loop_in_thread(loop, websocket_uri, res):
        asyncio.set_event_loop(loop)
        func = get_websocket_msg(websocket_uri)
        val = loop.run_until_complete(func)
        res.update(val)

    websocket_uri = api_response.connect_uri
    loop = asyncio.get_event_loop()

    thread = threading.Thread(target=loop_in_thread, args=(loop, websocket_uri, result_reference))
    thread.start()
    return thread
