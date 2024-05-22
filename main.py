import json
import os
import signal
import sys

import dotenv
import pulsar
import asyncio

from core.base_message_consumer import BaseMessagingConsumer
from core.base_message_router import Route, Router
from core.base_model import ProcessorStateDirection
from core.pulsar_messaging_provider import PulsarMessagingProvider
from db.processor_state_db_storage import ProcessorStateDatabaseStorage, PostgresDatabaseStorage
from pydantic import ValidationError
from logger import logging

dotenv.load_dotenv()
logging.info('starting up pulsar consumer for state routing')

# pulsar/kafka related
MSG_URL = os.environ.get("MSG__URL", "pulsar://localhost:6650")
MSG_TOPIC = os.environ.get("MSG_TOPIC", "ism_state_router")
MSG_MANAGE_TOPIC = os.environ.get("MSG_MANAGE_TOPIC", "ism_state_router_manage")
MSG_TOPIC_SUBSCRIPTION = os.environ.get("MSG_TOPIC_SUBSCRIPTION", "ism_state_router_subscription")

# database related
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# flag that determines whether to shut down the consumers
RUNNING = True

# consumer config
client = pulsar.Client(MSG_URL)
state_router_consumer = client.subscribe(MSG_TOPIC, MSG_TOPIC_SUBSCRIPTION)

# state storage using postgres engine as backend
state_storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

# routing of data to specific processing selectors.
# 1. query state inputs are consumed,
# 2. processor state associations are fetched as per state id direction = input
# 3. providers are selected per processors tate and query state (entry) is published
router = Router(
    yaml_file="routing.yaml"
)

# find the monitor route for telemetry updates
monitor_route = router.find_router("processor/monitor")



messaging_provider = PulsarMessagingProvider(
    message_url=MSG_URL,
    message_topic=MSG_TOPIC,
    message_topic_subscription=MSG_TOPIC_SUBSCRIPTION,
    management_topic=MSG_MANAGE_TOPIC
)

class MessagingConsumerMonitor(BaseMessagingConsumer):

    async def execute(self, message: dict):
        pass
_

def close(consumer):
    consumer.close()


async def execute_query_state(message: dict):

    if 'state_id' not in message:
        raise ValidationError('state id does not exist in message envelope')

    # fetch data elements from message body
    state_id = message['state_id']
    query_state = message['query_state']

    # fetch the processors to forward the state query to, state must be an input of the state id
    forwarding_processors = state_storage.fetch_processor_state(state_id=state_id,
                                                                direction=ProcessorStateDirection.INPUT)

    # ensure there are processors that can handle this query state entry/set
    if not forwarding_processors:
        raise LookupError(f'state {state_id} is not connected to any processors as inputs, '
                          f'ensure a processor state input state assocation exists for this '
                          f'state id ')

    # find processors and processor providers
    # TODO should cache these using a caching layer on the storage engine
    for processor_state in forwarding_processors:
        logging.debug(f'fetching processor id {processor_state.processor_id} provider details')
        processor = state_storage.fetch_processor(processor_id=processor_state.processor_id)
        provider_id = processor.provider_id
        logging.debug(f'fetching route for provider id {provider_id}')
        route = router.find_router(selector=provider_id)

        # check whether route was foundx
        if route:
            logging.debug(f'forwarding state_id: {state_id} query state entry '
                          f'to route identified by {route.selector}, '
                          f'topic: {route.topic}')

            processor_message = {
                "type": "query_state",
                "input_state_id": state_id,
                "output_state_id": None,
                "processor_id": processor.id,
                "provider_id": processor.provider_id,
                "query_state": [query_state]
            }
            processor_message_str = json.dumps(processor_message)

            route.send_message(msg=processor_message_str)
        else:
            raise LookupError(f'unable to find message route to forward state_id: {state_id}, '
                              f'query state entry {query_state} for provider id: {provider_id}')


async def execute_forward_state(message: dict):

    if 'state_id' not in message:
        raise ValidationError('state id does not exist in message envelope')

    if 'processor_id' not in message:
        raise ValidationError('processor id does not exist in message envelope')

    state_id = message['state_id']
    processor_id = message['processor_id']

    # fetch processor states
    processor_states = state_storage.fetch_processor_state(
        processor_id=processor_id,
        state_id=state_id,
        direction=ProcessorStateDirection.INPUT
    )

    if not processor_states:
        raise ValidationError(f'invalid number of states returned expected 1 input state received None, with '
                              f'processor_id: {processor_id},'
                              f'state_id: {state_id},'
                              f'direction: INPUT')

    # only one processing state should exist
    processor_state = processor_states[0]

    # we can now submit individual transactions into the processing queue/topic
    # TODO batch load and submit this in blocks instead of the entire thing, for now its fine :~).
    loaded_state = state_storage.load_state(state_id=state_id, load_data=True)

    #
    logging.info(f'pushing state {state_id} entries starting from current position: {processor_state.current_index}, '
                 f'having previously processed maximum index: {processor_state.maximum_index}, which will be reprocessed '
                 f'due to most current index. ')

    # fetch the target processor these state entries will be processed through.
    processor = state_storage.fetch_processor(processor_id=processor_state.processor_id)

    # find the route for given processor provider id
    route = router.find_router(processor.provider_id)

    # update the processor state to reflect the current loaded state object and last known position, if any
    processor_state.count = loaded_state.count
    processor_state.current_index = processor_state.current_index if processor_state.current_index else -1
    processor_state.maximum_index = processor_state.maximum_index if processor_state.maximum_index else -1
    processor_state = state_storage.insert_processor_state(processor_state=processor_state)

    start_index = processor_state.current_index if processor_state.current_index > 0 else 0
    end_index = processor_state.count


    # iterate each row of the loaded state and submit for processing to its associated processor endpoint.
    for index in range(start_index, end_index):
        query_state_entry = loaded_state.build_query_state_from_row_data(index=index)
        logging.debug(f'processing query state index: {index}, given query state entry: {query_state_entry}')

        processor_message = {
            "type": "query_state",
            "input_state_id": state_id,
            "processor_id": processor.id,
            "provider_id": processor.provider_id,
            "query_state": [query_state_entry]
        }
        processor_message_str = json.dumps(processor_message)
        route.send_message(msg=processor_message_str)


async def execute(data: str):
    # deserialize data dictionary
    message = json.loads(data)

    if 'type' not in message:
        raise ValidationError(f'inbound message "{message}" did not have a type')

    type = message['type']

    if 'query_state' == type:
        await execute_query_state(message)
    elif 'forward_state' == type:
        await execute_forward_state(message)
    else:
        raise ValidationError('query state value does not exist in message envelope')


async def state_router_topic_consumer():
    while RUNNING:
        try:
            msg = state_router_consumer.receive()
            data = msg.data().decode("utf-8")
            logging.info(f'Message received with {data}')
            logging.debug(f'received state data to be routed: {data}')

            # if processor_state.status in [ProcessorStatus.QUEUED, ProcessorStatus.RUNNING]:
            await execute(data=data)
            # else:
            #     logging.error(f'status not in QUEUED, unable to processor state: {processor_state}  ')

            # send ack that the message was consumed.
            state_router_consumer.acknowledge(msg)

        except pulsar.Interrupted:
            logging.error("Stop receiving messages")
            break
        except ValidationError as e:
            # it is safe to assume that if we get a validation error, there is a problem with the json object
            # TODO throw into an exception log or trace it such that we can see it on a dashboard
            state_router_consumer.acknowledge(msg)
            logging.error(f"Message validation error: {e} on asset data {data}")
        except Exception as e:
            state_router_consumer.acknowledge(msg)
            # TODO need to send this to a dashboard, all excdptions in consumers need to be sent to a dashboard
            logging.error(f"An error occurred: {e} on asset data {data}")


def graceful_shutdown(signum, frame):
    global RUNNING
    print("Received SIGTERM signal. Gracefully shutting down.")
    RUNNING = False

    sys.exit(0)


# Attach the SIGTERM signal handler
signal.signal(signal.SIGTERM, graceful_shutdown)

if __name__ == '__main__':
    asyncio.run(state_router_topic_consumer())
