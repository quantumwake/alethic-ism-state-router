import json
import os
import dotenv
from core.base_message_router import Router
from core.base_message_provider import BaseMessagingConsumer
from core.base_model import ProcessorStateDirection, ProcessorStatusCode
from core.pulsar_message_producer_provider import PulsarMessagingProducerProvider
from core.pulsar_messaging_provider import PulsarMessagingConsumerProvider
from db.processor_state_db_storage import PostgresDatabaseStorage
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

# state storage using postgres engine as backend
storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

# pulsar messaging provider is used, the routes are defined in the routing.yaml
pulsar_provider = PulsarMessagingProducerProvider()


# routing of data to specific processing selectors.
# 1. query state inputs are consumed,
# 2. processor state associations are fetched as per state id direction = input
# 3. providers are selected per processors tate and query state (entry) is published
router = Router(
    provider=pulsar_provider,
    yaml_file="routing.yaml"
)

# find the monitor route for telemetry updates
monitor_route = router.find_router("processor/monitor")

# define the consumer subsystem to use, in this case we are using pulsar but we can also use kafka
messaging_provider = PulsarMessagingConsumerProvider(
    message_url=MSG_URL,
    message_topic=MSG_TOPIC,
    message_topic_subscription=MSG_TOPIC_SUBSCRIPTION,
    management_topic=MSG_MANAGE_TOPIC
)


class MessagingStateRouterConsumer(BaseMessagingConsumer):

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.ROUTE)

    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.ROUTED)

    async def execute(self, message: dict):
        if 'type' not in message:
            raise ValueError(f'inbound message "{message}" did not have a type')

        type = message['type']

        # TODO combine
        if 'query_state_entry' == type:
            await self.execute_query_state(message)
        elif 'query_state_complete' == type:
            await self.execute_forward_state(message)
        else:
            raise ValueError('query state value does not exist in message envelope')

    async def execute_query_state(self, message: dict):

        if 'input_state_id' not in message:
            raise ValueError(f'input_state_id does not exist in message envelope {message}')

        # fetch data elements from message body
        input_state_id = message['input_state_id']
        query_state = message['query_state']

        # fetch the processors to forward the state query to, state must be an input of the state id
        forwarding_processors = storage.fetch_processor_state(
            state_id=input_state_id,
            direction=ProcessorStateDirection.INPUT
        )

        # ensure there are processors that can handle this query state entry/set
        if not forwarding_processors:
            raise LookupError(f'state {input_state_id} is not connected to any processors as inputs, '
                              f'ensure a processor state input state assocation exists for this '
                              f'state id ')

        # find processors and processor providers
        # TODO should cache these using a caching layer on the storage engine
        for processor_state in forwarding_processors:
            logging.debug(f'fetching processor id {processor_state.processor_id} provider details')
            processor = storage.fetch_processor(processor_id=processor_state.processor_id)
            provider_id = processor.provider_id
            logging.debug(f'fetching route for provider id {provider_id}')
            route = router.find_router(selector=provider_id)

            # check whether route was foundx
            if route:
                logging.debug(f'forwarding state_id: {input_state_id} query state entry '
                              f'to route identified by {route.selector}, '
                              f'topic: {route.topic}')

                processor_message = {
                    "type": "query_state",
                    "input_state_id": input_state_id,
                    "output_state_id": None,
                    "processor_id": processor.id,
                    "provider_id": processor.provider_id,
                    "query_state": [query_state]
                }
                processor_message_str = json.dumps(processor_message)

                route.send_message(msg=processor_message_str)
            else:
                raise LookupError(f'unable to find message route to forward state_id: {input_state_id}, '
                                  f'query state entry {query_state} for provider id: {provider_id}')

    async def execute_forward_state(self, message: dict):

        if 'input_state_id' not in message:
            raise ValueError(f'input_state_id does not exist in message envelope {message}')

        if 'processor_id' not in message:
            raise ValueError(f'processor id does not exist in message envelope {message}')

        input_state_id = message['input_state_id']
        processor_id = message['processor_id']

        # fetch processor states
        processor_states = storage.fetch_processor_state(
            processor_id=processor_id,
            state_id=input_state_id,
            direction=ProcessorStateDirection.INPUT
        )

        if not processor_states:
            raise ValueError(f'invalid number of states returned expected 1 input state received None, with '
                             f'processor_id: {processor_id},'
                             f'state_id: {input_state_id},'
                             f'direction: INPUT')

        # only one processing state should exist
        processor_state = processor_states[0]

        # we can now submit individual transactions into the processing queue/topic
        # TODO batch load and submit this in blocks instead of the entire thing, for now its fine :~).
        loaded_state = storage.load_state(state_id=input_state_id, load_data=True)

        #
        logging.info(f'pushing state {input_state_id} entries starting from current position: {processor_state.current_index}, '
                     f'having previously processed maximum index: {processor_state.maximum_index}, which will be reprocessed '
                     f'due to most current index. ')

        # fetch the target processor these state entries will be processed through.
        processor = storage.fetch_processor(processor_id=processor_state.processor_id)

        # find the route for given processor provider id
        route = router.find_router(processor.provider_id)

        # update the processor state to reflect the current loaded state object and last known position, if any
        processor_state.count = loaded_state.count
        processor_state.current_index = processor_state.current_index if processor_state.current_index else -1
        processor_state.maximum_index = processor_state.maximum_index if processor_state.maximum_index else -1
        processor_state = storage.insert_processor_state(processor_state=processor_state)

        start_index = processor_state.current_index if processor_state.current_index > 0 else 0
        end_index = processor_state.count

        # this is common across all query messages forwarded to the processor
        base_processor_message = {
            "type": "query_state",
            "input_state_id": input_state_id,
            "processor_id": processor.id,
            "provider_id": processor.provider_id,

            # TODO clean this up? this should always be present
            "user_id": message['user_id'] if 'user_id' in message else None,
            "project_id": message['project_id'] if 'project_id' in message else None
        }

        # if there is no data then send an empty query state to the identified route
        if start_index >= end_index:
            route.send_message(msg=json.dumps({
                    **base_processor_message,
                    "query_state": []
                })
            )
        else:   # otherwise iterate each of the input states from the state set and submit as a block of inputs
            # TODO send as a whole batch or split into multiple mini-batches (?defined by the processor conf?)
            # iterate each row of the loaded state and submit for processing to its associated processor endpoint.
            for index in range(start_index, end_index):
                query_state_entry = loaded_state.build_query_state_from_row_data(index=index)
                logging.debug(f'processing query state index: {index}, given query state entry: {query_state_entry}')
                route.send_message(msg=json.dumps({
                    **base_processor_message,
                    "query_state": [query_state_entry]
                }))


if __name__ == '__main__':
    monitor_route = router.find_router('processor/monitor')

    consumer = MessagingStateRouterConsumer(
        name="MessagingStateRouterConsumer",
        storage=storage,
        messaging_provider=messaging_provider,
        monitor_route=monitor_route
    )

    consumer.setup_shutdown_signal()
    consumer.start_topic_consumer()
