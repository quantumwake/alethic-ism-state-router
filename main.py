import asyncio
import json
import os
import random

import dotenv
from core.base_model import ProcessorStateDirection, ProcessorStatusCode
from core.errors import RouteNotFoundError
from core.messaging.base_message_provider import BaseMessageConsumer
from core.messaging.base_message_route_model import BaseRoute
from core.messaging.base_message_router import Router
from core.messaging.nats_message_provider import NATSMessageProvider
from db.processor_state_db_storage import PostgresDatabaseStorage
from logger import logging

dotenv.load_dotenv()
logging.info('starting up pulsar consumer for state routing')

# database related
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# Message Routing File (
#   The responsibility of this state router is to take inputs and
#   route them to the appropriate destination, as defined by the
#   route selector
# )
ROUTING_FILE = os.environ.get("ROUTING_FILE", '.routing.yaml')
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# state storage using postgres engine as backend
storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

# pulsar messaging provider is used, the routes are defined in the routing.yaml
messaging_provider = NATSMessageProvider()

# routing of data to specific processing selectors.
# 1. query state inputs are consumed,
# 2. processor state associations are fetched as per state id direction = input
# 3. providers are selected per processors tate and query state (entry) is published
router = Router(
    provider=messaging_provider,
    yaml_file=ROUTING_FILE
)

# find the monitor route for telemetry updates
monitor_route = router.find_route("processor/monitor")
state_router_route = router.find_route("processor/state/router")


class MessagingStateRouterConsumer(BaseMessageConsumer):

    def __init__(self, route: BaseRoute, monitor_route: BaseRoute = None, **kwargs):
        super().__init__(route=route, monitor_route=monitor_route)

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.ROUTED)

    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        # WE REMOVED ROUTED BECAUSE IT SEEMS TO BE COMPETING WITH THE PROCESSORS RUNNING/COMPLETED STATE
        # INSTEAD WE GO DIRECTLY INTO A ROUTED STATE AND THEN WHEN THE PROCESSOR PICKS IT UP IT WILL GO
        # INTO A RUNNING STATE, OR FAILED STATE, OR COMPLETED STATE
        # IF THERE IS ANY ERROR SENDING THE DATA OVER TO THE PROCESSOR, WE HANDLE A FAILED STATE IN THIS CONSUMER
        pass


    async def execute(self, message: dict):
        if 'type' not in message:
            raise ValueError(f'inbound message "{message}" did not have a type')

        type = message['type']

        if 'query_state_entry' == type:
            await self.execute_query_state_entry(message)
        elif 'query_state_route' == type:
            await self.execute_processor_state_route(message)
        else:
            raise ValueError('query state value does not exist in message envelope')

    async def execute_query_state_entry(self, message: dict):
        if 'route_id' not in message:
            raise ValueError(f'route_id does not exist in message envelope {message}')

        if 'query_state' not in message:
            raise ValueError(f'no query_state entries found in message envelop {message}')

        # get the route this message is ingress on
        route_id = message['route_id']

        # fetch data elements from message body
        query_state = message['query_state']
        if not isinstance(query_state, list):
            query_state = [query_state]

        # fetch the processors to forward the state query to
        forward_processor_state = storage.fetch_processor_state_route_by_route_id(route_id=route_id)

        if not forward_processor_state:
            raise ValueError(f'processor state not found for route_id: {route_id}')

        # find processors and processor providers
        # TODO should cache these using a caching layer on the storage engine
        logging.debug(f'fetching processor id {forward_processor_state.processor_id} provider details')
        processor = storage.fetch_processor(processor_id=forward_processor_state.processor_id)
        provider_id = processor.provider_id

        # find the forwarding route provider on the association of the processor given input state
        logging.debug(f'fetching provider id {provider_id}, route: {route_id}')
        route = router.find_route(selector=provider_id)

        if not route:  # check whether route was found
            raise ValueError(f"unable to find route provider: {provider_id}, route: {route_id}")

        # otherwise we found the route to forward the message to
        logging.debug(f'sending query state entry to route provider: {route.selector}, route: {route_id}')
        route_message = {
            "type": "query_state",
            "route_id": route_id,
            "query_state": query_state,
            "context": message['context'] if 'context' in message else {}
        }

        processor_message_str = json.dumps(route_message)
        await route.publish(msg=processor_message_str)

    async def execute_processor_state_route(self, message: dict):

        # if 'input_state_id' not in message:
        #     raise ValueError(f'input_state_id does not exist in message envelope {message}')
        #
        # if 'processor_id' not in message:
        #     raise ValueError(f'processor id does not exist in message envelope {message}')
        # input_state_id = message['input_state_id']
        # processor_id = message['processor_id']

        if 'route_id' not in message:
            raise ValueError(f'route id does not exist in message envelope {message}')

        route_id = message['route_id']
        routes = storage.fetch_processor_state_route(message['route_id'])

        if not routes:
            raise ValueError(f'invalid route: {route_id}, not found')

        # TODO: might have to rethink this?
        # For now, only a single route can be executed as an input, how the processor handles is a separate problem??
        if len(routes) != 1:
            raise ValueError(f'invalid number of routes. expected 1, got {len(routes)}')

        # only one processing state should exist
        processor_state_route = routes[0]

        # we can now submit individual transactions into the processing queue/topic
        # TODO batch load and submit this in blocks instead of the entire thing, for now its fine :~).
        loaded_state = storage.load_state(state_id=processor_state_route.state_id, load_data=True)

        #
        logging.info(f'execute route {route_id}, at position: {processor_state_route.current_index}, '
                     f'maximum processed index: {processor_state_route.maximum_index}, '
                     f'up for reprocessing')

        # update the processor state to reflect the current loaded state object and last known position, if any
        route_count = loaded_state.count
        processor_state_route.current_index = processor_state_route.current_index if processor_state_route.current_index else -1
        processor_state_route.maximum_index = processor_state_route.maximum_index if processor_state_route.maximum_index else -1
        ## TODO *** CRITICAL ** THIS MIGHT NEED TO GO INTO THE PROCESSOR STATE CONSUMER OR THE
        processor_state_route = storage.insert_processor_state_route(processor_state=processor_state_route)

        start_index = processor_state_route.current_index if processor_state_route.current_index > 0 else 0
        end_index = route_count

        # this is common across all query messages forwarded to the processor
        base_processor_message = {
            "type": "query_state",
            "route_id": route_id,
            "context": message['context'] if 'context' in message else {}
        }

        # fetch the target processor the input state entries are to be processed by.
        processor = storage.fetch_processor(processor_id=processor_state_route.processor_id)

        if not processor:
            err = RouteNotFoundError(route_id, message)
            self.fail_execute_processor_state(route_id=route_id, exception=err)
            return

        # find the route, given the provider id, such that we can route the input messages to.
        route = router.find_route(processor.provider_id)

        # if there is no data then send an empty query state to the identified route
        if start_index >= end_index:
            await route.publish(msg=json.dumps({
                **base_processor_message,
                "query_state": []
            }))
        else:  # otherwise iterate each of the input states from the state set and submit as a block of inputs
            # TODO send as a whole batch or split into multiple mini-batches (?defined by the processor conf?)
            # iterate each row of the loaded state and submit for processing to its associated processor endpoint.
            for index in range(start_index, end_index):
                query_state_entry = loaded_state.build_query_state_from_row_data(index=index)
                logging.debug(f'processing query state index: {index}, given query state entry: {query_state_entry}')
                msg_dict = {
                    **base_processor_message,
                    "query_state": [query_state_entry]
                }
                msg_str = json.dumps(msg_dict)
                await route.publish(msg=msg_str)


if __name__ == '__main__':
    # asyncio.get_event_loop().run_until_complete(router.connect_all())

    consumer = MessagingStateRouterConsumer(
        route=state_router_route,
        monitor_route=monitor_route
    )

    consumer.setup_shutdown_signal()
    logging.info(f"entering control loop")
    consumer_no = random.randint(0, 5)
    asyncio.get_event_loop().run_until_complete(consumer.start_consumer(consumer_no=consumer_no))
    logging.info(f"exited control loop")
