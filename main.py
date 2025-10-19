import asyncio
import json
import math
import os
import dotenv
from ismcore.messaging.base_message_provider import BaseMessageConsumer
from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.messaging.base_message_router import Router
from ismcore.messaging.errors import RouteNotFoundError
from ismcore.messaging.nats_message_provider import NATSMessageProvider
from ismcore.model.base_model import ProcessorStatusCode
from ismdb.postgres_storage_class import PostgresDatabaseStorage

from logger import logging

dotenv.load_dotenv()
logging.info('starting up NATS consumer for state routing')

# =============================================================================
# Configuration
# =============================================================================

# Database connection URL
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# Message routing configuration file
# The state router takes input messages and routes them to appropriate
# processor destinations based on the route selector definitions
ROUTING_FILE = os.environ.get("ROUTING_FILE", '.routing.yaml')
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Batch size for loading state data to prevent OOM on large state sets
STATE_BATCH_SIZE = int(os.environ.get("STATE_BATCH_SIZE", "1000"))

# =============================================================================
# Infrastructure Initialization
# =============================================================================

# PostgreSQL storage backend for state persistence
storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

# NATS messaging provider for message routing
messaging_provider = NATSMessageProvider()

# Message router for directing state queries to processors
# Routing flow:
# 1. Query state inputs are consumed from the state router route
# 2. Processor state associations are fetched from database
# 3. Messages are published to processor-specific routes
router = Router(
    provider=messaging_provider,
    yaml_file=ROUTING_FILE
)

# Route definitions for monitoring and state routing
monitor_route = router.find_route("processor/monitor")
state_router_route = router.find_route("processor/state/router")

# =============================================================================
# State Router Consumer
# =============================================================================

class MessagingStateRouterConsumer(BaseMessageConsumer):
    """
    Consumer for routing state messages to appropriate processors.

    This consumer handles two types of messages:
    1. query_state_entry: Routes individual state entries to processors
    2. query_state_route: Routes entire state sets to processors for batch processing

    The router fetches processor associations from the database and forwards
    messages to the appropriate processor endpoints via NATS messaging.
    """

    def __init__(self, route: BaseRoute, monitor_route: BaseRoute = None, **kwargs):
        super().__init__(route=route, monitor_route=monitor_route)

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        """
        Hook executed before processing a message.

        Updates the processor state to ROUTED status to indicate the message
        has been received and is being processed.

        Args:
            consumer_message_mapping: Mapping containing the consumed message details
            **kwargs: Additional keyword arguments
        """
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.ROUTED)

    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        """
        Hook executed after processing a message.

        Note: Status updates are handled elsewhere to avoid race conditions.
        - Pre-execute sets status to ROUTED
        - Processors update status to RUNNING/COMPLETED/FAILED when they process
        - Error handling in this consumer sets FAILED status if routing fails

        Args:
            consumer_message_mapping: Mapping containing the consumed message details
            **kwargs: Additional keyword arguments
        """
        pass


    async def execute(self, message: dict):
        """
        Main execution handler that routes messages based on their type.

        Args:
            message: Message dictionary containing 'type' field and other data

        Raises:
            ValueError: If message type is missing or invalid

        Supported message types:
            - query_state_entry: Routes individual state entries
            - query_state_route: Routes entire state sets for batch processing
        """
        if 'type' not in message:
            raise ValueError(f'inbound message "{message}" did not have a type')

        message_type = message['type']

        if 'query_state_entry' == message_type:
            await self.execute_query_state_entry(message)
        elif 'query_state_route' == message_type:
            await self.execute_processor_state_route(message)
        else:
            raise ValueError('query state value does not exist in message envelope')

    def _fetch_processor_and_route(self, processor_id: str):
        """
        Fetches processor details and determines the messaging route.

        Args:
            processor_id: ID of the processor to fetch

        Returns:
            tuple: (processor, route) objects

        Raises:
            ValueError: If processor or route cannot be found
        """
        processor = storage.fetch_processor(processor_id=processor_id)
        if not processor:
            raise ValueError(f'Processor not found: {processor_id}')

        route = router.find_route_wildcard(processor.provider_id)
        if not route:
            raise ValueError(f'Route not found for provider: {processor.provider_id}')

        return processor, route

    def _build_base_route_message(self, route_id: str, context: dict = None) -> dict:
        """
        Builds the base message structure for routing.

        Args:
            route_id: Route identifier
            context: Optional context data to include in message

        Returns:
            dict: Base message structure with type, route_id, and context
        """
        return {
            "type": "query_state",
            "route_id": route_id,
            "context": context if context else {}
        }

    async def execute_query_state_entry(self, message: dict):
        """
        Routes individual query state entries to their assigned processor.

        Fetches the processor associated with the route_id, determines the
        appropriate messaging route, and forwards the query state entries.

        Args:
            message: Message containing:
                - route_id: Route identifier for fetching processor assignment
                - query_state: State entry or list of entries to process
                - context: Optional context data

        Raises:
            ValueError: If route_id or query_state is missing, or if processor/route not found
        """
        if 'route_id' not in message:
            raise ValueError(f'route_id does not exist in message envelope {message}')

        if 'query_state' not in message:
            raise ValueError(f'no query_state entries found in message envelop {message}')

        # Get the route this message is ingress on
        route_id = message['route_id']

        # Normalize query_state to a list
        query_state = message['query_state']
        if not isinstance(query_state, list):
            query_state = [query_state]

        # Fetch the processor state route association
        forward_processor_state = storage.fetch_processor_state_route_by_route_id(route_id=route_id)
        if not forward_processor_state:
            raise ValueError(f'processor state not found for route_id: {route_id}')

        # Fetch processor and route using helper method
        # TODO: Add caching layer on the storage engine to reduce database calls
        logging.debug(f'fetching processor {forward_processor_state.processor_id} and route details')
        processor, route = self._fetch_processor_and_route(forward_processor_state.processor_id)

        # Build and publish the route message
        logging.debug(f'sending query state entry to route provider: {route.selector}, route: {route_id}')
        route_message = self._build_base_route_message(
            route_id=route_id,
            context=message.get('context')
        )
        route_message['query_state'] = query_state

        await route.publish(msg=json.dumps(route_message))

    async def execute_processor_state_route(self, message: dict):
        """
        Routes entire state sets to processors for batch processing.

        Loads all state data for the given route, iterates through each row,
        and publishes individual query state entries to the assigned processor.
        Tracks processing progress via current_index and maximum_index.

        Args:
            message: Message containing:
                - route_id: Route identifier for fetching processor state route
                - context: Optional context data

        Raises:
            ValueError: If route_id is missing or invalid
            RouteNotFoundError: If processor or route cannot be found
        """
        if 'route_id' not in message:
            raise ValueError(f'route id does not exist in message envelope {message}')

        route_id = message['route_id']
        routes = storage.fetch_processor_state_route(message['route_id'])

        if not routes:
            raise ValueError(f'invalid route: {route_id}, not found')

        # TODO: Support multiple routes per input state
        # Currently restricted to single route execution. Multi-route handling
        # should be managed by the processor itself.
        if len(routes) != 1:
            raise ValueError(f'invalid number of routes. expected 1, got {len(routes)}')

        processor_state_route = routes[0]

        # Load only metadata (columns, count) to avoid OOM on large state sets
        state_metadata = storage.load_state_metadata(state_id=processor_state_route.state_id)
        if not state_metadata:
            raise ValueError(f'State metadata not found for state_id: {processor_state_route.state_id}')

        total_count = state_metadata.count
        logging.info(f'execute route {route_id}, state has {total_count} rows, will process in batches')

        # =============================================================================
        # COMMENTED OUT: Index tracking for resumable/incremental processing
        # =============================================================================
        # The original intent was to track processing progress (current_index, maximum_index)
        # to enable resumable processing if the router crashed mid-batch.
        #
        # PROBLEMS:
        # 1. Falsy value bug: "if current_index else -1" treats 0 as falsy, incorrectly resetting to -1
        # 2. Unnecessary complexity: The goal is to send the ENTIRE state set every time
        # 3. OOM issues: Loading full state with load_data=True causes OOMKill on large sets
        #
        # DECISION: Always process entire state set from 0 to count
        # - Processors are idempotent and can handle reprocessing
        # - Batch loading prevents OOM issues
        # - Simpler mental model: "route entire state set"
        #
        # If resumable processing is needed in the future:
        # - Fix: Use "if current_index is not None else -1" to handle 0 properly
        # - Store checkpoints AFTER successful batch completion
        # - Consider moving to processor state consumer to avoid race conditions
        # =============================================================================
        # processor_state_route.current_index = processor_state_route.current_index if processor_state_route.current_index else -1
        # processor_state_route.maximum_index = processor_state_route.maximum_index if processor_state_route.maximum_index else -1
        # processor_state_route = storage.insert_processor_state_route(processor_state=processor_state_route)
        # start_index = processor_state_route.current_index if processor_state_route.current_index > 0 else 0
        # =============================================================================

        # Always process entire state set
        # start_index = 0
        # end_index = total_count

        # Build base message structure common to all forwarded messages
        base_processor_message = self._build_base_route_message(
            route_id=route_id,
            context=message.get('context')
        )

        # Fetch the target processor and its messaging route
        try:
            processor, route = self._fetch_processor_and_route(processor_state_route.processor_id)
        except ValueError:
            await self.fail_execute_processor_state(route_id=route_id, exception=RouteNotFoundError(route_id, message))
            return

        # get state row count
        state_row_count = state_metadata.count

        # Handle empty state sets by sending an empty query to the processor
        if state_row_count == 0:
            logging.info(f'route {route_id} has empty state set, sending empty query')
            await route.publish(msg=json.dumps({
                **base_processor_message,
                "query_state": []
            }))
            return

        # Process state data in batches to avoid OOM
        batch_limit = STATE_BATCH_SIZE
        total_batches = math.ceil(state_row_count / batch_limit) # Ceiling division

        for batch_num in range(total_batches):
            batch_offset = batch_num * batch_limit
            logging.debug(f'loading batch {batch_num + 1}/{total_batches}, offset={batch_offset}, limit={batch_limit}')

            # Load batch of state data
            batch_state = storage.load_state(
                state_id=processor_state_route.state_id,
                load_data=True,
                offset=batch_offset,
                limit=batch_limit
            )

            if not batch_state or not batch_state.data:
                logging.warning(f'batch {batch_num + 1} returned no data, skipping')
                continue

            # Calculate expected row count for this batch
            # State data is immutable, so we can rely on state_row_count
            batch_row_count = state_row_count - batch_offset
            batch_row_count = min(batch_row_count, batch_limit)
            logging.debug(f'batch {batch_num + 1} loaded {batch_row_count} rows')

            for row_index in range(batch_row_count):
                query_state_entry = batch_state.build_query_state_from_row_data(index=row_index)
                route_message = {
                    **base_processor_message,
                    "query_state": [query_state_entry]
                }
                await route.publish(msg=json.dumps(route_message))

            logging.info(f'completed batch {batch_num + 1}/{total_batches}')

# =============================================================================
# Main Execution
# =============================================================================

if __name__ == '__main__':
    # Initialize the state router consumer
    consumer = MessagingStateRouterConsumer(
        route=state_router_route,
        monitor_route=monitor_route
    )

    # Setup graceful shutdown handling
    consumer.setup_shutdown_signal()

    # Start the consumer event loop
    logging.info("entering control loop")
    asyncio.get_event_loop().run_until_complete(consumer.start_consumer())
    logging.info("exited control loop")
