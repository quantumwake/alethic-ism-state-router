import asyncio
import json
import math
import os
from typing import Tuple

import dotenv
from ismcore.messaging.base_message_provider import BaseMessageConsumer
from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.messaging.base_message_router import Router
from ismcore.messaging.errors import RouteNotFoundError
from ismcore.messaging.nats_message_provider import NATSMessageProvider
from ismcore.model.base_model import ProcessorStatusCode, Processor, ProcessorPropertiesBase
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
                - input_route_id: Optional input route id for calibration/retry support

        Raises:
            ValueError: If route_id or query_state is missing, or if processor/route not found
        """
        if 'route_id' not in message:
            raise ValueError(f'route_id does not exist in message envelope {message}')

        if 'query_state' not in message:
            raise ValueError(f'no query_state entries found in message envelop {message}')

        # Get the route this message is ingress on
        route_id = message['route_id']

        # Get input_route_id for calibration/retry support (pass-through)
        input_route_id = message.get('input_route_id')

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
        priority = message.get('priority')
        subject = route.get_publish_subject(priority=priority, partition_key=processor.project_id)
        logging.debug(f'sending query state entry to route provider: {route.selector}, route: {route_id}, subject: {subject}')

        route_message = self._build_base_route_message(
            route_id=route_id,
            context=message.get('context'),
            input_route_id=input_route_id
        )
        route_message['query_state'] = query_state

        await route.publish(msg=json.dumps(route_message), subject=subject)

    async def execute_processor_state_route(self, message: dict):
        """
        Routes entire state sets to processors for batch processing.

        This method implements two-level batching:
        1. Database loading batches (maxBatchSize): Prevents OOM when loading large state sets
        2. Output publishing batches (maxBatchLimit): Controls message size sent to processors

        Args:
            message: Message containing:
                - route_id: Route identifier for fetching processor state route
                - context: Optional context data
                - input_route_id: Optional input route id for calibration/retry support

        Raises:
            ValueError: If route_id is missing or invalid
            RouteNotFoundError: If processor or route cannot be found
        """
        # Validate message and fetch route data
        route_id, processor_state_route, state_metadata = self._validate_and_fetch_route_data(message)

        # Get input_route_id for calibration/retry support (pass-through)
        input_route_id = message.get('input_route_id')

        total_count = state_metadata.count
        logging.info(f'execute route {route_id}, state has {total_count} rows, will process in batches')

        # Build base message structure common to all forwarded messages
        base_processor_message = self._build_base_route_message(
            route_id=route_id,
            context=message.get('context'),
            input_route_id=input_route_id
        )

        # Fetch the target processor and its messaging route
        try:
            processor, route, processor_properties = self._prepare_processor_and_route(processor_state_route)
        except ValueError:
            await self.fail_execute_processor_state(
                route_id=route_id,
                exception=RouteNotFoundError(route_id, message)
            )
            return

        # Build priority subject if enabled
        priority = message.get('priority')
        subject = route.get_publish_subject(priority=priority, partition_key=processor.project_id)

        # Handle empty state sets
        state_row_count = state_metadata.count
        if state_row_count == 0:
            await self._handle_empty_state(route, base_processor_message, route_id, subject)
            return

        # Process state data in batches to avoid OOM
        batch_size = processor_properties.maxBatchSize
        max_batch_limit = processor_properties.maxBatchLimit
        total_batches = math.ceil(state_row_count / batch_size)
        state_row_count_processed = 0

        for batch_num in range(total_batches):
            rows_processed = await self._process_state_batch(
                processor_state_route,
                batch_num,
                batch_size,
                total_batches,
                state_row_count,
                max_batch_limit,
                base_processor_message,
                route,
                route_id,
                subject
            )
            state_row_count_processed += rows_processed

        logging.info(
            f'completed total batches: {total_batches}, '
            f'total rows processes: {state_row_count_processed} / {state_row_count} rows for route {route_id}'
        )

    def _fetch_processor_and_route(self, processor_id: str) -> Tuple[Processor, BaseRoute]:
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

    def _build_base_route_message(self, route_id: str, context: dict = None, input_route_id: str = None) -> dict:
        """
        Builds the base message structure for routing.

        Args:
            route_id: Route identifier
            context: Optional context data to include in message
            input_route_id: The input route id where the input originally came from (for calibration/retry)

        Returns:
            dict: Base message structure with type, route_id, input_route_id, and context
        """
        return {
            "type": "query_state",
            "route_id": route_id,
            "input_route_id": input_route_id,
            "context": context if context else {}
        }

    def _validate_and_fetch_route_data(self, message: dict):
        """
        Validates the message and fetches the processor state route.

        Args:
            message: Message containing route_id

        Returns:
            tuple: (route_id, processor_state_route, state_metadata)

        Raises:
            ValueError: If validation fails or data not found
        """
        if 'route_id' not in message:
            raise ValueError(f'route id does not exist in message envelope {message}')

        route_id = message['route_id']
        routes = storage.fetch_processor_state_route(route_id)

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

        return route_id, processor_state_route, state_metadata

    def _prepare_processor_and_route(self, processor_state_route):
        """
        Fetches the processor and route, and loads processor properties.

        Args:
            processor_state_route: The processor state route object

        Returns:
            tuple: (processor, route, processor_properties)

        Raises:
            ValueError: If processor or route not found
        """
        processor, route = self._fetch_processor_and_route(processor_state_route.processor_id)

        # Load processor properties with defaults
        if not processor.properties:
            processor_properties = ProcessorPropertiesBase()
        else:
            processor_properties = ProcessorPropertiesBase(**processor.properties)

        return processor, route, processor_properties

    def _build_query_state_entries(self, batch_state, start_index: int, end_index: int) -> list:
        """
        Builds query state entries from a slice of batch data.

        Args:
            batch_state: The loaded state batch
            start_index: Start index (inclusive) within the batch
            end_index: End index (exclusive) within the batch

        Returns:
            list: Query state entries for the specified range
        """
        return [
            batch_state.build_query_state_from_row_data(index=row_index)
            for row_index in range(start_index, end_index)
        ]

    async def _handle_empty_state(self, route, base_processor_message, route_id, subject=None):
        """
        Handles empty state sets by sending an empty query to the processor.

        Args:
            route: The route to publish to
            base_processor_message: Base message structure
            route_id: Route identifier for logging
            subject: Optional priority subject override
        """
        logging.info(f'route {route_id} has empty state set, sending empty query')
        await route.publish(msg=json.dumps({
            **base_processor_message,
            "query_state": []
        }), subject=subject)

    async def _publish_output_batch(self, query_state_entries: list, base_processor_message: dict, route, subject=None):
        """
        Publishes a single output batch to the processor route.

        Args:
            query_state_entries: List of query state entries to publish
            base_processor_message: Base message structure
            route: The route to publish to
            subject: Optional priority subject override

        Returns:
            int: Number of entries published
        """
        route_message = {
            **base_processor_message,
            "query_state": query_state_entries
        }
        await route.publish(msg=json.dumps(route_message), subject=subject)
        return len(query_state_entries)

    async def _process_and_publish_output_batches(
        self,
        batch_state,
        batch_row_count: int,
        max_batch_limit: int,
        base_processor_message: dict,
        route,
        batch_num: int,
        total_batches: int,
        subject=None
    ) -> int:
        """
        Splits a loaded state batch into smaller output batches and publishes them.

        This is the second level of batching - splitting already-loaded data
        into smaller chunks for publishing (controlled by maxBatchLimit).

        Args:
            batch_state: The loaded state batch
            batch_row_count: Number of rows in this batch
            max_batch_limit: Maximum rows per output batch (processor's maxBatchLimit)
            base_processor_message: Base message structure
            route: The route to publish to
            batch_num: Current batch number (for logging)
            total_batches: Total number of batches (for logging)
            subject: Optional priority subject override

        Returns:
            int: Total number of rows processed from this batch
        """
        total_output_batches = math.ceil(batch_row_count / max_batch_limit)
        rows_processed = 0

        for output_batch_num in range(total_output_batches):
            logging.debug(
                f'processing output batch {output_batch_num + 1}/{total_output_batches} '
                f'for state batch {batch_num + 1}/{total_batches}'
            )

            # Calculate slice indices within the current loaded batch
            output_batch_offset_start = output_batch_num * max_batch_limit
            output_batch_offset_end = min(
                output_batch_offset_start + max_batch_limit,
                batch_row_count
            )

            # Build and publish the output batch
            query_state_entries = self._build_query_state_entries(
                batch_state,
                output_batch_offset_start,
                output_batch_offset_end
            )

            rows_published = await self._publish_output_batch(
                query_state_entries,
                base_processor_message,
                route,
                subject
            )
            rows_processed += rows_published

        return rows_processed

    async def _process_state_batch(
        self,
        processor_state_route,
        batch_num: int,
        batch_size: int,
        total_batches: int,
        state_row_count: int,
        max_batch_limit: int,
        base_processor_message: dict,
        route,
        route_id: str,
        subject=None
    ) -> int:
        """
        Loads and processes a single database batch.

        This is the first level of batching - loading chunks from the database
        to avoid OOM (controlled by maxBatchSize).

        Args:
            processor_state_route: The processor state route
            batch_num: Current batch number (0-indexed)
            batch_size: Size of each database load batch (processor's maxBatchSize)
            total_batches: Total number of batches to process
            state_row_count: Total number of rows in the entire state
            max_batch_limit: Maximum rows per output batch (processor's maxBatchLimit)
            base_processor_message: Base message structure
            route: The route to publish to
            route_id: Route identifier for logging
            subject: Optional priority subject override

        Returns:
            int: Number of rows processed from this batch
        """
        batch_offset = batch_num * batch_size
        logging.debug(f'loading batch {batch_num + 1}/{total_batches}, offset={batch_offset}, limit={batch_size}')

        # Load batch of state data from database
        batch_state = storage.load_state(
            state_id=processor_state_route.state_id,
            load_data=True,
            offset=batch_offset,
            limit=batch_size
        )

        if not batch_state or not batch_state.data:
            logging.warning(f'batch {batch_num + 1} returned no data, skipping')
            return 0

        # Calculate expected row count for this batch (state data is immutable)
        batch_row_count = min(state_row_count - batch_offset, batch_size)
        logging.debug(f'batch {batch_num + 1} loaded {batch_row_count} rows')

        # Process and publish output batches
        rows_processed = await self._process_and_publish_output_batches(
            batch_state,
            batch_row_count,
            max_batch_limit,
            base_processor_message,
            route,
            batch_num,
            total_batches,
            subject
        )

        logging.info(f'completed batch {batch_num + 1}/{total_batches} for route {route_id}, rows: {rows_processed}')
        return rows_processed


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
