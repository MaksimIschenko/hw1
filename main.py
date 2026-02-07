import asyncio
import logging

from src.config import (
    BATCH_SIZE,
    CONSUMER_GROUP_ID_1,
    CONSUMER_GROUP_ID_2,
    KAFKA_BOOTSTRAP_CONFIG,
    TEST_TOPIC_NAME,
)
from src.consumer import (
    BatchMessageConsumer,
    SingleMessageConsumer,
)
from src.producer import Producer

logger = logging.getLogger(__name__)


async def producer_runner(stop_event: asyncio.Event) -> None:
    """Run producer."""
    async with Producer(
        KAFKA_BOOTSTRAP_CONFIG,
        TEST_TOPIC_NAME,
        stop_event=stop_event,
    ) as producer:
        await producer.send_messages()


async def single_consumer_runner(stop_event: asyncio.Event) -> None:
    """Run single message consumer."""
    async with SingleMessageConsumer(
        kafka_bootstrap_config=KAFKA_BOOTSTRAP_CONFIG,
        group_id=CONSUMER_GROUP_ID_1,
        topics=TEST_TOPIC_NAME,
        stop_event=stop_event,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    ) as consumer:
        await consumer.read_messages()
        
async def batch_consumer_runner(stop_event: asyncio.Event) -> None:
    """Run batch messages consumer."""
    async with BatchMessageConsumer(
        KAFKA_BOOTSTRAP_CONFIG,
        CONSUMER_GROUP_ID_2,
        TEST_TOPIC_NAME,
        batch_size=BATCH_SIZE,
        stop_event=stop_event,
    ) as consumer:
        await consumer.read_messages()


async def main():
    stop_event = asyncio.Event()

    # Create tasks
    producer_task = asyncio.create_task(producer_runner(stop_event))
    single_msg_consumer_task = asyncio.create_task(single_consumer_runner(stop_event))
    batch_msgs_consumer_task = asyncio.create_task(batch_consumer_runner(stop_event))

    try:
        await asyncio.gather(
            producer_task, 
            single_msg_consumer_task, 
            batch_msgs_consumer_task
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        stop_event.set()  # попросим consumer выйти сам

        producer_task.cancel()
        single_msg_consumer_task.cancel()
        batch_msgs_consumer_task.cancel()

        await asyncio.gather(
            producer_task,
            single_msg_consumer_task,
            batch_msgs_consumer_task,
            return_exceptions=True,
        )

    except Exception as e:
        logger.error("Error in main: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
        raise SystemExit(0) from None
