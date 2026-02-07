import asyncio
import uuid
from collections.abc import Mapping
from typing import Any

from confluent_kafka.aio import AIOProducer


class Producer:
    """Kafka async producer. Use as async context manager."""

    def __init__(
        self,
        kafka_bootstrap_config: Mapping[str, Any],
        topic: str,
        *,
        acks: str = "all",
        enable_idempotence: bool = True,
        retries: int = 3,
        linger_ms: int = 5,
        stop_event: asyncio.Event | None = None,
    ):
        """Initialize a Producer.

        :param kafka_bootstrap_config: Kafka bootstrap configuration dictionary.
        :type kafka_bootstrap_config: Mapping[str, Any]
        :param topic: Topic to produce to.
        :type topic: str
        :param acks: Number of acknowledgments (default: 'all').
        :type acks: str, optional
        :param enable_idempotence: Enable idempotent producer (default: True).
        :type enable_idempotence: bool, optional
        :param retries: Number of retries on failure (default: 3).
        :type retries: int, optional
        :param linger_ms: Batching delay in ms (default: 5).
        :type linger_ms: int, optional
        :param stop_event: Optional event to signal producer to stop (default: None).
        :type stop_event: asyncio.Event | None, optional
        """
        if not topic:
            raise ValueError("topic must not be empty")

        self._topic = topic
        self._stop_event = stop_event
        self.__producer = AIOProducer(
            {
                **kafka_bootstrap_config,
                "acks": acks,
                "enable.idempotence": enable_idempotence,
                "retries": retries,
                "linger.ms": linger_ms,
            }
        )
        self._started = False

    async def __aenter__(self) -> "Producer":
        self._started = True
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if hasattr(self.__producer, "flush"):
                try:
                    await self.__producer.flush(timeout=5.0)
                except Exception:
                    pass
        finally:
            self._started = False
            await self.__producer.close()

    async def send(
        self,
        value: str | bytes,
        key: str | bytes | None = None,
    ) -> Any:
        """Send one message to the topic.

        :param value: Message value.
        :type value: str | bytes
        :param key: Optional message key (default: random UUID string).
        :type key: str | bytes | None, optional
        :returns: Delivery result (topic, partition, offset).
        :raises RuntimeError: If producer is not started (use context manager).
        """
        if not self._started:
            raise RuntimeError(
                "Producer must be used as context manager (async with) before sending"
            )
        if key is None:
            key = str(uuid.uuid4())
        if isinstance(value, str):
            value = value.encode("utf-8")
        future = await self.__producer.produce(
            topic=self._topic,
            key=key,
            value=value,
        )
        msg = await future
        return msg

    async def send_messages(self) -> None:
        """Send messages in a loop (message_count_0, 1, 2, ...).
        Handles CancelledError for graceful shutdown.

        :raises RuntimeError: If producer is not started (use context manager).
        """
        if not self._started:
            raise RuntimeError(
                "Producer must be used as context manager (async with) before running"
            )

        count = 0
        while True:
            if self._stop_event and self._stop_event.is_set():
                print("Producer: stop_event set, exiting")
                return

            try:
                value = f"message_count_{count}"
                msg = await self.send(value)
                print(
                    f"Producer delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                )
                count += 1
            except asyncio.CancelledError:
                print("Producer: Cancellation requested")
                raise
            except Exception as e:
                print(f"Producer error: {e}")

            await asyncio.sleep(0.1)
