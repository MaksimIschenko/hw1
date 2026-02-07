import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer

MessageHandler = Callable[[Any], Awaitable[None]]
BatchMessageHandler = Callable[[list[Any]], Awaitable[None]]

class SingleMessageConsumer:
    """Single message consumer"""
    
    def __init__(
        self,
        kafka_bootstrap_config: Mapping[str, Any],
        group_id: str,
        topics: str | tuple[str],
        *,
        enable_auto_commit: bool = True,
        auto_offset_reset: str = 'earliest',
        stop_event: asyncio.Event | None = None,
        handler: MessageHandler | None = None,
    ):
        """Initialize a SingleMessageConsumer.

        :param kafka_bootstrap_config: Kafka bootstrap configuration dictionary.
        :type kafka_bootstrap_config: Mapping[str, Any]
        :param group_id: Kafka consumer group ID.
        :type group_id: str
        :param topics: Topic name or tuple of topic names to subscribe to.
        :type topics: str | tuple[str, ...]
        :param enable_auto_commit: Enable or disable auto-commit of offsets (default: True).
        :type enable_auto_commit: bool, optional
        :param auto_offset_reset: Policy for resetting offsets if there are none (default: 'earliest').
        :type auto_offset_reset: str, optional
        :param stop_event: Optional event to signal consumer to stop (default: None).
        :type stop_event: asyncio.Event | None, optional
        :param handler: Optional async callback invoked for each message (default: None).
        :type handler: MessageHandler | None, optional
        """
        if isinstance(topics, str):
            self.topics: tuple[str, ...] = (topics,)
        else:
            self.topics = tuple(topics)
        if not self.topics:
            raise ValueError("topics must not be empty")
        
        self.__consumer = AIOConsumer(
            {
                **kafka_bootstrap_config,
                'group.id': group_id,
                'enable.auto.commit': enable_auto_commit,
                'auto.offset.reset': auto_offset_reset
            }
        )
        self._stop_event = stop_event
        self._handler = handler
        self._subscribed = False
        
    async def __aenter__(self):
        try:
            await self.__consumer.subscribe(list(self.topics))
            self._subscribed = True
            return self
        except Exception:
            try:
                await self.__consumer.close()
            finally:
                self._subscribed = False
            raise
        
    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._subscribed:
                try:
                    await self.__consumer.unsubscribe()
                except Exception:
                    pass
        finally:
            self._subscribed = False
            await self.__consumer.close()
        
    async def read_messages(self):
        """Read messages in a loop. Handles CancelledError for graceful shutdown.
        
        :raises RuntimeError: If consumer is not subscribed (use context manager).
        """
        if not self._subscribed:
            raise RuntimeError("Consumer must be used as context manager (async with) before reading messages")
        
        while True:
            if self._stop_event and self._stop_event.is_set():
                print("SingleMessageConsumer: stop_event set, exiting")
                return
            try:
                msg = await self.__consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    # PARTITION_EOF — нормальное событие
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    
                    if getattr(err, "fatal", None) and err.fatal():
                        raise RuntimeError(f"Kafka fatal error: {err}")
                    
                    # остальные — логируем и продолжаем
                    print(f"SingleMessageConsumer error: {err}")
                    continue
                
                if self._handler is not None:
                    await self._handler(msg)
                else:
                    val = msg.value()
                    if val is None:
                        print("SingleMessageConsumer received: <NULL>")
                    else:
                        print(f"SingleMessageConsumer received:{val.decode('utf-8')}")
                    
            except asyncio.CancelledError:
                print('SingleMessageConsumer: Cancellation requested')
                raise
            except Exception as e:
                print(f'Consumer polling error: {e}')
                await asyncio.sleep(1)


class BatchMessageConsumer:
    """Batch message consumer. Commits offsets once per batch."""

    def __init__(
        self,
        kafka_bootstrap_config: Mapping[str, Any],
        group_id: str,
        topics: str | tuple[str, ...],
        *,
        batch_size: int = 10,
        fetch_wait_max_ms: int = 2000,
        auto_offset_reset: str = "earliest",
        stop_event: asyncio.Event | None = None,
        handler: BatchMessageHandler | None = None,
    ):
        """Initialize a BatchMessageConsumer.

        :param kafka_bootstrap_config: Kafka bootstrap configuration dictionary.
        :type kafka_bootstrap_config: Mapping[str, Any]
        :param group_id: Kafka consumer group ID.
        :type group_id: str
        :param topics: Topic name or tuple of topic names to subscribe to.
        :type topics: str | tuple[str, ...]
        :param batch_size: Number of messages to collect before processing (default: BATCH_SIZE).
        :type batch_size: int, optional
        :param fetch_wait_max_ms: Max time to wait for fetch to accumulate data, ms (default: 2000).
        :type fetch_wait_max_ms: int, optional
        :param auto_offset_reset: Policy for resetting offsets if there are none (default: 'earliest').
        :type auto_offset_reset: str, optional
        :param stop_event: Optional event to signal consumer to stop (default: None).
        :type stop_event: asyncio.Event | None, optional
        :param handler: Optional async callback invoked for each batch (default: None).
        :type handler: BatchMessageHandler | None, optional
        """
        if isinstance(topics, str):
            self.topics: tuple[str, ...] = (topics,)
        else:
            self.topics = tuple(topics)
        if not self.topics:
            raise ValueError("topics must not be empty")

        self.__consumer = AIOConsumer(
            {
                **kafka_bootstrap_config,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
                "fetch.wait.max.ms": fetch_wait_max_ms,
            }
        )
        self._batch_size = batch_size
        self._stop_event = stop_event
        self._handler = handler
        self._subscribed = False

    async def __aenter__(self) -> "BatchMessageConsumer":
        try:
            await self.__consumer.subscribe(list(self.topics))
            self._subscribed = True
            return self
        except Exception:
            try:
                await self.__consumer.close()
            finally:
                self._subscribed = False
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if self._subscribed:
                try:
                    await self.__consumer.unsubscribe()
                except Exception:
                    pass
        finally:
            self._subscribed = False
            await self.__consumer.close()

    async def read_messages(self) -> None:
        """Read messages in batches. Commits after each successful batch.
        Handles CancelledError for graceful shutdown.

        :raises RuntimeError: If consumer is not subscribed (use context manager).
        """
        if not self._subscribed:
            raise RuntimeError(
                "Consumer must be used as context manager (async with) before reading batches"
            )

        while True:
            if self._stop_event and self._stop_event.is_set():
                print("BatchMessageConsumer: stop_event set, exiting")
                return

            batch: list[Any] = []

            try:
                while len(batch) < self._batch_size:
                    if self._stop_event and self._stop_event.is_set():
                        return

                    msg = await self.__consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    err = msg.error()
                    if err is not None:
                        if err.code() == KafkaError._PARTITION_EOF:
                            continue
                        if getattr(err, "fatal", None) and err.fatal():
                            raise RuntimeError(f"Kafka fatal error: {err}")
                        print(f"BatchMessageConsumer error: {err}")
                        continue

                    batch.append(msg)

                if not batch:
                    continue

                if self._handler is not None:
                    await self._handler(batch)
                else:
                    print(f"BatchMessageConsumer received {len(batch)} messages...")
                    for idx, m in enumerate(batch):
                        val = m.value()
                        s = val.decode("utf-8") if val else "<NULL>"
                        print(f"{idx + 1}: {s} (p={m.partition()}, o={m.offset()})")

                await self.__consumer.commit(asynchronous=False)

            except asyncio.CancelledError:
                print("BatchMessageConsumer: Cancellation requested")
                raise
            except Exception as e:
                print(f"BatchMessageConsumer processing error: {e}")
                await asyncio.sleep(1)




