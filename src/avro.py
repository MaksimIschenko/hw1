from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry._async.avro import (
    AsyncAvroDeserializer,
    AsyncAvroSerializer,
)


def build_schema_registry_client(url: str) -> AsyncSchemaRegistryClient:
    return AsyncSchemaRegistryClient({"url": url})


@dataclass(slots=True)
class AvroContext:
    sr_client: AsyncSchemaRegistryClient
    serializer: AsyncAvroSerializer
    deserializer: AsyncAvroDeserializer

    @classmethod
    async def create(
        cls,
        *,
        schema_registry_url: str,
        schema_str: str,
    ) -> AvroContext:
        sr_client = build_schema_registry_client(schema_registry_url)
        serializer = await AsyncAvroSerializer(sr_client, schema_str=schema_str)
        deserializer = await AsyncAvroDeserializer(sr_client)
        return cls(sr_client=sr_client, serializer=serializer, deserializer=deserializer)
