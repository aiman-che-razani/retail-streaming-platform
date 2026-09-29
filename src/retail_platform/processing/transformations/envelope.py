"""Decode the Confluent wire format and parse the JSON envelope — native functions only.

Input: the Kafka source DataFrame (key, value, topic, partition, offset, timestamp, headers).
Output adds:
    wire_ok     boolean  value starts with magic byte 0x00 and has a body
    schema_id   int      Schema Registry id from bytes 1..4
    body        string   UTF-8 JSON after the 5-byte header
    is_envelope boolean  body is a JSON object with `metadata` and `payload` objects
    metadata    struct   parsed per processing.schemas.METADATA
    payload     struct   parsed per the dataset's payload schema
    event_ts    timestamp parsed metadata.event_timestamp (null if missing/unparseable)
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from retail_platform.contracts.catalog import Dataset
from retail_platform.processing.schemas import envelope_schema

HEADER_BYTES = 5


def _try_timestamp(column: str) -> Column:
    # try_cast: an unparseable timestamp must become null (-> rule ENV-006), never an exception
    # that would crash the whole micro-batch (poison pill, FM-22).
    return F.expr(f"try_cast({column} AS TIMESTAMP)")


def decode_confluent(df: DataFrame) -> DataFrame:
    value = F.col("value")
    wire_ok = (
        value.isNotNull()
        & (F.length(value) > HEADER_BYTES)
        & (F.substring(value, 1, 1) == F.unhex(F.lit("00")))
    )
    return df.withColumns(
        {
            "wire_ok": F.coalesce(wire_ok, F.lit(False)),
            "schema_id": F.when(
                wire_ok, F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int")
            ),
            "body": F.when(
                wire_ok, F.expr(f"decode(substring(value, {HEADER_BYTES + 1}), 'UTF-8')")
            ),
        }
    )


def parse_envelope(df: DataFrame, dataset: Dataset) -> DataFrame:
    parsed = F.from_json(F.col("body"), envelope_schema(dataset), {"mode": "PERMISSIVE"})
    is_envelope = (
        F.get_json_object("body", "$.metadata").isNotNull()
        & F.get_json_object("body", "$.payload").isNotNull()
        & F.get_json_object("body", "$.metadata").startswith("{")
        & F.get_json_object("body", "$.payload").startswith("{")
    )
    return (
        df.withColumn("_env", parsed)
        .withColumns(
            {
                "is_envelope": F.coalesce(is_envelope, F.lit(False)),
                "metadata": F.col("_env.metadata"),
                "payload": F.col("_env.payload"),
            }
        )
        .withColumn("event_ts", _try_timestamp("metadata.event_timestamp"))
        .drop("_env")
    )


def decode_and_parse(kafka_df: DataFrame, dataset: Dataset) -> DataFrame:
    return parse_envelope(decode_confluent(kafka_df), dataset)
