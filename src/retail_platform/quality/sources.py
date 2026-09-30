"""Gather reconciliation inputs from Kafka (watermarks) and Snowflake (audit, RAW, STAGING)."""

from __future__ import annotations

from typing import Any

from confluent_kafka import Consumer, TopicPartition

from retail_platform.contracts.catalog import Dataset, TopicCatalog
from retail_platform.quality.reconciliation import PartitionOffsets, TopicInputs

RAW_TABLES = {
    Dataset.POS_TRANSACTIONS: "RAW.POS_TRANSACTIONS",
    Dataset.INVENTORY_MOVEMENTS: "RAW.INVENTORY_MOVEMENTS",
    Dataset.CUSTOMER_EVENTS: "RAW.CUSTOMER_EVENTS",
    Dataset.PRODUCT_EVENTS: "RAW.PRODUCT_EVENTS",
}
# (business key JSON path, staging table, staging key column) for datasets with business keys
BUSINESS_KEYS = {
    Dataset.POS_TRANSACTIONS: (
        "EVENT:payload:transaction_id::VARCHAR",
        "STAGING.STG_POS_TRANSACTION_LINES",
        "TRANSACTION_ID",
    ),
    Dataset.INVENTORY_MOVEMENTS: (
        "EVENT:payload:movement_id::VARCHAR",
        "STAGING.STG_INVENTORY_MOVEMENTS",
        "MOVEMENT_ID",
    ),
}


def kafka_watermarks(consumer: Consumer, topic: str) -> dict[int, PartitionOffsets]:
    metadata = consumer.list_topics(topic, timeout=10)
    result = {}
    for partition in metadata.topics[topic].partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        result[partition] = PartitionOffsets(low=low, high=high)
    return result


def _scalar(cursor: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def gather(cursor: Any, consumer: Consumer, catalog: TopicCatalog) -> list[TopicInputs]:
    inputs: list[TopicInputs] = []
    for dataset, raw_table in RAW_TABLES.items():
        topic = catalog.primary_for_dataset(dataset)
        # Audit rows can repeat if a batch was re-landed: keep the latest per partition-batch.
        cursor.execute(
            """
            SELECT KAFKA_PARTITION, MAX(MAX_OFFSET), SUM(RECORDS_READ), SUM(RECORDS_VALID),
                   SUM(RECORDS_REJECTED)
            FROM (
                SELECT * FROM RAW.INGEST_BATCH_AUDIT
                WHERE DATASET = %s AND KAFKA_TOPIC = %s
                QUALIFY ROW_NUMBER() OVER (PARTITION BY SPARK_QUERY_ID, SPARK_BATCH_ID,
                                           KAFKA_PARTITION ORDER BY _LOADED_AT DESC) = 1
            )
            GROUP BY KAFKA_PARTITION
            """,
            (dataset.value, topic),
        )
        rows = cursor.fetchall()
        raw_offsets = _scalar(
            cursor,
            f"SELECT COUNT(DISTINCT KAFKA_PARTITION, KAFKA_OFFSET) FROM {raw_table} "
            "WHERE KAFKA_TOPIC = %s",
            (topic,),
        )
        staged = raw_keys = None
        if dataset in BUSINESS_KEYS:
            key_expr, stg_table, stg_col = BUSINESS_KEYS[dataset]
            raw_keys = _scalar(cursor, f"SELECT COUNT(DISTINCT {key_expr}) FROM {raw_table}")
            staged = _scalar(cursor, f"SELECT COUNT(DISTINCT {stg_col}) FROM {stg_table}")
        inputs.append(
            TopicInputs(
                topic=topic,
                kafka=kafka_watermarks(consumer, topic),
                spark_max_offset={int(r[0]): int(r[1]) for r in rows},
                spark_read=sum(int(r[2]) for r in rows),
                spark_valid=sum(int(r[3]) for r in rows),
                spark_rejected=sum(int(r[4]) for r in rows),
                raw_distinct_offsets=raw_offsets,
                staged_keys=staged,
                raw_distinct_keys=raw_keys,
            )
        )
    return inputs


def latest_dq_results(cursor: Any) -> list[dict[str, Any]]:
    cursor.execute(
        "SELECT RULE_ID, RULE_NAME, SEVERITY, STATUS, OBSERVED_VALUE, THRESHOLD, CHECKED_AT "
        "FROM OPS.VW_DQ_LATEST ORDER BY RULE_ID"
    )
    names = [d[0].lower() for d in cursor.description]
    return [
        {k: (str(v) if k == "checked_at" else v) for k, v in zip(names, row, strict=False)}
        for row in cursor.fetchall()
    ]
