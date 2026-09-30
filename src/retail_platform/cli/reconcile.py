"""`retail-reconcile` — end-to-end reconciliation and data-quality report (JSON on stdout).

    retail-reconcile report            # Kafka -> Spark -> RAW -> STAGING accounting + latest DQ
    retail-reconcile report --run-dq   # run OPS.SP_RUN_DQ_CHECKS() first

Exit code 1 when any topic needs investigation or an ERROR-severity DQ rule failed, so the
command can gate CI/cron jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from retail_platform.cli.common import EXIT_FAILURE, EXIT_OK, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings, SnowflakeAdminSettings, SnowflakeSettings
from retail_platform.errors import ConfigurationError
from retail_platform.observability.logging import get_logger


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-reconcile", description=__doc__)
    parser.add_argument("command", choices=["report"])
    parser.add_argument("--run-dq", action="store_true", help="execute the DQ procedure first")
    args = parser.parse_args(argv)

    def body() -> int:
        from confluent_kafka import Consumer

        from retail_platform.messaging.dlq import consumer_config
        from retail_platform.quality.reconciliation import as_dict, reconcile
        from retail_platform.quality.sources import gather, latest_dq_results
        from retail_platform.warehouse.connection import connect, connection_params

        account, admin, kafka = SnowflakeSettings(), SnowflakeAdminSettings(), KafkaSettings()
        if not (account.account and admin.has_credentials):
            raise ConfigurationError("reconciliation needs SNOWFLAKE_ACCOUNT and SNOWFLAKE_ADMIN_*")
        catalog = load_catalog(kafka)
        consumer = Consumer(consumer_config(kafka, f"reconcile-{uuid.uuid4().hex[:8]}"))
        try:
            with connect(connection_params(account, admin, component="reconcile")) as conn:
                cursor = conn.cursor()
                if args.run_dq:
                    cursor.execute(f"CALL {account.database}.OPS.SP_RUN_DQ_CHECKS()")
                topics = [as_dict(reconcile(i)) for i in gather(cursor, consumer, catalog)]
                dq = latest_dq_results(cursor)
        finally:
            consumer.close()

        failing_dq = [r for r in dq if r["status"] == "FAIL" and r["severity"] == "ERROR"]
        investigate = [t["topic"] for t in topics if t["status"] != "OK"]
        report = {"topics": topics, "data_quality": dq}
        sys.stdout.write(json.dumps(report, indent=2, default=str) + "\n")
        log = get_logger("retail-reconcile")
        if investigate or failing_dq:
            log.error(
                "reconciliation_failed",
                topics=investigate,
                dq_rules=[r["rule_id"] for r in failing_dq],
                status="failed",
            )
            return EXIT_FAILURE
        log.info("reconciliation_ok", status="ok")
        return EXIT_OK

    return run_main("retail-reconcile", body, logs_to_stderr=True)


if __name__ == "__main__":
    sys.exit(main())
