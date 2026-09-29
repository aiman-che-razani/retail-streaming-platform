"""`retail-snowflake-migrate` — bootstrap the account, apply migrations, control tasks.

    SNOWFLAKE_ADMIN_ROLE=ACCOUNTADMIN retail-snowflake-migrate bootstrap   # once
    retail-snowflake-migrate status        # list pending scripts
    retail-snowflake-migrate apply         # apply V### then changed R__ scripts
    retail-snowflake-migrate tasks --resume|--suspend

Runs as the ADMIN identity (SNOWFLAKE_ADMIN_*), never as the loader service user.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from retail_platform.cli.common import EXIT_OK, run_main
from retail_platform.config.settings import SnowflakeAdminSettings, SnowflakeSettings
from retail_platform.errors import ConfigurationError

SNOWFLAKE_DIR = Path("snowflake")
ROOT_TASK = "OPS.TASK_PIPELINE_ROOT"
SNAPSHOT_TASK = "OPS.TASK_INVENTORY_SNAPSHOT"


def _public_key_body(path: Path) -> str:
    if not path.is_file():
        raise ConfigurationError(
            f"loader public key not found at {path} - run scripts/snowflake_keypair.sh first"
        )
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    return "".join(line for line in lines if not line.startswith("-----"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-snowflake-migrate", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    boot = sub.add_parser("bootstrap", help="account objects (ACCOUNTADMIN, once)")
    boot.add_argument("--public-key", type=Path, default=Path("secrets/snowflake_loader_key.pub"))
    boot.add_argument("--credit-quota", type=int, default=20)
    sub.add_parser("status")
    apply = sub.add_parser("apply")
    apply.add_argument("--dry-run", action="store_true")
    tasks = sub.add_parser("tasks")
    group = tasks.add_mutually_exclusive_group(required=True)
    group.add_argument("--resume", action="store_true")
    group.add_argument("--suspend", action="store_true")
    args = parser.parse_args(argv)

    def body() -> int:
        from retail_platform.warehouse.connection import connect, connection_params
        from retail_platform.warehouse.migrations import MigrationRunner, run_bootstrap

        account, admin = SnowflakeSettings(), SnowflakeAdminSettings()
        if not admin.has_credentials:
            raise ConfigurationError(
                "set SNOWFLAKE_ADMIN_USER and SNOWFLAKE_ADMIN_PASSWORD/_AUTHENTICATOR/"
                "_PRIVATE_KEY_PATH (see docs/runbooks/snowflake-setup.md)"
            )
        variables = {"DATABASE": account.database}
        if args.command == "bootstrap":
            variables |= {
                "LOADER_PUBLIC_KEY": _public_key_body(args.public_key),
                "ADMIN_USER": admin.user,
                "MONTHLY_CREDIT_QUOTA": str(args.credit_quota),
            }
            # Bootstrap creates the warehouse and database, so it cannot select them up front.
            params = connection_params(account, admin, component="bootstrap")
            params.pop("database"), params.pop("schema"), params.pop("warehouse")
            with connect(params) as conn:
                run_bootstrap(conn, SNOWFLAKE_DIR, variables)
            return EXIT_OK

        with connect(connection_params(account, admin, component="migrations")) as conn:
            if args.command == "tasks":
                db = account.database
                cursor = conn.cursor()
                if args.resume:
                    # Resumes the root and every dependent task in the graph.
                    cursor.execute(f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{db}.{ROOT_TASK}')")
                    cursor.execute(f"ALTER TASK {db}.{SNAPSHOT_TASK} RESUME")
                else:
                    cursor.execute(f"ALTER TASK {db}.{ROOT_TASK} SUSPEND")
                    cursor.execute(f"ALTER TASK {db}.{SNAPSHOT_TASK} SUSPEND")
                return EXIT_OK
            runner = MigrationRunner(conn, SNOWFLAKE_DIR, variables)
            if args.command == "status":
                pending = [s.path.name for s in runner.pending()]
                sys.stdout.write(json.dumps({"pending": pending}, indent=2) + "\n")
                return EXIT_OK
            runner.apply(dry_run=args.dry_run)
            return EXIT_OK

    return run_main("retail-snowflake-migrate", body, logs_to_stderr=True)


if __name__ == "__main__":
    sys.exit(main())
