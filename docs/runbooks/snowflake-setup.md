# Runbook: Snowflake setup (one time, ~15 minutes)

Everything except the warehouse runs without Snowflake. Follow this when you want the RAW → STAGING → ANALYTICS path.

## 1. Account

1. Create a trial account at <https://signup.snowflake.com> (Enterprise edition, any cloud/region; `AWS ap-southeast-1` is closest to Malaysia).
2. Find the **account identifier**: Snowsight → bottom-left account menu → *Connect a tool to Snowflake* → `ORGNAME-ACCOUNTNAME`.

## 2. Identities

Two identities, following least privilege (ADR-005):

| Identity | Used for | Auth | `.env` variables |
|---|---|---|---|
| **You** (the trial login user) | bootstrap (as `ACCOUNTADMIN`, once) and migrations (as `RETAIL_ADMIN`) | password + MFA via browser (`externalbrowser`) or password | `SNOWFLAKE_ADMIN_USER`, `SNOWFLAKE_ADMIN_AUTHENTICATOR` or `SNOWFLAKE_ADMIN_PASSWORD` |
| **`RETAIL_LOADER_SVC`** (created by bootstrap) | the loader container | RSA key pair, no password possible (`TYPE = SERVICE`) | `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` |

Generate the loader key pair (Git Bash or Linux/macOS, needs `openssl`):

```bash
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='<a long random passphrase>'   # also put it in .env
scripts/snowflake_keypair.sh        # -> secrets/snowflake_loader_key.p8 and .pub (git-ignored)
```

Fill `.env`:

```dotenv
SNOWFLAKE_ACCOUNT=ORGNAME-ACCOUNTNAME
SNOWFLAKE_ADMIN_USER=<your login>
SNOWFLAKE_ADMIN_AUTHENTICATOR=externalbrowser     # or SNOWFLAKE_ADMIN_PASSWORD=...
SNOWFLAKE_USER=RETAIL_LOADER_SVC
SNOWFLAKE_PRIVATE_KEY_PATH=secrets/snowflake_loader_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<same passphrase>
```

## 3. Bootstrap (ACCOUNTADMIN, once)

```bash
SNOWFLAKE_ADMIN_ROLE=ACCOUNTADMIN uv run retail-snowflake-migrate bootstrap --credit-quota 20
```

This creates the roles, the two XSMALL warehouses (60 s auto-suspend), the resource monitor (20 credits/month by default), the database and schemas, and the loader service user with your public key. It is idempotent, so it's safe to re-run.

## 4. Migrations (RETAIL_ADMIN)

```bash
uv run retail-snowflake-migrate status     # what would run
make snowflake-migrate                     # apply V### then changed R__ scripts
```

## 5. Load and transform

```bash
make load                      # one loader cycle now (PUT + COPY)
make up-all                    # or run the loader container every 15 minutes
make snowflake-tasks-resume    # start the task graph (credits are consumed only when streams have data)
make snowflake-tasks-suspend   # stop it when you're done for the day
```

## Cost checklist

- Warehouses auto-suspend after 60 s; an idle loader cycle never connects.
- Tasks skip runs when their streams are empty (`WHEN SYSTEM$STREAM_HAS_DATA`).
- **Suspend tasks when not developing.** Check spend: Snowsight → Admin → Cost management, or `SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY ORDER BY START_TIME DESC;`
- The resource monitor suspends the warehouses at the quota.

## Key rotation

Snowflake supports two public keys per user (`RSA_PUBLIC_KEY` and `RSA_PUBLIC_KEY_2`). Generate a new pair into another directory, `ALTER USER RETAIL_LOADER_SVC SET RSA_PUBLIC_KEY_2 = '...'`, switch `.env`, then `UNSET RSA_PUBLIC_KEY`.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `JWT token is invalid` | The public key wasn't registered, or `SNOWFLAKE_USER` is wrong. `DESC USER RETAIL_LOADER_SVC` shows `RSA_PUBLIC_KEY_FP`. |
| `Warehouse ... suspended` / queries cancelled | The resource monitor hit its quota. Raise it: `ALTER RESOURCE MONITOR RETAIL_MONTHLY_MONITOR SET CREDIT_QUOTA = 30;` |
| `... was modified after being applied` | A `V###` file was edited. Revert it and add a new migration instead. |
| Loader logs `snowflake_unavailable` | Network or outage. Batches stay in `data/landing` and are retried with backoff (FM-11). |
