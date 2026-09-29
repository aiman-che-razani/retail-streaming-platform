#!/usr/bin/env sh
# Generate an encrypted RSA key pair for the Snowflake loader service user (key-pair auth).
#   SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=... scripts/snowflake_keypair.sh
# Writes secrets/snowflake_loader_key.p8 (private, encrypted PKCS#8) and .pub (public).
# The secrets/ directory is git-ignored. Never commit these files.
set -eu

: "${SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:?set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (see .env)}"
out_dir="${1:-secrets}"
mkdir -p "$out_dir"
private="$out_dir/snowflake_loader_key.p8"
public="$out_dir/snowflake_loader_key.pub"

if [ -f "$private" ]; then
  echo "$private already exists - refusing to overwrite (rotate deliberately, see runbook)" >&2
  exit 1
fi

openssl genrsa 2048 2>/dev/null \
  | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out "$private" \
      -passout env:SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
openssl rsa -in "$private" -passin env:SNOWFLAKE_PRIVATE_KEY_PASSPHRASE -pubout -out "$public" 2>/dev/null
chmod 600 "$private"
echo "Created $private and $public"
