# Spark image: Python 3.12 + PySpark 4.2 + JRE 21, running Spark in local mode (ADR-004).
# Connector jars are resolved at BUILD time and copied into pyspark/jars so containers never
# download dependencies at runtime (reproducible, works offline).
FROM eclipse-temurin:21-jre-noble AS jre

FROM python:3.14-slim-bookworm AS runtime

ARG SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.13"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    JAVA_HOME=/opt/java/openjdk \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PATH=/opt/venv/bin:/opt/java/openjdk/bin:$PATH

COPY --from=jre /opt/java/openjdk /opt/java/openjdk

# procps: Spark's launch scripts call `ps`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir uv==0.12.20 \
 && useradd --uid 10001 --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --extra spark

# Resolve connector jars (and transitive deps) once, then place them on Spark's classpath.
RUN python - <<EOF
import glob, os, shutil
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[1]")
         .config("spark.jars.packages", "${SPARK_PACKAGES}")
         .config("spark.jars.ivy", "/tmp/ivy").getOrCreate())
spark.stop()
import pyspark
target = os.path.join(os.path.dirname(pyspark.__file__), "jars")
bundled = set(os.listdir(target))
for jar in glob.glob("/tmp/ivy/jars/*.jar"):
    # Ivy names jars "<group>_<artifact>-<version>.jar"; PySpark bundles "<artifact>-<version>.jar".
    plain = os.path.basename(jar).split("_", 1)[1]
    if plain in bundled:
        print("already bundled", plain)
        continue
    shutil.copy(jar, os.path.join(target, plain))
    print("added", plain)
EOF
RUN rm -rf /tmp/ivy

COPY src ./src
RUN uv sync --frozen --no-dev --extra spark
COPY kafka ./kafka
COPY spark ./spark

RUN mkdir -p /app/data && chown -R app:app /app/data
USER app

# Test stage: same runtime plus pytest, used by `make test-spark` (JVM-dependent tests).
FROM runtime AS test
USER root
RUN uv sync --frozen --extra spark --group dev
COPY tests ./tests
RUN chown -R app:app /app
USER app
