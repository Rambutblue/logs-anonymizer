FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.6.1 pycapnp==2.0.0 clickhouse-connect==0.8.9

# pycapnp cannot resolve the Go annotations, so strip them; the schema stays the single source of truth
COPY http_log.capnp /tmp/http_log.capnp
RUN sed -e '/go.capnp/d' -e '/\$Go\./d' /tmp/http_log.capnp > /app/http_log.capnp

COPY test/common.py test/producer.py test/checker.py ./
