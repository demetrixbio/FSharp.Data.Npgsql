FROM postgis/postgis:13-3.0-alpine

ENV POSTGRES_DB dvdrental
ENV POSTGRES_PASSWORD postgres

COPY ./dvdrental.tar /var/lib/postgresql/backup/dvdrental.tar
COPY ./restore_dvdrental.sh /docker-entrypoint-initdb.d/restore_dvdrental.sh

HEALTHCHECK --interval=10s --timeout=5s CMD pg_isready
