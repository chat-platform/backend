FROM postgres:16-alpine
COPY shared/migrations/000001_init.up.sql /migrations/000001_init.up.sql
