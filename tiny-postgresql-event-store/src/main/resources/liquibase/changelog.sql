--liquibase formatted sql
--changeset vekajp:es-001

create sequence if not exists ${schema}.event_record_created_at_sequence;

create table if not exists ${schema}.event_record
(
    id                   text primary key,
    aggregate_table_name text,
    aggregate_id         text,
    aggregate_version    bigint,
    event_title          text,
    payload              text,
    saga_context         text,
    created_at           bigint default nextval('${schema}.event_record_created_at_sequence'),
    unique (aggregate_id, aggregate_version)
);

CREATE INDEX idx_created_at ON ${schema}.event_record(aggregate_table_name, created_at);
CREATE INDEX idx_aggregate_id_version ON ${schema}.event_record(aggregate_id, aggregate_version);

create table if not exists ${schema}.snapshot
(
    id                         text primary key,
    snapshot_table_name        text,
    aggregate_state_class_name text,
    snapshot                   text,
    version                    bigint
);

create table if not exists ${schema}.event_stream_read_index
(
    id         text primary key,
    read_index bigint,
    version    bigint
);

create table if not exists ${schema}.event_stream_active_readers
(
    id               text primary key,
    version          bigint,
    reader_id        text,
    read_position    bigint,
    last_interaction bigint
);

CREATE OR REPLACE FUNCTION ${schema}.execute_batch(
    p_commands TEXT[]
) RETURNS INT[] LANGUAGE plpgsql AS '
DECLARE
    p_results INT[] := ''{}'';
    i INT;
BEGIN
    FOR i IN array_lower(p_commands, 1) .. array_upper(p_commands, 1) LOOP
            BEGIN
                EXECUTE p_commands[i];
            EXCEPTION
                WHEN OTHERS THEN
                    p_results := array_append(p_results, i);
            END;
        END LOOP;
    RETURN p_results;
END;';