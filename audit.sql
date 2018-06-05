/*
 * Copyright (c) 2017-2018 Two Sigma Open Source, LLC.
 * All Rights Reserved
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL TWO SIGMA OPEN SOURCE, LLC BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF TWO SIGMA OPEN SOURCE, LLC HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * TWO SIGMA OPEN SOURCE, LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
 * BASIS, AND TWO SIGMA OPEN SOURCE, LLC HAS NO OBLIGATIONS TO PROVIDE
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

\set ON_ERROR_STOP on
\set ROOT `echo "$ROOT"`
\i :ROOT/backend/preamble.sql

/*
 * This file arranges for all tables in elected SCHEMAs to have a corresponding
 * table in a SCHEMA named "<original>_audit" that tracks all inserts/updates/
 * deletes on the source table.
 *
 * This will audit all TABLEs in the SCHEMA "my_schema":
 *
 *   INSERT INTO audit_ctl.audit_schemas(schema_name) VALUES ('my_schema');
 *
 * This will audit the TABLE "other_schema"."my_table":
 *
 *   INSERT INTO audit_ctl.audit_tables(schema_name, table_name)
 *   VALUES ('other_schema','my_table');
 *
 * You can also exclude tables from auditing:
 *
 *   INSERT INTO audit_ctl.audit_tables(schema_name, table_name, include)
 *   VALUES ('my_schema','some_table',false);
 *
 * This is home-grown audit functionality.  All inspected open source PG audit
 * extensions that record old/new rows in audit tables use row_to_json() to
 * encode old/new row values.  Using row_to_json() makes it difficult or
 * impossible to use old/new row values in relational queries.
 *
 * Contents:
 *
 *  - an event trigger and procedure for creating TABLEs, trigger procedures,
 *    and TRIGGERs for auditing tables in selected SCHEMAs
 *
 *    Auditing will be setup automatically for all TABLEs in selected SCHEMAs
 *    as each table is created.
 *
 *    If an audited TABLE is DROPped, then the corresponding audit table also
 *    WILL be dropped.
 *
 *  - tables of the same names as those in the audited schema, with the
 *    following columns:
 *
 *     _txid        -- transaction ID
 *     _timestamp   -- start of transaction
 *     _by          -- pg role (usually a Kerberos principal) who did this
 *     _op          -- 'INSERT', 'UPDATE', or 'DELETE'
 *     old_record   -- prev value of the  updated/deleted row; null for insert
 *     new_record   -- new  value of the inserted/updated row; null for delete
 *
 *    The "old_record" and "new_record" columns are of the base table's
 *    composite type.  (Note that if the base table is ALTERed to add or remove
 *    columns, then the audit records are automatically also altered
 *    accordingly.  This happens automatically because PG will notice the
 *    presence/absence of trailing new columns -corresponding to added columns-
 *    in record values, while dropped columns remain in the storage format but
 *    are logically deleted.)
 *
 *    The use of the base table's record/composite type allows one to do
 *    queries based on those records.  For example,
 *
 *      SELECT t._txid, (t.new_record).id
 *      FROM foo_schema.foo_tbl t
 *      WHERE t.new_record IS DISTINCT FROM NULL AND
 *            (t.new_record).some_column = 'some_value'
 *      ORDER BY t._txid DESC;
 *
 * TODO:
 *
 *  - Record all DDLs on a schema.  This might be too general for this
 *    particular audit system.
 *
 *  - Use JSONB and to_jsonb() for the _all audit tables.
 *
 *  - Get rid of the audit_ctl._tbls table and all INSERTs/UPDATEs/DELETEs on
 *    it.
 *
 *  - Handle DROP SCHEMA events: drop the _audit schema for any base schema
 *    that is dropped.
 */

/*
 * Theory of operation:
 *
 *  - we use an EVENT TRIGGER to create <schema>_audit.<table> to match
 *    <schema>.<table> as the latter are created
 *
 *  - because EVENT TRIGGER procedures don't get enough information provided to
 *    them, they must discover what has been done (and thus what to do) by
 *    querying the "pg_catalog" tables.
 *
 *  - for each table in an audited schema we create a corresponding audit
 *    table, trigger procedure, and trigger for keeping the audit table updated
 *
 *  - triggers created by this are named x*, so that they run after all other
 *    triggers on the same tables
 *
 * NOTE WELL: The functions defined here are SECURITY DEFINER functions.
 *            This means that they run as the user that created them, and this
 *            means that current_user in their bodies will be... the definer
 *            user, not the calling (invoker) user.  There seems to be no way
 *            to get the calling user (checked the sources; a patch would be
 *            simple)...
 *
 *            Therefore we use session_user instead of current_user.  But this
 *            requires that the application have done SET SESSION AUTHORIZATION
 *            when impersonating users, otherwise the wrong user will be logged
 *            here.
 */

SET client_min_messages TO NOTICE;

CREATE SCHEMA IF NOT EXISTS audit_ctl;
SET search_path = "audit_ctl";

CREATE TABLE IF NOT EXISTS audit_ctl.audit_schemas (
    schema_name     TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS audit_ctl.audit_tables (
    schema_name     TEXT,
    table_name      TEXT,
    include         BOOLEAN DEFAULT(true),
    PRIMARY KEY (schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS audit_ctl._all (
    _txid               BIGINT DEFAULT(txid_current()),
    _txtime             TIMESTAMP WITHOUT TIME ZONE DEFAULT(current_timestamp),
    _timestamp          TIMESTAMP WITHOUT TIME ZONE DEFAULT(clock_timestamp()),
    /*
     * NOTE WELL: See note above about current_user vs. session_user in the
     *            context of SECURITY DEFINER functions.
     */
    _by                 TEXT DEFAULT(session_user),
    _op                 TEXT NOT NULL,
    _schema             TEXT NOT NULL,
    _table              TEXT NOT NULL,
    old_record          TEXT,
    new_record          TEXT,
    UNIQUE              (_txid, new_record)
);

CREATE INDEX IF NOT EXISTS audit_all_by_tx ON audit_ctl._all
    (_txid ASC, _timestamp ASC);
CREATE INDEX IF NOT EXISTS audit_all_by_by ON audit_ctl._all
    (_by, _txid ASC, _timestamp ASC);

DROP EVENT TRIGGER IF EXISTS make_audit_trigger CASCADE;

/* Generator of generic AFTER-DML-on-any-table-for-each-row audit trigger */
CREATE OR REPLACE FUNCTION audit_ctl.make_audit_trigger(
    _schema_name    TEXT,
    _table_name     TEXT)
RETURNS void AS $$
BEGIN
RAISE NOTICE 'Creating trigger procedure "trig_f_audit_% on %.%',
    _table_name, _schema_name, _table_name;
EXECUTE format(
    $fmt$

    CREATE OR REPLACE FUNCTION %1$I.%3$I()
    RETURNS TRIGGER AS $qq$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            RAISE DEBUG 'Trigger %% (procedure %%.%%) %% %% ON %%.%%; txid %%, start time %%; NEW row = %%',
                TG_NAME, %1$L, %2$L, TG_WHEN, TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME,
                txid_current(), clock_timestamp(), NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            RAISE DEBUG 'Trigger %% (procedure %%.%%) %% %% ON %%.%%; txid %%, start time %%; OLD / NEW row = %% / %%',
                TG_NAME, %1$L, %2$L, TG_WHEN, TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME,
                txid_current(), clock_timestamp(), OLD, NEW;
        ELSE
            RAISE DEBUG 'Trigger %% (procedure %%.%%) %% %% ON %%.%%; txid %%, start time %%; OLD row = %%',
                TG_NAME, %1$L, %2$L, TG_WHEN, TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME,
                txid_current(), clock_timestamp(), OLD;
        END IF;

        /*
         * XXX We need to distinguish (using a string argument from the CREATE
         * TRIGGER) between immediate and deferred invocation.
         *
         * We then need to merge all DMLs affecting a single row into a single
         * audit row for the relvant audit tables.
         *
         * We need to record not just old/new row values, but old/initial_new/
         * final_new row values.
         *
         *  - An INSERT/UPDATE/DELETE sequence should record (NULL,
         *    <as-inserted>, NULL).
         *  - A DELETE/re-INSERT/UPDATE sequence should record (<old>,
         *    <as-inserted>, <as-updated>).
         *  - An UPDATE/DELETE sequence should be (<old>, <as-updated>, NULL).
         *  - An UPDATE/DELETE/re-INSERT sequence should be (<old>, <as-updated>,
         *    <as-inserted>).
         *
         * For convenience the final new record column should stay
         * "new_record".
         */

        /* Durable, JSON-encoded audit trail */
        INSERT INTO %1$I._all (_op, _schema, _table, old_record, new_record)
        SELECT TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME,
               CASE TG_OP WHEN 'INSERT' THEN NULL ELSE row_to_json(OLD) END,
               CASE TG_OP WHEN 'DELETE' THEN NULL ELSE row_to_json(NEW) END
        ON CONFLICT (_txid, new_record) DO UPDATE
            SET new_record = row_to_json(NEW);

        /*
         * Relational audit trail, subject to dropping when the base table is
         * dropped.
         */
        INSERT INTO %1$I.%2$I (_op, old_record, new_record)
        SELECT TG_OP,
               CASE TG_OP WHEN 'INSERT' THEN NULL ELSE OLD END,
               CASE TG_OP WHEN 'DELETE' THEN NULL ELSE NEW END
        ON CONFLICT (_txid, new_record) DO UPDATE
            SET new_record = NEW;

        /* Notify anyone who might be curious, mostly data gen service */
        PERFORM pg_notify(TG_TABLE_SCHEMA || '_channel', TG_TABLE_NAME);

        RETURN CASE TG_OP WHEN 'DELETE' THEN OLD ELSE NEW END;
    END; $qq$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path = %1$I,%4$I;

    $fmt$, _schema_name || '_audit', _table_name,
    'trig_f_audit_' || _table_name, _schema_name);

    RAISE NOTICE 'Creating trigger "adtrig_audit_% on %.%',
        _table_name, _schema_name, _table_name;
    EXECUTE format(
        $fmt$

        DROP TRIGGER IF EXISTS %4$I ON %1$I.%3$I CASCADE;
        CREATE CONSTRAINT TRIGGER %4$I
        AFTER INSERT OR UPDATE OR DELETE
        ON %1$I.%3$I
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE PROCEDURE %2$I.%5$I();

        $fmt$,
        _schema_name, _schema_name || '_audit', _table_name,
        'adtrig_audit_' || _table_name, 'trig_f_audit_' || _table_name);

    RAISE NOTICE 'Creating trigger "atrig_audit_% on %.%',
        _table_name, _schema_name, _table_name;
    EXECUTE format(
        $fmt$

        DROP TRIGGER IF EXISTS %4$I ON %1$I.%3$I CASCADE;
        CREATE TRIGGER %4$I
        AFTER INSERT OR UPDATE OR DELETE
        ON %1$I.%3$I
        FOR EACH ROW
        EXECUTE PROCEDURE %2$I.%5$I();

        $fmt$,
        _schema_name, _schema_name || '_audit', _table_name,
        'atrig_audit_' || _table_name, 'trig_f_audit_' || _table_name);
END; $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path FROM CURRENT;

/*
 * This should be a temp table created in make_audit_tables_and_triggers()
 * below, however, that triggers infinite recursion, so it has to exist before
 * that function is ever invoked, which means it can't be TEMP.
 */
CREATE TABLE IF NOT EXISTS audit_ctl._tbls (
    schema_name TEXT,
    table_name  TEXT,
    ready       BOOLEAN DEFAULT (FALSE),
    dropping    BOOLEAN DEFAULT (FALSE),
    dropped     BOOLEAN DEFAULT (FALSE),
    PRIMARY     KEY(schema_name, table_name)
);

CREATE OR REPLACE VIEW audit_ctl._tbls_view AS
SELECT q.schema_name AS schema_name,
       q.table_name AS table_name,
       bool_and(q.ready) AS ready,
       bool_or(q.dropped) AS dropped
FROM (
/* Tables that need or have an audit table */
SELECT q.schema_name AS schema_name,
       q.table_name AS table_name,
       q.ready AS ready,
       FALSE AS dropped
FROM (
    /*
     * Select tables that exist and which are listed in audit_ctl.audit_tables
     * or which are in schemas that are listed in audit_ctl.audit_schemas.
     */
    SELECT n.nspname::text, c.relname::text,
           EXISTS ( /* true if the audit trigger for this table exists */
            SELECT *
            FROM pg_trigger t
            WHERE t.tgrelid = c.oid AND
                  t.tgname  = 'atrig_audit_' || c.relname)
    FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid
    WHERE n.nspname <> 'audit_ctl'          AND
          n.nspname NOT LIKE '%\_audit'     AND
          c.relname <> 'base_table'         AND
          c.relname <> 'txids'              AND
          c.relname <> '_tbls'              AND
          c.relname <> '_all'               AND
          c.relkind = 'r'                   AND
          c.relname NOT LIKE '%_new'        AND
          c.relname NOT LIKE '%_updates'    AND
          c.relname NOT LIKE '%_deltas'     AND
          ROW(n.nspname, c.relname) NOT IN (
                SELECT a.schema_name, a.table_name
                FROM audit_ctl.audit_tables a
                WHERE NOT coalesce(a.include, true)
          ) AND (
              n.nspname IN (SELECT schema_name FROM audit_ctl.audit_schemas) OR
              ROW(n.nspname, c.relname) IN (
                            SELECT a.schema_name, a.table_name
                            FROM audit_ctl.audit_tables a
                            WHERE coalesce(a.include, true)))
    ) q(schema_name, table_name, ready)
UNION ALL /* add tables to drop */
SELECT q.schema_name, q.table_name, true, true
FROM (
    /* Select tables for which an audit table exists without a source table */
    SELECT substring(n.nspname, 1, position('_audit' in n.nspname) - 1),
           c.relname::text
    FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid
    WHERE n.nspname LIKE '%\_audit'         AND
          c.relname <> 'base_table'         AND
          c.relname <> 'txids'              AND
          c.relname <> '_tbls'              AND
          c.relname <> '_all'               AND
          c.relkind = 'r'                   AND
          c.relname NOT LIKE '%_new'        AND
          c.relname NOT LIKE '%_updates'    AND
          c.relname NOT LIKE '%_deltas'     AND
          NOT EXISTS (
            SELECT c2.*
            FROM pg_namespace n2 JOIN pg_class c2 ON c2.relnamespace = n2.oid
            WHERE n2.nspname = substring(n.nspname, 1,
                                         position('_audit' in n.nspname) - 1) AND
                  c2.relname = c.relname)
        ) q(schema_name, table_name)
) q(schema_name, table_name, ready, dropped)
GROUP BY schema_name, table_name;

/*
 * Event trigger to make sure *every* table that needs it gets an audit
 * trigger.
 */
CREATE OR REPLACE FUNCTION audit_ctl.make_audit_tables_and_triggers()
RETURNS void AS $$
DECLARE
    schema_sub record;
    tbl record;
BEGIN
    FOR schema_sub IN (SELECT schema_name FROM audit_ctl.audit_schemas
                       UNION
                       SELECT schema_name FROM audit_ctl.audit_tables)
    LOOP
        CONTINUE WHEN EXISTS (
                SELECT *
                FROM pg_catalog.pg_namespace n
                WHERE n.nspname = schema_sub.schema_name
            );
        EXECUTE format($q$
                CREATE SCHEMA IF NOT EXISTS %1$I;
                CREATE TABLE IF NOT EXISTS %1$I._all (
                    LIKE audit_ctl._all INCLUDING ALL
                );
            $q$, schema_sub.schema_name || '_audit');
    END LOOP;

    INSERT INTO audit_ctl._tbls (schema_name, table_name,
                                 ready, dropping, dropped)
    SELECT q.schema_name, q.table_name, q.ready, false, q.dropped
    FROM audit_ctl._tbls_view q
    ON CONFLICT DO NOTHING;

    UPDATE audit_ctl._tbls t
    SET ready =
           EXISTS ( /* true if the audit trigger for this table exists */
            SELECT *
            FROM pg_trigger tg
            JOIN pg_class c ON c.oid = tg.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = t.table_name AND
                  tg.tgname  = 'atrig_audit_' || t.table_name),
        dropped = coalesce(
            (
                SELECT q.dropped
                FROM audit_ctl._tbls_view q
                WHERE t.schema_name = q.schema_name AND
                      t.table_name = q.table_name), true);

    FOR tbl IN (SELECT t.* FROM audit_ctl._tbls t)
    LOOP
        IF tbl.dropped AND NOT tbl.dropping THEN
            RAISE NOTICE 'Dropping audit table %.%',
                tbl.schema_name, tbl.table_name;
            UPDATE audit_ctl._tbls
            SET dropping = TRUE
            WHERE dropped;
            EXECUTE format($q$
                DROP TRIGGER IF EXISTS %3$I ON %2$I.%2$I CASCADE;
            $q$, tbl.schema_name || '_audit', tbl.table_name,
                 'atrig_audit_' || t.table_name);
            EXECUTE format($q$
                DROP TRIGGER IF EXISTS %3$I ON %2$I.%2$I CASCADE;
            $q$, tbl.schema_name || '_audit', tbl.table_name,
                 'adtrig_audit_' || t.table_name);
            EXECUTE format($q$
                DROP TABLE IF EXISTS %1$I.%2$I CASCADE;
            $q$, tbl.schema_name || '_audit', tbl.table_name);
            DELETE FROM audit_ctl._tbls t
            WHERE t.schema_name = tbl.schema_name AND t.table_name = tbl.table_name;
            INSERT INTO audit_ctl._all
                (_by, _op, _schema, _table, old_record)
            SELECT session_user, 'DROP TABLE', tbl.schema_name, tbl.table_name,
                json_build_object('schema_name', tbl.schema_name,
                                  'table_name', tbl.table_name)
            ON CONFLICT (_txid, new_record) DO NOTHING;
            CONTINUE;
        END IF;

        /* Prevent infinite recursion */
        CONTINUE WHEN tbl.ready OR EXISTS (
            SELECT c.relname
            FROM pg_namespace n
            JOIN pg_class c ON c.relnamespace = n.oid
            WHERE n.nspname = tbl.schema_name || '_audit' AND
                  c.relname = tbl.table_name
        );
        RAISE NOTICE 'Creating audit table %_audit.%',
            tbl.schema_name, tbl.table_name;
        EXECUTE format($q$
                CREATE SCHEMA IF NOT EXISTS %2$I;
                CREATE TABLE IF NOT EXISTS %2$I._all (
                    LIKE audit_ctl._all INCLUDING ALL
                );
                CREATE TABLE IF NOT EXISTS %2$I.%3$I (
                    _txid BIGINT DEFAULT(txid_current()),
                    _txtime    TIMESTAMP WITHOUT TIME ZONE DEFAULT(current_timestamp),
                    _timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT(clock_timestamp()),
                    _by TEXT DEFAULT(session_user),
                    _op TEXT NOT NULL,
                    old_record %1$I.%3$I,
                    new_record %1$I.%3$I,
                    UNIQUE (_txid, new_record)
                    /* Note: no primary key here */
                );
                CREATE INDEX IF NOT EXISTS %4$I ON %2$I.%3$I
                    (_txid ASC, _timestamp ASC);
                CREATE INDEX IF NOT EXISTS %5$I ON %2$I.%3$I
                    (_by, _txid ASC, _timestamp ASC);
                CREATE INDEX IF NOT EXISTS %6$I ON %2$I.%3$I
                    (old_record, new_record);
                CREATE INDEX IF NOT EXISTS %7$I ON %2$I.%3$I
                    (new_record, old_record);
                CREATE UNIQUE INDEX IF NOT EXISTS %8$I ON %2$I.%3$I
                    (_txid, new_record);
            $q$, tbl.schema_name, tbl.schema_name || '_audit', tbl.table_name,
            'audit_idx_by_tx_'      || tbl.table_name,
            'audit_idx_by_by_'      || tbl.table_name,
            'audit_idx_by_oldnew_'  || tbl.table_name,
            'audit_idx_by_newold_'  || tbl.table_name,
            'audit_uidx_by_newtx_'  || tbl.table_name
            );
        PERFORM audit_ctl.make_audit_trigger(tbl.schema_name, tbl.table_name);
        UPDATE audit_ctl._tbls
        SET ready = TRUE;
        INSERT INTO audit_ctl._all
            (_by, _op, _schema, _table, new_record)
        SELECT session_user,
            'CREATE TABLE', tbl.schema_name, tbl.table_name,
            json_build_object('schema_name', tbl.schema_name,
                              'table_name', tbl.table_name)
        ON CONFLICT DO NOTHING;
    END LOOP;

    RETURN;
END; $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path FROM CURRENT;

CREATE OR REPLACE FUNCTION audit_ctl.event_trig_f_audit()
RETURNS event_trigger AS $$
DECLARE tbl record;
BEGIN
    IF current_setting('audit_ctl.enter',true) IS NULL OR
       current_setting('audit_ctl.enter',true) = '' OR
       NOT current_setting('audit_ctl.enter',true)::boolean THEN
        /* Avoid infinite recursion */
        PERFORM set_config('audit_ctl.enter','true',false);
        PERFORM audit_ctl.make_audit_tables_and_triggers();
        PERFORM set_config('audit_ctl.enter','false',false);
    END IF;
END; $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path FROM CURRENT;

/* Arrange for all future new tables in audited schemas to be audited */
CREATE EVENT TRIGGER make_audit_trigger ON ddl_command_end
WHEN tag IN ('CREATE TABLE', 'DROP TABLE')
EXECUTE PROCEDURE audit_ctl.event_trig_f_audit();

/* Create all audit tables now */
SELECT audit_ctl.make_audit_tables_and_triggers();

SET client_min_messages TO WARNING;
