/*
 * Copyright (c) 2017 Two Sigma Open Source, LLC.
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

 * This file demonstrates how to create COMMIT, BEGIN, and session
 * CONNECT TRIGGERs for PostgreSQL using PlPgSQL only, via normal and
 * CONSTRAINT TRIGGERs.
 *
 * There have been many threads on the PG mailing lists about commit
 * triggers, with much skepticism shown about the possible semantics of
 * such a thing.
 *
 * Below we list use cases, and demonstrate reasonable, useful, and
 * desirable semantics.
 *
 * There is one shortcoming of this implementation: it is inefficient
 * because it has to use FOR EACH ROW triggers under the hood, so if you
 * do 1,000 inserts, then 999 of the resulting internal trigger
 * procedure invocations will be unnecessary.  These are FOR EACH ROW
 * triggers because that is the only level permitted for CONSTRAINT
 * triggers, which are used under the hood to trigger running at commit
 * time.
 *
 * Use of SET CONSTRAINTS .. IMMEDIATE is detected if it causes commit
 * triggers to run too soon, and causes an exception to be raised.
 *
 * Also, for simplicity we use SECURITY DEFINER functions here.
 *
 * Note that in this implementation users can cause CONNECT triggers to
 * re-run by dropping a temp table, but users should not be able to
 * cause BEGIN or COMMIT triggers to not fire, or to fire more than once
 * other than by catching exceptions and then trying more DMLs and/or
 * COMMITs.
 *
 * Use-cases:
 *
 *  - create TEMP schema before it's needed by regular triggers
 *
 *    This can be useful if you have DDL event triggers that slow things
 *    down, as CREATE TEMP TABLE IF NOT EXISTS will fire triggers on
 *    CREATE TABLE even when the table exists.
 *
 *  - cleanup internal, temporary state left by triggers from earlier
 *    transactions
 *
 *  - perform global consistency checks ("assertions", if you like)
 *
 *    Note that these can be made to scale by checking only the changes
 *    made by the current transaction.  Transition tables, temporal
 *    tables, audit tables -- these can all help for the purpose of
 *    checking only deltas as opposed to the entire database.
 *
 *  - update materializations of views when all the relevant deltas can
 *    be considered together
 *
 *  - call C functions that have external side-effects when you know the
 *    transaction will succeed (or previous ones that have succeeded but
 *    not had those functions called)
 *
 * For example, to create a commit trigger that invokes example_proc()
 * at the end of any write transaction, run the following in psql:
 *
 *      -- Load commit trigger functionality:
 *      \i commit_trigger.sql
 *
 *      -- CREATE COMMIT TRIGGER egt
 *      -- EXECUTE PROCEDURE commit_trigger.example_proc();
 *      INSERT INTO commit_trigger.triggers
 *                      (trig_name, trig_kind, proc_schema, proc_name)
 *      SELECT 'egt', 'COMMIT', 'commit_trigger', 'example_proc';
 *
 * Demo:
 *
 *  db=# \i commit_trigger.sql
 *  <noise>
 *  db=# INSERT INTO commit_trigger.triggers
 *  db-#                 (trig_name, trig_kind, proc_schema, proc_name)
 *  db-# SELECT 'egt', 'COMMIT', 'commit_trigger', 'example_proc';
 *  db=#
 *  db=# CREATE SCHEMA eg;
 *  CREATE SCHEMA
 *  db=# CREATE TABLE eg.x(a text primary key);
 *  CREATE TABLE
 *  db=# BEGIN;
 *  BEGIN
 *  db=#     INSERT INTO eg.x (a) VALUES('foo');
 *  INSERT 0 1
 *  db=#     INSERT INTO eg.x (a) VALUES('bar');
 *  INSERT 0 1
 *  db=# COMMIT;
 *  NOTICE:  example_proc() here!  Should be just one for this TX (txid 208036)
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at RAISE
 *  ...
 *  COMMIT
 *  db=# INSERT INTO eg.x (a) VALUES('foobar');
 *  NOTICE:  example_proc() here!  Should be just one for this TX (txid 208037)
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at RAISE
 *  ...
 *  db=# INSERT INTO eg.x (a) VALUES('baz');
 *  NOTICE:  example_proc() here!  Should be just one for this TX (txid 208038)
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at RAISE
 *  ...
 *  db=#
 *
 * Semantics:
 *
 *  - connect/begin/commit trigger procedures called exactly once per-
 *    transaction that had any writes (even if they changed nothing
 *    in the end), with one exception:
 *
 *     - exceptions thrown by triggers may be caught, and the triggering
 *       statement retried, in which case triggers will run again
 *
 *  - connect/begin trigger procedures called in order of trigger
 *    name (ascending) before any rows are inserted/updated/deleted by
 *    any DML statements on non-TEMP tables in the current transaction
 *
 *  - commit trigger procedures called in order of commit trigger name
 *    (ascending) at commit time, after the last statement sent by the
 *    client/user for the current transaction
 *
 *  - begin and commit trigger procedures may perform additional write
 *    operations, and if so that will NOT cause additional invocations
 *    of begin/commit trigger procedures
 *
 *  - commit trigger procedures may RAISE EXCEPTION, triggering a
 *    rollback of the transaction
 */

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS commit_trigger;
SET search_path = "commit_trigger";

DO LANGUAGE plpgsql $$
BEGIN
    CREATE TYPE commit_trigger.trigger_kind AS ENUM ();
EXCEPTION WHEN OTHERS THEN
    RAISE DEBUG 'ENUM type commit_trigger.trigger_kind already exists';
END; $$;

ALTER TYPE commit_trigger.trigger_kind ADD VALUE IF NOT EXISTS 'BEGIN';
ALTER TYPE commit_trigger.trigger_kind ADD VALUE IF NOT EXISTS 'COMMIT'
                                                               AFTER 'BEGIN';
ALTER TYPE commit_trigger.trigger_kind ADD VALUE IF NOT EXISTS 'CONNECT'
                                                               AFTER 'COMMIT';

CREATE TABLE IF NOT EXISTS commit_trigger.triggers (
    trig_name       TEXT PRIMARY KEY,
    trig_kind       trigger_kind NOT NULL
                    DEFAULT ('COMMIT'::trigger_kind),
    proc_schema     TEXT NOT NULL,
    proc_name       TEXT NOT NULL
);

/*
 * State needed to prevent more than one commit trigger call per-commit.
 *
 * This need not survive crashes, thus it's UNLOGGED.
 */
CREATE UNLOGGED TABLE IF NOT EXISTS commit_trigger.trigger_called (
    _txid           BIGINT PRIMARY KEY
                    CHECK(_txid = txid_current())
                    DEFAULT(txid_current()),
    _kind           trigger_kind NOT NULL
                    DEFAULT ('COMMIT'::trigger_kind)
);

/* Example commit trigger procesdure */
CREATE OR REPLACE FUNCTION commit_trigger.example_proc()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE
        'example_proc() here!  Should be just one for this TX (txid %)',
        txid_current();
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.example_proc2()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE
        'example_proc2() here!  Should be just one for this TX (txid %)',
        txid_current();
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.example_proc3()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE
        'example_proc3() here!  Should be just one for this TX (txid %)',
        txid_current();
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE VIEW commit_trigger.tables_needing_triggers AS
SELECT rn.nspname   AS tbl_schema,
       r.relname    AS tbl_name,
       r.oid        AS tbl_oid
FROM pg_catalog.pg_class r
JOIN pg_catalog.pg_namespace rn ON rn.oid = r.relnamespace
WHERE r.relkind = 'r' AND
      rn.nspname NOT LIKE 'pg\_%' AND
      rn.nspname NOT IN ('commit_trigger', 'information_schema');

CREATE OR REPLACE VIEW commit_trigger.synthetic_triggers AS
SELECT tnt.tbl_schema AS tbl_schema,
       tnt.tbl_name   AS tbl_name,
       'zzz_' || k.kind || '_' || tnt.tbl_schema || '_' || tnt.tbl_name AS tg_name,
       t.tgenabled AS tg_enabled,
       k.kind AS kind
FROM commit_trigger.tables_needing_triggers tnt
CROSS JOIN (SELECT 'BEGIN' UNION SELECT 'COMMIT') k(kind)
LEFT JOIN pg_trigger t ON t.tgrelid = tnt.tbl_oid
WHERE t.tgname IS NULL OR
      t.tgname = 'zzz_' || k.kind || '_' ||
                 tnt.tbl_schema || '_' || tnt.tbl_name;

CREATE OR REPLACE FUNCTION commit_trigger.invoke_triggers(kind trigger_kind)
RETURNS VOID AS $$
DECLARE
    t record;
    _msg text;
    _hint text;
    _detail text;
    _sqlstate text;
BEGIN
    FOR t IN (
        SELECT ct.proc_schema AS proc_schema, proc_name AS proc_name
        FROM commit_trigger.triggers ct
        JOIN pg_catalog.pg_proc p ON ct.proc_name = p.proname
        JOIN pg_catalog.pg_namespace pn ON p.pronamespace = pn.oid AND
                                           pn.nspname = ct.proc_schema
        WHERE trig_kind = kind
        ORDER BY trig_name ASC)
    LOOP
        EXECUTE format($q$
                SELECT %1$I.%2$I();
            $q$, t.proc_schema, t.proc_name);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS _msg := MESSAGE_TEXT,
                            _hint := PG_EXCEPTION_HINT,
                            _detail := PG_EXCEPTION_DETAIL,
                            _sqlstate := RETURNED_SQLSTATE;

    /* User may try again if they catch the exception we re-throw */
    DELETE FROM commit_trigger.trigger_called
    WHERE _txid = txid_current() AND _kind = kind;

    /* Re-throw exception */
    RAISE EXCEPTION USING
        ERRCODE = _sqlstate, MESSAGE = _msg, DETAIL = _detail, HINT = _hint;
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.trig_proc()
RETURNS TRIGGER AS $$
DECLARE
    kind trigger_kind;
BEGIN
    RAISE NOTICE 'ENTER % % % % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL,
        TG_TABLE_NAME;
    kind := (CASE TG_WHEN
             WHEN 'BEFORE' THEN 'BEGIN'
             ELSE 'COMMIT' END)::trigger_kind;

    /* Debounce so we do this just once per-{txid, kind} */
    IF NOT EXISTS(
            SELECT *
            FROM commit_trigger.trigger_called tc
            WHERE tc._txid = txid_current() AND tc._kind = kind) THEN

        /*
         * Check that SET CONSTRAINTS .. IMMEDIATE has not been done!
         *
         * This check must be done inside the debounce if so that commit
         * triggers may do DMLs that would cause our internal triggers
         * to run but never get here.  Otherwise commit triggers would
         * not be able to run DMLs!
         */
        IF kind = 'BEGIN' AND EXISTS (
                SELECT *
                FROM commit_trigger.trigger_called tc
                WHERE tc._txid = txid_current() AND tc._kind = 'COMMIT') THEN
            RAISE EXCEPTION 'NOT ALLOWED: SET CONSTRAINTS .. IMMEDIATE';
        END IF;

        /* Before anything else, debounce-call session CONNECT triggers */
        IF pg_my_temp_schema() = 0 OR NOT EXISTS (
                SELECT c.*
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname = 'session_trigger_called' AND
                      n.oid = pg_my_temp_schema()) THEN
            CREATE TEMP TABLE session_trigger_called ();
            RAISE NOTICE 'Calling CONNECT triggers (txid = %)', txid_current();
            PERFORM commit_trigger.invoke_triggers('CONNECT');
        END IF;

        INSERT INTO commit_trigger.trigger_called (_kind)
        SELECT kind
        ON CONFLICT DO NOTHING; /* Other half of debounce */

        RAISE NOTICE 'Calling % triggers (txid = %)', kind, txid_current();
        PERFORM commit_trigger.invoke_triggers(kind);

        /* Keep trigger_called from getting too large */
        DELETE FROM commit_trigger.trigger_called
        WHERE _txid < txid_current() - 1000;
    ELSE
        RAISE NOTICE 'DEBOUNCED';
    END IF;

    RAISE NOTICE 'EXIT % % % % %', TG_NAME, TG_WHEN, TG_OP, TG_LEVEL,
        TG_TABLE_NAME;
    IF TG_LEVEL = 'STATEMENT' THEN
        RETURN NULL;
    END IF;
    RETURN  CASE TG_OP
                WHEN 'INSERT' THEN NEW
                WHEN 'UPDATE' THEN NEW
                ELSE OLD
            END;
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.make_triggers()
RETURNS void AS $$
DECLARE
    t record;
    _kind text;
    _status text;
    _when text;
    _level text;
BEGIN
    FOR t IN (
        SELECT st.tg_name       AS tg_name,
               st.kind          AS kind,
               st.tbl_schema    AS tbl_schema,
               st.tbl_name      AS tbl_name
        FROM commit_trigger.synthetic_triggers st
        WHERE st.tg_enabled IS NULL)
    LOOP
        _kind   := CASE t.kind WHEN 'BEGIN' THEN '' ELSE 'CONSTRAINT' END;
        _when   := CASE t.kind WHEN 'BEGIN' THEN 'BEFORE' ELSE 'AFTER' END;
        _status := CASE t.kind WHEN 'BEGIN' THEN '' ELSE 'INITIALLY DEFERRED' END;
        _level  := CASE t.kind WHEN 'BEGIN' THEN 'STATEMENT' ELSE 'ROW' END;

        EXECUTE format($q$
                CREATE %4$s TRIGGER %1$I
                %5$s INSERT OR UPDATE OR DELETE
                ON %2$I.%3$I
                %6$s FOR EACH %7$s
                EXECUTE PROCEDURE commit_trigger.trig_proc();
            $q$, t.tg_name, t.tbl_schema, t.tbl_name,
                 _kind, _when, _status, _level);
    END LOOP;
    DELETE FROM commit_trigger.triggers ct
    WHERE NOT EXISTS (
        SELECT p.*
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace pn ON p.pronamespace = pn.oid
        WHERE pn.nspname = ct.proc_schema AND p.proname = ct.proc_name
    );
END $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.event_make_triggers()
RETURNS event_trigger AS $$
BEGIN
    PERFORM commit_trigger.make_triggers();
END $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path = commit_trigger;

/*
 * Make sure we define our internal triggers for all future new TABLEs,
 * and that we cleanup when commit trigger procedures are DROPped.
 */
DROP EVENT TRIGGER IF EXISTS commit_trigger_make_triggers;
CREATE EVENT TRIGGER commit_trigger_make_triggers ON ddl_command_end
WHEN tag IN ('CREATE TABLE', 'DROP FUNCTION')
EXECUTE PROCEDURE commit_trigger.event_make_triggers();

/* Create our internal triggers for all existing tables now */
SELECT commit_trigger.make_triggers();
