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

/*
 * This file demonstrates how to create a "COMMIT TRIGGER" for
 * PostgreSQL using CONSTRAINT TRIGGERs.
 *
 * There have been many threads on the PG mailing lists about commit
 * triggers, with much skepticism shown about the possible semantics of
 * such a thing.
 *
 * Below we demonstrate reasonable, useful, and desirable semantics, how
 * to obtain them with PG today.
 *
 * There are three shortcomings of this implementation:
 *
 * a) It is possible defeat this implementation by using
 *
 *      SET CONSTRAINTS ... IMMEDIATE;
 *
 *    or otherwise disabling the triggers created under the hood herein.
 *
 *    The ability to make these triggers run early can be *dangerous*,
 *    depending on the application.  It is especially dangerous given
 *    that no privilege is needed in order to do this, and there's no
 *    way for a CONSTRAINT TRIGGER to detect when it is called _last_,
 *    only when it is called _first_, in any transaction.
 *
 * b) This implementation serializes write transactions implicitly by
 *    having a single row encode commit trigger state.
 *
 *    (This is easily fixed though.)
 *
 * c) This implementation is inefficient because CONSTRAINT TRIGGERs
 *    have to be FOR EACH ROW triggers.  Thus a transaction that does
 *    1,000 inserts will cause 999 unnecessary trigger procedure calls
 *    under the hood.  Also, because CONSTRAINT TRIGGERs have to be FOR
 *    EACH ROW triggers, PG has to track OLD/NEW row values for all
 *    affected rows, even though commit triggers obviously don't need
 *    this.
 *
 * (Also, for simplicity we use SECURITY DEFINER functions here,
 * otherwise we'd have to have additional code to grant to
 * public the ability to call our functions.  We would need additional
 * code by which to ensure that users do not toggle internal state to
 * prevent commit trigger execution.)
 *
 * For example, to create a commit trigger that invokes
 * commit_trigger.example_proc() at the end of any _write_ transaction,
 * run the following in psql:
 *
 *      -- Load commit trigger functionality:
 *      \i commit_trigger.sql
 *
 *      -- CREATE COMMIT TRIGGER egt
 *      -- EXECUTE PROCEDURE commit_trigger.example_proc();
 *      INSERT INTO commit_trigger.triggers
 *                      (trig_name, proc_schema, proc_name)
 *      SELECT 'egt', 'commit_trigger', 'example_proc';
 *
 * Demo:
 *
 *  db=# \i commit_trigger.sql
 *  <noise>
 *  db=# INSERT INTO commit_trigger.triggers
 *  db-#                 (trig_name, proc_schema, proc_name)
 *  db-# SELECT 'egt', 'commit_trigger', 'example_proc';
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
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at
 *  RAISE
 *  COMMIT
 *  db=# INSERT INTO eg.x (a) VALUES('foobar');
 *  NOTICE:  example_proc() here!  Should be just one for this TX (txid 208037)
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at
 *  db=# INSERT INTO eg.x (a) VALUES('baz');
 *  NOTICE:  example_proc() here!  Should be just one for this TX (txid 208038)
 *  CONTEXT:  PL/pgSQL function example_proc() line 3 at
 *  db=#
 *
 * Semantics:
 *
 *  - commit trigger procedures called exactly once per-transaction that
 *    had any writes (even if they changed nothing in the end)
 *
 *    (*Unless* someone first runs SET CONSTRAINTS ALL IMMEDIATE!)
 *
 *  - commit trigger procedures called in order of commit trigger name
 *    (ascending)
 *
 *  - commit trigger procedures may perform additional write operations,
 *    and if so that will NOT cause additional invocations of commit
 *    trigger procedures
 *
 *  - commit trigger procedures may RAISE EXCEPTION, triggering a
 *    rollback of the transaction
 *
 * The above semantics are exactly what would be desired of a properly-
 * integrated COMMIT TRIGGER feature, except that it SHOULD NEVER be
 * possible to cause commit triggers to fire early by executing
 * SET CONSTRAINTS ALL IMMEDIATE.
 */

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS commit_trigger;

CREATE TABLE IF NOT EXISTS commit_trigger.triggers (
    trig_name       TEXT PRIMARY KEY,
    proc_schema     TEXT NOT NULL,
    proc_name       TEXT NOT NULL
);

/* State needed to prevent more than one commit trigger call per-commit */
CREATE TABLE IF NOT EXISTS commit_trigger.commit_trigger_called (
    _id BIGINT PRIMARY KEY CHECK(_id = 0) DEFAULT(0),
    _txid BIGINT CHECK(_txid = txid_current()) DEFAULT(txid_current()))
;

INSERT INTO commit_trigger.commit_trigger_called
SELECT
ON CONFLICT DO NOTHING;

/* Example commit trigger procesdure */
CREATE OR REPLACE FUNCTION commit_trigger.example_proc()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE
        'example_proc() here!  Should be just one for this TX (txid %)',
        txid_current();
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE VIEW commit_trigger.synthetic_triggers AS
SELECT rn.nspname   AS tbl_schema,
       r.relname    AS tbl_name,
       coalesce(t.tgname,
                'zzz_commit_trigger_' || rn.nspname || '_' || r.relname) AS tg_name,
       t.tgenabled  AS tg_enabled
FROM pg_catalog.pg_class r
JOIN pg_catalog.pg_namespace rn ON rn.oid = r.relnamespace
LEFT JOIN pg_trigger t ON t.tgrelid = r.oid
WHERE r.relkind = 'r' AND
      (t.tgname IS NULL OR t.tgname LIKE 'zzz\_commit\_trigger\_%') AND
      rn.nspname NOT IN ('commit_trigger', 'pg_catalog');

CREATE OR REPLACE FUNCTION commit_trigger.invoke_commit_triggers()
RETURNS VOID AS $$
DECLARE
    t record;
BEGIN
    FOR t IN (
        SELECT ct.proc_schema AS proc_schema, proc_name AS proc_name
        FROM commit_trigger.triggers ct
        JOIN pg_catalog.pg_proc p ON ct.proc_name = p.proname
        JOIN pg_catalog.pg_namespace pn ON p.pronamespace = pn.oid AND
                                           pn.nspname = ct.proc_schema
        ORDER BY trig_name ASC)
    LOOP
        EXECUTE format($q$
                SELECT %1$I.%2$I();
            $q$, t.proc_schema, t.proc_name);
    END LOOP;
END $$ LANGUAGE PLPGSQL SECURITY INVOKER SET search_path = commit_trigger;

CREATE OR REPLACE FUNCTION commit_trigger.trig_proc()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS(
            SELECT *
            FROM commit_trigger.commit_trigger_called
            WHERE _txid = txid_current()) THEN
        /*RAISE NOTICE 'Calling commit triggers (txid = %)', txid_current();*/
        PERFORM commit_trigger.invoke_commit_triggers();
        UPDATE commit_trigger.commit_trigger_called
        SET _txid = txid_current();
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
BEGIN
    FOR t IN (
        SELECT st.tg_name       AS tg_name,
               st.tbl_schema    AS tbl_schema,
               st.tbl_name      AS tbl_name
        FROM commit_trigger.synthetic_triggers st
        WHERE st.tg_enabled IS NULL)
    LOOP
        EXECUTE format($q$
                CREATE CONSTRAINT TRIGGER %1$I
                AFTER INSERT OR UPDATE OR DELETE
                ON %2$I.%3$I
                INITIALLY DEFERRED FOR EACH ROW
                EXECUTE PROCEDURE commit_trigger.trig_proc();
            $q$, t.tg_name, t.tbl_schema, t.tbl_name);
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
