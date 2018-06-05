/*
 * Copyright (c) 2016-2018 Two Sigma Open Source, LLC.
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
 * NAME
 *  pseudo_mat_views
 *
 * This file creates functions (in a schema named pseudo_mat_views) that
 * implement PlPgSQL-coded "MATERIALIZED" VIEWs (MV) supporting CONCURRENT
 * refreshes and, critically, saving of deltas, as well as triggers (on the
 * view or which modify the view), and indexes.
 *
 *
 * SYNOPSIS
 *
 *  -- To create a pseudo-materialized VIEW:
 *  CREATE VIEW ...;
 *  SELECT pseudo_mat_views.create_view(schema_name,
 *                                      pseudo_view_name,
 *                                      orig_schema_name,
 *                                      orig_view_name);
 *
 *  -- To refresh a pseudo-materialized VIEW:
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name);
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name, id, tstamp);
 *
 *  -- Or refresh with triggers disabled (faster if there are a lot of pending
 *  -- changes):
 *  SET session_replication_role = replica;
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name);
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name, id, tstamp);
 *  SET session_replication_role = origin;
 *
 *  -- To drop a pseudo-materialized VIEW:
 *  SELECT pseudo_mat_views.drop_view(schema_name, view_name, cascade_bool);
 *
 *  -- Check if a materialized view needs to be refreshed:
 *  SELECT pseudo_mat_views.needs_refresh_p(schema_name, view_name);
 *  SELECT pseudo_mat_views.needs_refresh_p(schema_name, view_name, sla_interval);
 *
 *  -- To signal that a materialized view needs to be refreshed (this NOTIFYs
 *  -- subscribers):
 *  SELECT pseudo_mat_views.set_needs_refresh(schema_name, view_name);
 *  SELECT pseudo_mat_views.set_needs_refresh(schema_name, view_name, 'urgent');
 *  SELECT pseudo_mat_views.set_needs_refresh(schema_name, view_name, 'some other msg');
 *
 *  -- To view a materialized view's history:
 *  SELECT * FROM <schema_name>.<view_name>_updates ...;
 *
 *  -- To refer to the VIEW without materialization:
 *  SELECT * FROM <orig_schema_name>.<orig_view_name> ...;
 *
 *  -- To set a VIEW's update SLA to 5 minutes:
 *  UPDATE pseudo_mat_views.state
 *  SET sla = interval '5 minutes'
 *  WHERE schema_name = '<schema_name>' AND view_name = '<view_name>';
 *
 *  -- To cause periodic refreshes (by an external daemon)
 *  UPDATE pseudo_mat_views.state
 *  SET periodic_refresh_needed = true
 *  WHERE schema_name = '<schema_name>' AND view_name = '<view_name>';
 *
 *  -- To set a VIEW's notification channel:
 *  UPDATE pseudo_mat_views.state
 *  SET needs_refresh_notify_channel = '<channel_name>'
 *  WHERE schema_name = '<schema_name>' AND view_name = '<view_name>';
 *
 * DESCRIPTION
 *
 * MATERIALIZED VIEWs are fantastic, but there's no way to record the changes
 * seen during a REFRESH.  We want to reify those changes.  I.e., we want
 * something like:
 *
 *      CREATE MATERIALIZED VIEW name
 *      WITH HISTORY = name_hist (id bigint, tstamp timestamp) ...;
 *
 *      REFRESH MATERIALIZED VIEW name CONCURRENTLY
 *      WITH (id, tstamp) AS (SELECT 123, current_timestamp);
 *
 * with deltas inserted into [name_hist] as (id, tstamp, awld, noo), where
 * [awld] and [noo] are columns storing values of record types containing rows
 * from [name] that are deleted and inserted, respectively.
 *
 * Further more, we want to be able to:
 *
 *  - alter materialized views to add column and/or table constraints
 *
 *    NOTE: Source view output MUST be consistent, and any UNIQUE
 *          constraints must be deferred.
 *
 *  - create indexes on materialized views
 *
 *  - create triggers on materialized views
 *
 *  - update materialized views from triggers on other tables, as if
 *    materialized views were TABLEs and record those changes in the
 *    materialized views' history tables as if the updates had happened via a
 *    refresh
 *
 *    (Someday we might be able to generate some such triggers from a VIEW's
 *     definition query for some types of queries, or for some table sources of
 *     more complex queries where we can't generate such triggers for all table
 *     sources.
 *
 * Since we're NOT going to patch Postgres for this, we PlPgSQL-code the
 * equivalent here, with a function interface.  If ever we patch Postgres we'd
 * probably import this implementation, wrap it in C-coded functions, and add
 * syntactic sugar.
 *
 * Our implementation is based entirely on PlPgSQL-coding what Postgres
 * actually does in C and SQL to implement MATERIALIZED VIEWs.
 *
 * This file defines several functions for creating, refreshing, and dropping
 * "pseudo"-materialized views, and other related operations.  See above.
 *
 * These "materialized views" look and feel almost exactly the same as a
 * Postgres MATERIALIZED VIEW that is REFRESHed CONCURRENTLY, with just the
 * additional capabilities mentioned above and with a function-based interface
 * rather than extended syntax.
 *
 * The generation of updates, copied from Postgres' internals, works as
 * follows:
 *
 *  - acquire an EXCLUSIVE LOCK on a helper TABLE for the materialized view
 *
 *  - populate a helper TABLE like the materialized one to populate with
 *    SELECT * FROM :the_view;
 *
 *  - execute an INSERT INTO diff_table whose SELECT body does a FULL OUTER
 *    JOIN of the previously-polulated and newly-generated output of the VIEW
 *    where one or the other side is NULL
 *
 *    If the MV has been ALTERed to have a PRIMARY KEY constraint, then the PK
 *    columns will be used for this JOIN, otherwise a NATURAL FULL OUTER JOIN
 *    will be used instead.
 *
 *  - DELETE FROM the materialization table the recorded deletions
 *
 *  - INSERT INTO the materialization table the recorded additions
 *
 *  - If the MV has been ALTERed to have a PRIMARY KEY constraint, then UPDATE
 *    the materialization table to set non-PRIMARY KEY columns of rows
 *    modified
 *
 *  - record the changes by INSERTing into the update history table from the
 *    refresh delta table
 *
 *  - (at COMMIT time) impliedly release the EXCLUSIVE LOCK
 *
 * We could do non-concurrent (atomic) updates by renaming the helper table
 * into place, just as non-concurrent materialization does in PG.  We'd still
 * do the FULL OUTER JOIN in that case in order to record the changes.
 *
 * We will skip most of the sanity checks in the Postgres implementation of
 * MATERIALIZED VIEWs:
 *
 *  - we will check if a view's materialization table has contents -- if not
 *    then instead of failing we'll update it but won't record the updates
 *
 *  - we will NOT check that the view's new contents has no duplicate rows;
 *    instead we'll simply not duplicate inserts in the update phase (or maybe
 *    rely on the view not generating duplicates, or maybe create a unique
 *    index across all columns).
 *
 * We use the Postgres format() function to format dynamic SQL statements.  We
 * use the ability to refer to an entire row as a single value, and the ability
 * to have record types, to reduce the need for having to refer to any columns
 * from the VIEW being materialized.
 *
 * NOTES
 *
 *  - VIEWs with output rows that contain NULLs MUST be ALTERed to have a
 *    PRIMARY KEY
 *
 *    This is true even for native PG MVs, so this code is not lesser than
 *    native PG MVs in this sense, but native PG MVs cannot be ALTERed to have
 *    PKs.
 *
 * IMPLEMENTATION NOTES
 *
 *  - We do count on NEW.* and table_source.* to produce stable column
 *    orderings in some cases.  Normally this is a no-no, but the way we use it
 *    here should work stably.
 *
 *    In particular we rely on this to work correctly:
 *
 *     INSERT INTO table SELECT composite_value_of_table_type.* ...;
 *
 *    to match up the columns in the SELECT expression with the columns in the
 *    TABLE where the rows are to be inserted (which in Postgres is true as
 *    long as the two composite types are isomorphic including column order),
 *    and:
 *
 *     CAST(row(NEW.*) AS composite_type)
 *
 *    to do the right thing provided NEW is isomorphic to composite_type
 *    including column order.
 *
 *    The latter is needed because there's no support for casting between
 *    isomorphic composite types, and this can be made safer using JSON
 *    functions as follows:
 *
 *     json_populate_record(null::target_composite_type,
 *                          row_to_json(row_of_other_composite_type))
 *
 *    (This is safer because column ordering is irrelevant because
 *    row_to_json() produces a JSON object with column names as keys.)
 *
 *    This arises here due to creating two TABLEs for a materialized VIEW --
 *    one to hold the results, and one for the refresh process -- where both
 *    have the same columns and column types as the original VIEW, and where we
 *    need to JOIN the two tables to generate updates to record and to update
 *    the materialization table.
 *
 * - We use IS [NOT] DISTINCT FROM NULL instead of IS [NOT] NULL because the
 *   latter outputs NULL when the LHS is a non-NULL row/record containing
 *   NULLs.
 */

\set ON_ERROR_STOP on
SET client_min_messages TO WARNING;
--RESET SESSION AUTHORIZATION; /* run as superuser pls */

/* It's safe to source this file repeatedly */
CREATE SCHEMA IF NOT EXISTS pseudo_mat_views;
SET search_path = "pseudo_mat_views";
CREATE SEQUENCE IF NOT EXISTS tx_id;

DROP EVENT TRIGGER IF EXISTS pseudo_mat_views_drop_state_trig CASCADE;

CREATE OR REPLACE VIEW pseudo_mat_views.triggers AS
SELECT t.tgname     AS tg_name,
       CASE
        WHEN t.tgenabled = 'A' THEN TRUE
        WHEN t.tgenabled = 'O' AND current_setting('session_replication_role') = 'origin'
            THEN TRUE
        WHEN t.tgenabled = 'R' AND current_setting('session_replication_role') = 'replica'
            THEN TRUE
        ELSE FALSE
        END         AS tg_enabled_now,
       t.tgenabled  AS tg_enabled,
       rn.nspname   AS tbl_schema,
       r.relname    AS tbl_name,
       pn.nspname   AS proc_schema,
       p.proname    AS proc_name
FROM pg_trigger t
JOIN pg_class r on t.tgrelid = r.oid
JOIN pg_namespace rn on rn.oid = r.relnamespace
JOIN pg_proc p on t.tgfoid = p.oid
JOIN pg_namespace pn on p.pronamespace = pn.oid
WHERE pn.nspname <> 'pg_catalog';

CREATE SEQUENCE IF NOT EXISTS pseudo_mat_views.serial MINVALUE 0 START WITH 0;

CREATE TABLE IF NOT EXISTS pseudo_mat_views.state (
    schema_name text,
    view_name text,
    source_schema_name text,
    source_view_name text,
    /* Views with lower refresh_order are refreshed first */
    refresh_order integer UNIQUE DEFAULT (nextval('pseudo_mat_views.serial')),
    periodic_refresh_needed boolean DEFAULT false,
    sla interval,
    debounce interval,
    refresh_enabled boolean DEFAULT true,
    needs_refresh boolean,
    needs_first_refresh boolean,
    needs_refresh_notify_channel text,
    delta_warn_limit bigint DEFAULT (500),
    last_update timestamp without time zone,
    last_update_time interval,
    txid bigint
 );

CREATE TABLE IF NOT EXISTS pseudo_mat_views.refresh_history (
    _schema         TEXT,
    _view           TEXT,
    _txid           BIGINT NOT NULL DEFAULT (txid_current()),
    _txtime         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (current_timestamp),
    _tstamp         TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (clock_timestamp()),
    _op             TEXT NOT NULL DEFAULT ('refresh'),
    _who            TEXT NOT NULL DEFAULT (current_user),
    _start          TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (clock_timestamp()),
    _end            TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (_schema, _view, _op, _txid)
);

CREATE INDEX IF NOT EXISTS idx_refresh_history1 ON pseudo_mat_views.refresh_history
    (_txid);
CREATE INDEX IF NOT EXISTS idx_refresh_history2 ON pseudo_mat_views.refresh_history
    (_start);

CREATE OR REPLACE FUNCTION pseudo_mat_views.update_txid()
RETURNS trigger AS $$
BEGIN
    UPDATE pseudo_mat_views.state
    SET txid = txid_current()
    WHERE schema_name = TG_TABLE_SCHEMA AND view_name = TG_TABLE_NAME;
    RETURN NULL;
END; $$ LANGUAGE PLPGSQL VOLATILE SET search_path FROM CURRENT;

/* Supporting function */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.drop_triggers(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
DECLARE
    _prefix TEXT := '_' || given_schema_name || '__' || given_view_name;
BEGIN
    EXECUTE format($q$
        DROP TRIGGER IF EXISTS %3$I ON %1$I.%2$I CASCADE;
        DROP TRIGGER IF EXISTS %4$I ON %1$I.%2$I CASCADE;
        DROP TRIGGER IF EXISTS %5$I ON %1$I.%2$I CASCADE;
    $q$, given_schema_name, given_view_name,
    _prefix || '_ins_trigger',
    _prefix || '_upd_trigger',
    _prefix || '_del_trigger');
END
$$ LANGUAGE plpgsql VOLATILE SET search_path FROM CURRENT;

/* Supporting function */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.create_triggers(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
DECLARE
    _prefix TEXT := '_' || given_schema_name || '__' || given_view_name;
BEGIN
    PERFORM pseudo_mat_views.drop_triggers(given_schema_name, given_view_name);

    /*
     * XXX Make a single trigger procedure (function) and a single trigger
     * per-materialization, not one for each TG_OP.
     */

    /*
     * Using cast to row() we might be able to make the _updates tables
     * generic, and thus the trigger functions for them also generic.  Alas,
     * record is a pseudo-type and PG won't allow table columns to be of
     * pseudo-types.  We could encode a record as JSON, but then we'd have to
     * worry about JSON equality, and there's no JSONB equality operator.  Alas
     * for now we cannot make these trigger functions and history tables
     * generic :(
     *
     * The trigger functions here need to support the possibility of insert,
     * update, and/or delete of a row in the same transaction.  An insert and
     * delete of the same row should result in no history, even if an update
     * occurs in between.  An insert then update (but no delete) of the same
     * row should result in just one item in the history.  An update and a
     * delete should also result in just one item in the history.
     *
     * INSERTs are easy to handle.  UPDATEs and DELETEs have to either update a
     * history entry for the same current transaction, or insert one.
     */
    EXECUTE format($q$
            CREATE OR REPLACE FUNCTION %1$I.%2$I()
            RETURNS trigger AS $body$
            BEGIN
                INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                SELECT txid_current(),
                       CAST(clock_timestamp() AS timestamp without time zone),
                       NULL, CAST(row(NEW.*) AS %1$I.%4$I);
                RETURN NEW;
            END; $body$ LANGUAGE PLPGSQL VOLATILE;
        $q$, given_schema_name, _prefix || '_ins_func',
        given_view_name || '_updates', given_view_name || '_new');

    EXECUTE format($q$
            CREATE OR REPLACE FUNCTION %1$I.%2$I()
            RETURNS trigger AS $body$
            BEGIN
                UPDATE %1$I.%3$I
                SET noo = CAST(row(NEW.*) AS %1$I.%4$I)
                WHERE id = txid_current() AND
                    noo = CAST(row(OLD.*) AS %1$I.%4$I);
                INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                SELECT txid_current(),
                       CAST(clock_timestamp() AS timestamp without time zone),
                       OLD, CAST(row(NEW.*) AS %1$I.%4$I)
                WHERE -- Only if the OLD and NEW rows differ
                      CAST(row(OLD.*) AS %1$I.%4$I) IS DISTINCT FROM CAST(row(NEW.*) AS %1$I.%4$I) AND
                      -- Only if there's no entry already with the same noo row
                      NOT EXISTS (
                        SELECT * FROM %1$I.%3$I
                        WHERE id = txid_current() AND
                            noo IS DISTINCT FROM NULL AND
                            noo IS NOT DISTINCT FROM CAST(row(NEW.*) AS %1$I.%4$I));
                RETURN NEW;
            END; $body$ language plpgsql VOLATILE;
        $q$, given_schema_name, _prefix || '_upd_func',
        given_view_name || '_updates', given_view_name || '_new');

    EXECUTE format($q$
            CREATE OR REPLACE FUNCTION %1$I.%2$I()
            RETURNS trigger AS $body$
            BEGIN
                -- Insert deletion record if there is no insert/update
                -- record from same TX to modify
                INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                SELECT txid_current(),
                       CAST(clock_timestamp() AS timestamp without time zone),
                       OLD, NULL
                WHERE NOT EXISTS (
                    SELECT * FROM %1$I.%3$I
                    WHERE id = txid_current() AND
                        noo = CAST(row(OLD.*) AS %1$I.%4$I));
                -- Delete matching insert record from same TX
                DELETE FROM %1$I.%3$I
                WHERE id = txid_current() AND
                    awld IS NOT DISTINCT FROM NULL AND
                    noo = CAST(row(OLD.*) AS %1$I.%4$I);
                -- Update update matching record from same TX to be
                -- deletion record
                UPDATE %1$I.%3$I
                SET noo = NULL
                WHERE id = txid_current() AND
                    noo = CAST(row(OLD.*) AS %1$I.%4$I);
                DELETE FROM %1$I.%3$I
                WHERE id = txid_current() AND
                    awld IS NOT DISTINCT FROM NULL AND noo IS NOT
                    DISTINCT FROM NULL;
                RETURN OLD;
            END; $body$ language plpgsql VOLATILE;
        $q$, given_schema_name, _prefix || '_del_func',
        given_view_name || '_updates', given_view_name || '_new');

    IF NOT EXISTS (SELECT *
               FROM pseudo_mat_views.triggers
               WHERE tg_name = _prefix || '_upd_txid') THEN
        EXECUTE format($q$
                CREATE TRIGGER %3$I
                AFTER INSERT OR UPDATE OR DELETE ON %1$I.%2$I
                FOR EACH STATEMENT
                EXECUTE PROCEDURE pseudo_mat_views.update_txid();
            $q$, given_schema_name, given_view_name,
            _prefix || '_upd_txid');
    END IF;

    IF NOT EXISTS (SELECT *
               FROM pseudo_mat_views.triggers
               WHERE tg_name = _prefix || '_ins_trigger') THEN
        EXECUTE format($q$
                CREATE TRIGGER %3$I AFTER INSERT ON %1$I.%2$I
                FOR EACH ROW
                EXECUTE PROCEDURE %1$I.%4$I();
            $q$, given_schema_name, given_view_name,
            _prefix || '_ins_trigger',
            _prefix || '_ins_func');
    END IF;

    IF NOT EXISTS (SELECT *
               FROM pseudo_mat_views.triggers
               WHERE tg_name = _prefix || '_upd_trigger') THEN
        EXECUTE format($q$
                CREATE TRIGGER %3$I AFTER UPDATE ON %1$I.%2$I
                FOR EACH ROW
                EXECUTE PROCEDURE %1$I.%4$I();
            $q$, given_schema_name, given_view_name,
            _prefix || '_upd_trigger',
            _prefix || '_upd_func');
    END IF;

    IF NOT EXISTS (SELECT *
               FROM pseudo_mat_views.triggers
               WHERE tg_name = _prefix || '_del_trigger') THEN
        EXECUTE format($q$
                CREATE TRIGGER %3$I AFTER DELETE ON %1$I.%2$I
                FOR EACH ROW
                EXECUTE PROCEDURE %1$I.%4$I();
            $q$, given_schema_name, given_view_name,
            _prefix || '_del_trigger', _prefix || '_del_func');
    END IF;
END
$$ LANGUAGE plpgsql VOLATILE SET search_path FROM CURRENT;


/**
 * Create a materialization of a VIEW.
 *
 * The materialized VIEW view supports concurrent refreshes, independent DMLs,
 * and reification of update history.
 *
 * To use this first create a VIEW, then call this function with the VIEW's
 * schema and name.
 *
 * The VIEW's body should not generate duplicate rows, and should not have
 * NULLs in any columns of any rows.
 *
 * Because this creates a normal TABLE for materializing the VIEW, you can
 * ALTER the TABLE to add constraints, you can create INDEXes, you can create
 * TRIGGERs on the TABLE, and you can even alter the TABLE's contents as usual
 * (e.g., from triggers on other TABLEs).  However, you should not alter the
 * TABLE's contents in ways that are inconsistent with the source VIEW -- you
 * may, but such changes will eventually be undone.
 *
 * An ancilliary TABLE, named the same as the materilization with a suffix
 * "_updates", will be created and maintained with history.
 *
 * You can refresh the materialization (concurrently) at any time by calling
 * pseudo_mat_views.refresh_view() or pseudo_mat_views.refresh_views().
 *
 * Example:
 *
 *  CREATE VIEW foo.bar_source AS SELECT ...;
 *
 *  -- Create a materialized view named foo.bar:
 *  SELECT pseudo_mat_views.create_view('foo','bar','foo','bar_source')
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.create_view(given_schema_name text,
                                 given_view_name text,
                                 given_source_schema_name text,
                                 given_source_view_name text,
                                 _if_not_exists boolean default(false))
RETURNS void AS $$
DECLARE
    /*
     * If the VIEW we're materializing is TEMP, then all elements we create
     * must be too.  Otherwise, some tables we create can be UNLOGGED.
     */
    kw text = CASE WHEN given_schema_name LIKE 'pg_temp%' THEN 'TEMP' ELSE 'UNLOGGED' END;
    _prefix TEXT := '_' || given_schema_name || '__' || given_view_name;
BEGIN
    IF _if_not_exists AND EXISTS (
        SELECT *
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = given_schema_name AND c.relname = given_view_name AND
              c.relkind = 'r')
    THEN
        RETURN;
    END IF;
    DELETE FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name;

    /* Note that format()'s %n$ notation is one-based... */
    /*
     * Create a TABLE with the VIEW's original name and store the VIEW's
     * content in it immediately.
     */
    EXECUTE format($q$
            CREATE TABLE %1$I.%2$I AS SELECT * FROM %3$I.%4$I LIMIT 0;
        $q$, given_schema_name, given_view_name,
        given_source_schema_name, given_source_view_name);
    /*
     * Create a second TABLE for that VIEW (for generating updates via a FULL
     * OUTER JOIN).
     *
     * Can't make the _new table temp for reasons, though I forget them now.
     * But it's just as well.  We need a table to lock and we'd rather not lock
     * the table users see.
     *
     * We could make this a CTE though.
     */
    EXECUTE format($q$
            CREATE %1$s TABLE %2$I.%4$I (LIKE %2$I.%3$I INCLUDING ALL);
        $q$, kw, given_schema_name, given_view_name, given_view_name || '_new');
    /*
     * Create a table in which to store deltas for a refresh, and another in
     * which to store historical updates from each refresh and/or triggers.
     */
    EXECUTE format($q$
            CREATE TABLE %1$I.%4$I (
                id bigint,
                tstamp timestamp without time zone,
                awld %1$I.%2$I,
                noo %1$I.%3$I);
        $q$,
        given_schema_name,
        given_view_name,
        given_view_name || '_new',
        given_view_name || '_updates');
    EXECUTE format($q$
            CREATE %1$s TABLE %2$I.%5$I (awld %2$I.%3$I, noo %2$I.%4$I);
        $q$, kw, given_schema_name, given_view_name,
        given_view_name || '_new', given_view_name || '_deltas');
    /* Index the _deltas table */
    EXECUTE format($q$
            CREATE INDEX %3$I ON %1$I.%2$I (awld, noo);
        $q$, given_schema_name, given_view_name || '_deltas',
        _prefix || '_deltas_idx');
    EXECUTE format($q$
            CREATE INDEX %3$I ON %1$I.%2$I (noo, awld);
        $q$, given_schema_name, given_view_name || '_deltas',
        _prefix || '_deltas_idx_awld');
    /* Index the _updates table */
    EXECUTE format($q$
            CREATE INDEX %3$I ON %1$I.%2$I (id, awld, noo);
        $q$, given_schema_name, given_view_name || '_updates',
        _prefix || '_updates_idx');
    EXECUTE format($q$
            CREATE INDEX %3$I ON %1$I.%2$I (awld);
        $q$, given_schema_name, given_view_name || '_updates',
        _prefix || '_updates_idx_awld');
    EXECUTE format($q$
            CREATE INDEX %3$I ON %1$I.%2$I (noo);
        $q$, given_schema_name, given_view_name || '_updates',
        _prefix || '_updates_idx_noo');

    /* Register this pseudo-materialized view */
    INSERT INTO pseudo_mat_views.state
        (schema_name, view_name, source_schema_name, source_view_name,
         needs_refresh, last_update, txid, needs_first_refresh)
    SELECT given_schema_name, given_view_name,
        given_source_schema_name, given_source_view_name, true,
        CAST(current_timestamp AS timestamp without time zone),
        txid_current(), true
    /* If it wasn't already registered */
    WHERE NOT EXISTS (SELECT * FROM pseudo_mat_views.state
                      WHERE schema_name = given_schema_name AND
                            view_name = given_view_name);

    /* Create triggers */
    PERFORM pseudo_mat_views.create_triggers(given_schema_name, given_view_name);
END
$$ LANGUAGE plpgsql VOLATILE SET search_path FROM CURRENT;

/**
 * Creates a materialization of a view of the same name as the given view name.
 *
 * The materialized VIEW view supports concurrent refreshes, independent DMLs,
 * and reification of update history.
 *
 * To use this first create a VIEW, then call this function with the VIEW's
 * schema and name.
 *
 * The VIEW's body should not generate duplicate rows, and should not have
 * NULLs in any columns of any rows.
 *
 * Because this creates a normal TABLE for materializing the VIEW, you can
 * ALTER the TABLE to add constraints, you can create INDEXes, you can create
 * TRIGGERs on the TABLE, and you can even alter the TABLE's contents as usual
 * (e.g., from triggers on other TABLEs).  However, you should not alter the
 * TABLE's contents in ways that are inconsistent with the source VIEW -- you
 * may, but such changes will eventually be undone.
 *
 * An ancilliary TABLE, named the same as the materilization with a suffix
 * "_updates", will be created and maintained with history.
 *
 * You can refresh the materialization (concurrently) at any time by calling
 * pseudo_mat_views.refresh_view() or pseudo_mat_views.refresh_views().
 *
 * The original view will be renamed away and a materialization table of the
 * same original name will be created.
 *
 * After calling this function you may create indices on the VIEW's original
 * name.
 *
 * Example:
 *
 *  -- Create a materialized view named foo.bar:
 *  CREATE VIEW foo.bar AS SELECT ...;
 *  SELECT pseudo_mat_views.create_view('foo','bar');
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.create_view(given_schema_name text,
                                 given_view_name text,
                                 _if_not_exists boolean default(false))
RETURNS void AS $$
BEGIN
    EXECUTE format($q$
        ALTER VIEW IF EXISTS %1$I.%2$I RENAME TO %3$I;
    $q$, given_schema_name, given_view_name, given_view_name || '_source');
    PERFORM pseudo_mat_views.create_view(given_schema_name, given_view_name,
        given_schema_name, given_view_name || '_source',_if_not_exists);
END $$ LANGUAGE plpgsql VOLATILE SET search_path FROM CURRENT;

/**
 * Drop a materialized view created by pseudo_mat_views.create_view().
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.drop_view(given_schema_name text, given_view_name text,
        do_cascade boolean default (false))
RETURNS void AS $$
DECLARE
    c text = CASE do_cascade WHEN true THEN 'CASCADE' ELSE '' END;
BEGIN
    PERFORM pseudo_mat_views.drop_triggers(given_schema_name, given_view_name);
    EXECUTE format($q$
            DROP TABLE IF EXISTS %1$I.%2$I %3s;
        $q$, given_schema_name, given_view_name || '_updates', c);
    EXECUTE format($q$
            DROP TABLE IF EXISTS %1$I.%2$I %3s;
        $q$, given_schema_name, given_view_name || '_deltas', c);
    EXECUTE format($q$
            DROP TABLE IF EXISTS %1$I.%2$I %3s;
        $q$, given_schema_name, given_view_name || '_new', c);
    EXECUTE format($q$
            DROP TABLE IF EXISTS %1$I.%2$I %3s;
        $q$, given_schema_name, given_view_name, c);

    /* "Unregister" the view */
    DELETE FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
END
$$ LANGUAGE plpgsql VOLATILE SET search_path FROM CURRENT;

/* Private: unused */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._have_lock(_schema text, _table text)
RETURNS boolean AS $$
    SELECT EXISTS (
        SELECT *
        FROM pg_catalog.pg_locks l
        JOIN pg_catalog.pg_class c ON l.relation = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE l.locktype = 'relation' AND l.granted AND
              l.pid = pg_backend_pid() AND
              n.nspname = _schema AND
              c.relname = _table);
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/* Private: returns true if table has a PRIMARY KEY constraint */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._has_pk(_schema text, _view text)
RETURNS boolean AS $$
SELECT EXISTS (
    SELECT *
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_constraint cc ON cc.conrelid = c.oid
    WHERE n.nspname = _schema AND c.relname = _view AND cc.contype = 'p'
);
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/* Private: returns string list of columns in constraint */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._constraint_cols(_schema text,
                                      _view text,
                                      _conname text DEFAULT (NULL))
RETURNS text AS $$
SELECT string_agg(format('%I',a.attname),',')
FROM (SELECT c.oid AS tbloid, unnest(cc.conkey) AS aid
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      JOIN pg_catalog.pg_constraint cc ON cc.conrelid = c.oid
      WHERE n.nspname = _schema AND c.relname = _view AND
            ((_conname IS NOT NULL AND cc.conname = _conname) OR
             (_conname IS NULL     AND cc.contype = 'p'))) q
JOIN pg_catalog.pg_attribute a ON q.tbloid = a.attrelid AND q.aid = a.attnum;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/* Private: returns full outer join clause for refresh delta computation */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._using(_schema text,
                            _view text,
                            _conname text DEFAULT (NULL))
RETURNS text AS $$
SELECT CASE pseudo_mat_views._has_pk(_schema, _view)
       WHEN TRUE THEN
            'USING (' ||
                pseudo_mat_views._constraint_cols(_schema, _view, _conname) ||
                ')'
       ELSE ''
       END;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/*
 * Private: returns WHERE/JOIN..ON equality clauses
 *
 * This is useful when a materialized view gets a PRIMARY KEY.  Then we can use
 * equi-joins on the PK column list instead of equi-joins on record values.
 * It's useful because the PG optimizer doesn't know how to do this same
 * decomposition automatically, so equi-joins on record values can be slow.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._gen_eq(
        _schema        text, -- schema of table whose PK column list to use
        _table         text, -- table whose PK column list to use
        _left_src      text, -- name of left  table source
        _right_src     text, -- name of right table source
        _left_rec_col  text DEFAULT NULL, -- name of record-type column on left
        _right_rec_col text DEFAULT NULL -- name of record-type column on right
    )
RETURNS text AS $$
DECLARE
    _left_ident text;
    _right_ident text;
BEGIN
    IF _left_rec_col IS NULL THEN
        _left_ident := format('%I', _left_src);
    ELSE
        _left_ident := format('(%I.%I)', _left_src, _left_rec_col);
    END IF;
    IF _right_rec_col IS NULL THEN
        _right_ident := format('%I', _right_src);
    ELSE
        _right_ident := format('(%I.%I)', _right_src, _right_rec_col);
    END IF;
    RETURN string_agg(format('%1$s.%3$I = %2$s.%3$I', _left_ident, _right_ident, a.attname), ' AND ')
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_constraint cc ON cc.conrelid = c.oid AND cc.contype = 'p',
    LATERAL (SELECT unnest(cc.conkey) AS k) k,
    LATERAL (SELECT a.attname AS attname
             FROM pg_catalog.pg_attribute a
             WHERE c.oid = a.attrelid AND attnum = k.k) a
    WHERE n.nspname = _schema AND c.relname = _table;
END; $$ LANGUAGE PLPGSQL VOLATILE SET search_path FROM CURRENT;

/* Private: returns SET clause for UPDATE excluding cols from constraint */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views._update_set_cols(_schema text,
                                      _view text,
                                      _src text,
                                      _conname text DEFAULT (NULL))
RETURNS text AS $$
SELECT string_agg(format('%1$I = (%2$s).%1$I', a.attname, _src),', ')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
WHERE n.nspname = _schema AND c.relname = _view AND a.attnum > 0 AND
      a.attnum NOT IN (
        SELECT unnest(cc.conkey)
        FROM pg_catalog.pg_constraint cc
        WHERE cc.conrelid = c.oid AND
              ((_conname IS NOT NULL AND cc.conname = _conname) OR
               (_conname IS NULL     AND cc.contype = 'p')));
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;


/**
 * Concurrently update a materialized view as created by
 * pseudo_mat_views.create_view().
 *
 * Updates will be recorded in a table named ${original_view_name}_updates,
 * which will have these columns:
 *
 *  - id        (BIGINT TX ID)
 *  - tstamp    (the timestamp given to this function)
 *  - awld      (deleted row)
 *  - noo       (inserted row)
 *
 * The given `id' should not have been used already in the history table.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.refresh_view(given_schema_name text,
        given_view_name text,
        tstamp timestamp without time zone DEFAULT (current_timestamp))
RETURNS void AS $$
DECLARE
    _source_schema_name text;
    _source_view_name text;
    _starttime TIMESTAMP WITHOUT TIME ZONE := clock_timestamp();
    t TIMESTAMP WITHOUT TIME ZONE;
    _state pseudo_mat_views.state;
    _prefix TEXT := '_' || given_schema_name || '__' || given_view_name;

    /* For warning/notice/info messages */
    _count_old          bigint;
    _count_new          bigint;
    _count_deltas       bigint;
    _count_deltas_nulls bigint;
    _sample record;
BEGIN
    SELECT * INTO _state
    FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND
          view_name = given_view_name;

    IF _state IS NULL THEN
        RAISE EXCEPTION 'Pseudo-materialized view %.% does not exist',
            given_schema_name, given_view_name;
    END IF;

    _source_schema_name := _state.source_schema_name;
    _source_view_name   := _state.source_view_name;

    RAISE DEBUG 'refresh 0 start refresh %.% at %', given_schema_name,
        given_view_name, clock_timestamp();

    /* Keep refresh history small */
    DELETE FROM pseudo_mat_views.refresh_history
    WHERE _start < current_timestamp - '7 days'::interval;

    /* Record refresh history */
    INSERT INTO pseudo_mat_views.refresh_history
        (_schema, _view, _op)
    SELECT given_schema_name, given_view_name, 'refresh';

    /*
     * Take EXCLUSIVE explicit LOCK on given_schema_name.given_view_name.  This allows
     * concurrent reads and even writes to the materialization, but ensures
     * only one session refreshes this at a time.
     */
    t := clock_timestamp();

    EXECUTE format($q$
            LOCK TABLE %1$I.%2$I IN EXCLUSIVE MODE;
        $q$, given_schema_name, given_view_name || '_new');
    RAISE DEBUG 'refresh 1 % %', clock_timestamp(), clock_timestamp() - t;

    IF (SELECT needs_first_refresh
        FROM pseudo_mat_views.state
        WHERE schema_name = given_schema_name AND
              view_name = given_view_name) THEN

        /*
         * First materialization: load directly from source view; do not record
         * _updates.
         */
        t := clock_timestamp();
        EXECUTE format($q$
                INSERT INTO %1$I.%2$I SELECT DISTINCT * FROM %3$I.%4$I
                ON CONFLICT DO NOTHING;
            $q$, given_schema_name, given_view_name,
            _source_schema_name, _source_view_name);
        RAISE DEBUG 'refresh 2 % %', clock_timestamp(), clock_timestamp() - t;
    ELSE
        /* Materialize the VIEW into a _new table for diff'ing */
        t := clock_timestamp();
        EXECUTE format($q$
                DELETE FROM %1$I.%2$I;
                INSERT INTO %1$I.%2$I SELECT DISTINCT * FROM %3$I.%4$I
                ON CONFLICT DO NOTHING;
            $q$, given_schema_name, given_view_name || '_new',
            _source_schema_name, _source_view_name);
        RAISE DEBUG 'refresh 3 % %', clock_timestamp(), clock_timestamp() - t;

        /*
         * Compute diffs into _deltas using a full outer join of current and
         * _new materializations.
         */
        t := clock_timestamp();
        EXECUTE format($q$
                DELETE FROM %1$I.%2$I; /* truncate previous _deltas */
            $q$, given_schema_name, given_view_name || '_deltas');
        RAISE DEBUG 'refresh 4 % %', clock_timestamp(), clock_timestamp() - t;
        t := clock_timestamp();
        EXECUTE format($q$
                INSERT INTO %1$I.%4$I /* _deltas */ (awld, noo)
                SELECT awld, noo
                FROM %1$I.%2$I awld /* current materialization */
                %5$s FULL OUTER
                JOIN %1$I.%3$I noo  /* new     materialization */ %6$s
                /*
                 * XXX Investigate whether this OR is slow and replacing with
                 * UNION ALL would be faster:
                 */
                WHERE awld IS NOT DISTINCT FROM NULL OR
                      noo IS NOT DISTINCT FROM NULL;
            $q$, given_schema_name, given_view_name,
            given_view_name || '_new', given_view_name || '_deltas',
            CASE pseudo_mat_views._has_pk(given_schema_name, given_view_name)
                WHEN TRUE THEN '' ELSE 'NATURAL' END,
            pseudo_mat_views._using(given_schema_name, given_view_name));
        RAISE DEBUG 'refresh 5 % %', clock_timestamp(), clock_timestamp() - t;
        EXECUTE format($q$
                SELECT count(*) FROM %1$I.%2$I
            $q$, given_schema_name, given_view_name) INTO _count_old;
        EXECUTE format($q$
                SELECT count(*) FROM %1$I.%2$I
            $q$, given_schema_name, given_view_name || '_new') INTO _count_new;
        EXECUTE format($q$
                SELECT count(*) FROM %1$I.%2$I
            $q$, given_schema_name, given_view_name || '_deltas') INTO _count_deltas;
        EXECUTE format($q$
                SELECT * FROM %1$I.%2$I LIMIT 1
            $q$, given_schema_name, given_view_name || '_deltas') INTO _sample;
        IF _count_deltas = 0 THEN
            RAISE DEBUG 'refresh %.% old, new: % %; no changes',
                given_schema_name, given_view_name || '_deltas',
                _count_old, _count_new;
        ELSE
            RAISE DEBUG 'refresh %.% old, new, delta counts: % % %; sample %',
                given_schema_name, given_view_name || '_deltas',
                _count_old, _count_new, _count_deltas, _sample;

            IF _count_deltas > _state.delta_warn_limit AND
               _count_old    > _state.delta_warn_limit THEN
                /*
                 * In PG, both of these are TRUE:
                 *
                 *  row('foo',null) IS     NULL
                 *  row('foo',null) IS NOT NULL
                 *
                 * We use this to test if a non-NULL row contains NULLs in one
                 * or more columns.
                 */
                EXECUTE format($q$
                        SELECT count(*)
                        FROM %1$I.%2$I d
                        WHERE (d.awld IS DISTINCT FROM NULL AND d.awld IS NULL AND d.awld IS NOT NULL) OR
                              (d.noo  IS DISTINCT FROM NULL AND d.noo  IS NULL AND d.noo  IS NOT NULL);
                    $q$, given_schema_name, given_view_name || '_deltas') INTO _count_deltas_nulls;
                IF _count_deltas_nulls > 0 THEN
                    RAISE WARNING
                        'refresh %.% % NULLs in the source VIEW?!',
                        given_schema_name, given_view_name, _count_deltas_nulls;
                END IF;
                RAISE WARNING 'refresh %.% old, new, delta counts: % % %; sample %',
                    given_schema_name, given_view_name || '_deltas',
                    _count_old, _count_new, _count_deltas, _sample;
            END IF;

            /*
             * Update materialization table of view.
             *
             * We assume no dups in the view or that dups are harmless.  If the view
             * produces dups then we could add an "AND NOT EXISTS (SELECT t FROM
             * %1$I.%2$I t WHERE t = upd.noo)" clause.
             *
             * We do deletions first so that we don't cause collisions if the user
             * should have created a PRIMARY KEY or UNIQUE constraint/index on the
             * materialization table.
             */
            RAISE DEBUG 'refresh 6 % %', clock_timestamp(), clock_timestamp() - t;
            IF NOT pseudo_mat_views._has_pk(given_schema_name, given_view_name) THEN
                /*
                 * We have to do a DELETE with a JOIN, either USING, or WHERE
                 * ... IN (SELECT ..)).  Specifically an equi-join.
                 *
                 * Here we don't have a PK on the materialized view, so we use
                 * a record value equi-join.
                 */
                EXECUTE format($q$
                        DELETE FROM %1$I.%2$I mv
                        USING %1$I.%3$I AS upd
                        WHERE mv = upd.awld AND
                              upd.noo IS NOT DISTINCT FROM NULL AND
                              upd.awld IS DISTINCT FROM NULL;
                    $q$,
                    given_schema_name, given_view_name, given_view_name || '_deltas');
            ELSE
                /*
                 * This materialized view has a PK.  The optimizer is not smart
                 * enough to decompose record equality joins into a join on
                 * some key that is indexed + equality for non-key columns :(
                 *
                 * Therefore we generate a conjunction of equality predicates
                 * for the PRIMARY KEY columns of the view.
                 *
                 * This makes this DELETE orders of magnitude faster than the
                 * form for the case where the view has no PRIMARY KEY.
                 */
                EXECUTE format($q$
                        DELETE FROM %1$I.%2$I mv
                        USING %1$I.%3$I AS upd
                        WHERE %4$s AND
                              upd.noo IS NOT DISTINCT FROM NULL AND
                              upd.awld IS DISTINCT FROM NULL;
                    $q$,
                    given_schema_name, given_view_name, given_view_name || '_deltas',
                    pseudo_mat_views._gen_eq(given_schema_name,
                                             given_view_name,
                                             'mv', 'upd', null, 'awld'));
            END IF;
            RAISE DEBUG 'refresh 7 % %', clock_timestamp(), clock_timestamp() - t;
            EXECUTE format($q$
                    INSERT INTO %1$I.%2$I
                    SELECT (upd.noo).*
                    FROM %1$I.%3$I upd
                    WHERE upd.awld IS NOT DISTINCT FROM NULL AND
                          upd.noo IS DISTINCT FROM NULL
                    ON CONFLICT DO NOTHING;
                $q$, given_schema_name, given_view_name, given_view_name || '_deltas');
            RAISE DEBUG 'refresh 8 % %', clock_timestamp(), clock_timestamp() - t;
            IF pseudo_mat_views._has_pk(given_schema_name, given_view_name) THEN
                EXECUTE format($q$
                        UPDATE %1$I.%2$I AS mv
                        SET %4$s
                        FROM %1$I.%3$I upd
                        WHERE upd.awld IS DISTINCT FROM NULL AND
                              upd.noo  IS DISTINCT FROM NULL AND
                              (%5$s) IN (SELECT %5$s FROM (SELECT (upd.noo).*) q);
                    $q$, given_schema_name, given_view_name, given_view_name || '_deltas',
                    pseudo_mat_views._update_set_cols(given_schema_name, given_view_name, 'mv', 'upd.noo'),
                    pseudo_mat_views._constraint_cols(given_schema_name, given_view_name));
            END IF;
            RAISE DEBUG 'refresh 9 % %', clock_timestamp(), clock_timestamp() - t;

            /*
             * Record _deltas in _updates table if the triggers for that were
             * disabled.
             */
            IF (SELECT count(*)
                FROM pseudo_mat_views.triggers
                WHERE tg_enabled_now AND
                      tg_name IN (_prefix || '_ins_trigger',
                                  _prefix || '_upd_trigger',
                                  _prefix || '_del_trigger')) <> 3 THEN
                EXECUTE format($q$
                        INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                        SELECT txid_current(),
                               CAST(%4$L AS timestamp without time zone),
                               awld, noo
                        FROM %1$I.%2$I;
                    $q$, given_schema_name, given_view_name || '_deltas',
                    given_view_name || '_updates', tstamp);
            END IF;
        END IF;
    END IF;
    RAISE DEBUG 'refresh 10 % %', clock_timestamp(), clock_timestamp() - t;

    UPDATE pseudo_mat_views.state
    SET needs_refresh = false,
        needs_first_refresh = false,
        last_update = CAST(_starttime AS timestamp without time zone),
        last_update_time =
            CAST(clock_timestamp() AS timestamp without time zone) -
            CAST(_starttime AS timestamp without time zone),
        txid = txid_current()
    WHERE schema_name = given_schema_name AND view_name = given_view_name;

    RAISE DEBUG 'refresh 11 % %', clock_timestamp(), clock_timestamp() - t;

    UPDATE pseudo_mat_views.refresh_history
    SET _end = clock_timestamp()
    WHERE _schema = given_schema_name AND _view = given_view_name AND
          _op = 'refresh' AND
          _txid = txid_current();

    RAISE DEBUG 'refresh 12 % %', clock_timestamp(), clock_timestamp() - _starttime;

    UPDATE pseudo_mat_views.state
    SET txid = txid_current()
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
    RAISE DEBUG 'refresh 13 % %', clock_timestamp(), clock_timestamp() - _starttime;
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path FROM CURRENT;

/*
 * Refresh multiple views, in declared/default refresh order.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.refresh_views(given_schema_name text DEFAULT (NULL),
        tstamp timestamp without time zone DEFAULT (current_timestamp))
RETURNS void AS $$
DECLARE
    r record;
BEGIN
    FOR r IN (
            SELECT schema_name, view_name, refresh_enabled
            FROM pseudo_mat_views.state
            WHERE given_schema_name IS NULL OR
                  given_schema_name = schema_name
            ORDER BY refresh_order asc
        )
    LOOP
        CONTINUE WHEN NOT r.refresh_enabled;
        PERFORM pseudo_mat_views.refresh_view(r.schema_name, r.view_name, tstamp);
    END LOOP;
END
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path FROM CURRENT;

/**
 * Marks the given view as needing a refresh, and NOTIFYs the refresh
 * channel(s).
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.set_needs_refresh(given_schema_name text,
        given_view_name text, msg text)
RETURNS void AS $$
    UPDATE pseudo_mat_views.state
    SET needs_refresh = true
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
    -- NOTIFY global subscribers
    SELECT pg_notify('pseudo_mat_views',
        format('NEEDS_REFRESH:%I.%I:%s', given_schema_name, given_view_name,
            coalesce(msg, '')));

    -- NOTIFY view-specpfic subscribers; empty message -> needs refresh
    SELECT pg_notify(needs_refresh_notify_channel, coalesce(msg, ''))
    FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name AND
        needs_refresh_notify_channel IS NOT NULL AND
        needs_refresh_notify_channel != '';
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

CREATE OR REPLACE FUNCTION
    pseudo_mat_views.set_needs_refresh(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
SELECT pseudo_mat_views.set_needs_refresh(given_schema_name, given_view_name,
    NULL);
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/**
 * Returns true if a materialized view needs to be refreshed.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.needs_refresh_p(given_schema_name text,
        given_view_name text, given_sla interval, given_debounce interval)
RETURNS boolean AS $$
    SELECT true
    FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name AND
        (needs_refresh OR needs_first_refresh OR
         (last_update +
             coalesce(given_debounce, debounce, last_update_time,
                      coalesce(given_sla, sla, interval '10 minutes') / 4) < current_timestamp) AND
          (periodic_refresh_needed AND
           coalesce(given_sla, sla, interval '10 minutes') > interval '0 second' AND
           last_update + coalesce(given_sla, sla, interval '10 minutes') < current_timestamp))
    UNION ALL
    SELECT false
    ORDER BY 1 DESC LIMIT 1;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/**
 * Returns the amount of time before the next refresh is due, if one is due at
 * all.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.time_to_next_refresh(given_schema_name text,
        given_view_name text, given_sla interval, given_debounce interval)
RETURNS interval AS $$
    SELECT
        CASE WHEN needs_refresh OR needs_first_refresh OR periodic_refresh_needed THEN
            last_update +
                coalesce(given_debounce, debounce, last_update_time,
                         coalesce(given_sla, sla, interval '10 minutes') / 4) -
                CAST(clock_timestamp() AS timestamp without time zone)
        ELSE null::interval END
    FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/**
 * Returns true if a materialized view needs to be refreshed.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.needs_refresh_p(given_schema_name text,
        given_view_name text, given_sla interval)
RETURNS boolean AS $$
    SELECT pseudo_mat_views.needs_refresh_p(given_schema_name, given_view_name,
        given_sla, NULL::interval);
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/**
 * Returns true if a materialized view needs to be refreshed.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.needs_refresh_p(given_schema_name text, given_view_name text)
RETURNS boolean AS $$
    SELECT pseudo_mat_views.needs_refresh_p(given_schema_name, given_view_name,
        NULL::interval);
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

/* XXX Grants needed? */
/* XXX We should grant permission to all to call all these functions... */

/* Tests */

SELECT set_config('pseudo_mat_views.test_value', 'abc', false);
SELECT set_config('pseudo_mat_views.test_value2', '123', false);
SELECT set_config('pseudo_mat_views.test_value3', 'xyz', false);
SELECT pseudo_mat_views.drop_view('pseudo_mat_views', 'test_view', true);
DROP VIEW IF EXISTS pseudo_mat_views.test_view_source;
CREATE VIEW pseudo_mat_views.test_view_source AS
SELECT current_setting('pseudo_mat_views.test_value') AS c
UNION ALL
SELECT current_setting('pseudo_mat_views.test_value2')
UNION ALL
SELECT current_setting('pseudo_mat_views.test_value3');

SELECT pseudo_mat_views.create_view('pseudo_mat_views', 'test_view',
                                    'pseudo_mat_views', 'test_view_source');

DROP TABLE IF EXISTS pseudo_mat_views.test_view_expected;
CREATE TABLE pseudo_mat_views.test_view_expected (id bigint, awld
    pseudo_mat_views.test_view, noo pseudo_mat_views.test_view_new);

CREATE OR REPLACE FUNCTION
    pseudo_mat_views.test_view_unequal_row_count()
RETURNS bigint AS $$
SELECT x.c + y.c FROM (
    SELECT count(*) c
    FROM (
        SELECT *
        FROM pseudo_mat_views.test_view
        EXCEPT
        SELECT *
        FROM pseudo_mat_views.test_view_source) a) x
    CROSS JOIN (
    SELECT count(*) c
    FROM (
        SELECT *
        FROM pseudo_mat_views.test_view_source
        EXCEPT
        SELECT *
        FROM pseudo_mat_views.test_view) a) y;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

CREATE OR REPLACE FUNCTION
    pseudo_mat_views.test_view_unequal_rows()
RETURNS TABLE (label text, c text) AS $$
    SELECT 'Row in test_view but not in test_view_source',
           x.*
    FROM (
        SELECT *
        FROM pseudo_mat_views.test_view
        EXCEPT
        SELECT *
        FROM pseudo_mat_views.test_view_source
    ) x
    UNION ALL
    SELECT 'Row in test_view_source but not in test_view',
           y.*
    FROM (
        SELECT *
        FROM pseudo_mat_views.test_view_source
        EXCEPT
        SELECT *
        FROM pseudo_mat_views.test_view
    ) y;
$$ LANGUAGE SQL VOLATILE SET search_path FROM CURRENT;

select pseudo_mat_views.refresh_view('pseudo_mat_views', 'test_view');
SELECT 'TEST CASE #0';
SELECT CASE x WHEN 0 THEN 'PASS' ELSE 'FAIL ' || CAST(x AS text) END
FROM pseudo_mat_views.test_view_unequal_row_count() x;
SELECT * FROM pseudo_mat_views.test_view_unequal_rows() x
WHERE pseudo_mat_views.test_view_unequal_row_count() != 0;

DROP TABLE IF EXISTS pseudo_mat_views.test_view_updates_expected;
CREATE TABLE pseudo_mat_views.test_view_updates_expected
    (id bigint,
     awld pseudo_mat_views.test_view,
     noo pseudo_mat_views.test_view_new);

-- First TX: add sdf
BEGIN;
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'sdf';
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT txid_current(), NULL, CAST(row('sdf') AS pseudo_mat_views.test_view_new);
SELECT * FROM pseudo_mat_views.test_view;
SELECT * FROM pseudo_mat_views.test_view_source;

SELECT 'TEST CASE #1';
SELECT CASE x WHEN 0 THEN 'FAIL' ELSE 'PASS ' || CAST(x AS text) END
FROM pseudo_mat_views.test_view_unequal_row_count() x;
SELECT * FROM pseudo_mat_views.test_view_unequal_rows() x
WHERE pseudo_mat_views.test_view_unequal_row_count() = 0;
COMMIT;

-- Second TX: add a1 then rename to a2; must yield only an insert record for a2
BEGIN;
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'a1';
UPDATE pseudo_mat_views.test_view SET c = 'a2' WHERE c = 'a1';
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT txid_current(), NULL, CAST(row('a2') AS pseudo_mat_views.test_view_new);

-- Third TX: add b1, rename to b2, delete it; must yield nothing
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'b1';
UPDATE pseudo_mat_views.test_view SET c = 'b2' WHERE c = 'b1';
DELETE FROM pseudo_mat_views.test_view WHERE c = 'b2';
COMMIT;

-- Fourth TX: update a2 to a3 then delete it; must yield deletion of a2
BEGIN;
UPDATE pseudo_mat_views.test_view SET c = 'a3' WHERE c = 'a2';
DELETE FROM pseudo_mat_views.test_view WHERE c = 'a3';
SELECT * FROM pseudo_mat_views.test_view_updates;
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT txid_current(), CAST(row('a2') AS pseudo_mat_views.test_view), null;
COMMIT;

-- Check the history table for this test
SELECT 'TEST CASE #2';
SELECT CASE x.c + y.c WHEN 0 THEN 'PASS' ELSE 'FAIL ' || CAST(x.c + y.c AS text) END
FROM (
    SELECT count(*) c FROM (
        SELECT id, awld, noo FROM pseudo_mat_views.test_view_updates
        EXCEPT
        SELECT id, awld, noo FROM pseudo_mat_views.test_view_updates_expected) a
) x, (
    SELECT count(*) c FROM (
        SELECT id, awld, noo FROM pseudo_mat_views.test_view_updates_expected
        EXCEPT
        SELECT id, awld, noo FROM pseudo_mat_views.test_view_updates) a
) y;

-- Cleanup test
SELECT pseudo_mat_views.drop_view('pseudo_mat_views', 'test_view', true);
DROP TABLE pseudo_mat_views.test_view_expected;
DROP TABLE pseudo_mat_views.test_view_updates_expected;
DROP FUNCTION pseudo_mat_views.test_view_unequal_row_count();

-- Grants
GRANT USAGE                    ON SCHEMA pseudo_mat_views TO public;
GRANT SELECT ON ALL TABLES     IN SCHEMA pseudo_mat_views TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pseudo_mat_views TO public;

CREATE OR REPLACE FUNCTION pseudo_mat_views.drop_state()
RETURNS void AS $$
DECLARE
    r record;
BEGIN
    FOR r IN (
            SELECT s.schema_name AS schema_name,
                   s.view_name AS view_name,
                   s.source_schema_name AS source_schema_name,
                   s.source_view_name AS source_view_name
            FROM pseudo_mat_views.state s
            WHERE NOT EXISTS (
                SELECT c.*
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = s.source_schema_name AND
                      c.relname = s.source_view_name
            )
        )
    LOOP
        RAISE NOTICE
            'Dropping state for materialization %.% of since-dropped source view %.%',
            r.schema_name, r.view_name, r.source_schema_name, r.source_view_name;
        EXECUTE format($q$
                DROP TABLE IF EXISTS %1$I.%2$I CASCADE;
                DROP TABLE IF EXISTS %1$I.%3$I CASCADE;
                DROP TABLE IF EXISTS %1$I.%4$I CASCADE;
                DROP TABLE IF EXISTS %1$I.%5$I CASCADE;
            $q$, r.schema_name, r.view_name, r.view_name || '_new',
            r.view_name || '_deltas', r.view_name || '_updates');
        DELETE FROM pseudo_mat_views.state s
        WHERE s.source_schema_name = r.source_schema_name AND
              s.source_view_name = r.source_view_name;
    END LOOP;
END; $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path FROM CURRENT;

CREATE OR REPLACE FUNCTION pseudo_mat_views.event_trig_f_drop_state()
RETURNS event_trigger AS $$
DECLARE tbl record;
BEGIN
    IF current_setting('pseudo_mat_views.enter',true) IS NULL OR
       current_setting('pseudo_mat_views.enter',true) = '' OR
       NOT current_setting('pseudo_mat_views.enter',true)::boolean THEN
        /* Avoid inf. recursion (not needed in this case, but just in case) */
        PERFORM set_config('pseudo_mat_views.enter','true',false);
        PERFORM pseudo_mat_views.drop_state();
    END IF;
    PERFORM set_config('pseudo_mat_views.enter','false',true);
END; $$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path FROM CURRENT;

/*
 * Cleanup state when source views are dropped (e.g., due to DROP SCHEMA ..
 * CASCADE).
 */
CREATE EVENT TRIGGER pseudo_mat_views_drop_state_trig ON ddl_command_end
WHEN tag IN ('DROP VIEW')
EXECUTE PROCEDURE pseudo_mat_views.event_trig_f_drop_state();

/* Re-create trigger functions in case our code changed */
SELECT pseudo_mat_views.create_triggers(schema_name, view_name)
FROM pseudo_mat_views.state;
