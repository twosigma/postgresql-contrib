/*
 * Copyright (c) 2016 Two Sigma Open Source, LLC.
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
 * implement PlPgSQL-coded "MATERIALIZED" VIEWs supporting CONCURRENT
 * refreshes and, critically, saving of deltas, as well as triggers (on the
 * view or which modify the view), and indexes.
 *
 * Ideally the MATERIALIZED VIEW feature in PostgreSQL can be improved
 * to include some of the features provided by this implementation.  A
 * desired end state would have syntax like this:
 *
 *   CREATE MATERIALIZED VIEW schema_name.view_name
 *      [ ( <column-name> [, ...] ) ]
 *      [ WITH ( storage_parameter [= value] [, ... ] ) ]
 *      [ TABLESPACE tablespace_name ]
 *   AS <query>
 *   WITH [ [ UNLOGGED ] HISTORY TABLE [ schema_name.view_name_history ], ]
 *        [ PRIMARY KEY ( <column-name> [, ...] ), ]
 *        [ [ NO ] DATA ];
 *
 *
 * SYNOPSIS
 *
 *  -- To create a pseudo-materialized VIEW:
 *  CREATE VIEW ...;
 *  SELECT pseudo_mat_views.create_view(schema_name, view_name);
 *
 *  -- Custom session variable for setting the audit ID to use for tracking
 *  -- materialized view history (postgres 9.3 and up):
 *  SELECT set_config('pseudo_mat_views.current_tx_id', CAST(12345 AS text), false);
 *
 *  -- To refresh a pseudo-materialized VIEW:
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name);
 *  SELECT pseudo_mat_views.refresh_view(schema_name, view_name, id, tstamp);
 *
 *  -- To drop a pseudo-materialized VIEW:
 *  SELECT pseudo_mat_views.drop_view(schema_name, view_name, cascade_bool);
 *
 *  -- To undo the materialization of a VIEW:
 *  SELECT pseudo_mat_views.undo_mat_view(schema_name, view_name, cascade_bool);
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
 *  --
 *  -- (two record-type columns represent changes: awld (old) and noo (new))
 *  SELECT * FROM <view_name>_updates ...;
 *
 *  -- To refer to the VIEW without materialization:
 *  SELECT * FROM <view_name>_source ...;
 *
 *  -- To set a VIEW's update SLA to 5 minutes:
 *  UPDATE pseudo_mat_views.state
 *  SET sla = interval '5 minutes'
 *  WHERE schema_name = '<schema_name>' AND view_name = '<view_name>';
 *
 *  -- To set a VIEW's notification channel:
 *  UPDATE pseudo_mat_views.state
 *  SET needs_refresh_notify_channel = '<channel_name>'
 *  WHERE schema_name = '<schema_name>' AND view_name = '<view_name>';
 *
 *  -- Next current ID for tracking materialized view updates:
 *  --
 *  -- (Note that this is best re-implemented by anyone using this to meet their
 *  --  needs.)
 *  SELECT pseudo_mat_views.current_id();
 *
 * DESCRIPTION
 *
 * MATERIALIZED VIEWs are fantastic, but there's no way to record the changes
 * seen during a REFRESH.  We want to reify those changes.  I.e., we want
 * something like:
 *
 *      CREATE MATERIALIZED VIEW name
 *      AS ...
 *      WITH HISTORY = name_hist (id bigint, tstamp timestamp) ...;
 *
 *      REFRESH MATERIALIZED VIEW name CONCURRENTLY
 *      WITH (id, tstamp) AS (SELECT 123, current_timestamp);
 *
 *      -- Show deltas
 *      SELECT id, tstamp, awld, noo FROM name_hist WHERE id >= 123;
 *
 * with deltas inserted into [name_hist] as (id, tstamp, awld, noo), where
 * [awld] and [noo] are columns storing values of record types containing rows
 * from [name] that are deleted and inserted, respectively.
 *
 * Further more, we want to be able to:
 *
 *  - create indexes on materialized views
 *  - create triggers on materialized views
 *  - create triggers that update materialized views as if they were TABLEs and
 *    record those changes in the view's history table as if the updates had
 *    happened via a refresh
 *
 * For now we PlPgSQL-code this.  If this is welcomed by the PostgreSQL
 * community, then we should properly integrate this into PostgreSQL,
 * with proper syntactic sugar and C coding as necessary.
 *
 * Our implementation is based entirely on PlPgSQL-coding what
 * PostgreSQL actually does in C and SQL to implement MATERIALIZED
 * VIEWs.
 *
 * This file defines several functions for creating, refreshing, dropping
 * and/or undoing materialization of a view, and other operations.  See above.
 *
 * These "materialized views" look and feel almost exactly the same as a
 * PostgreSQL MATERIALIZED VIEW that is REFRESHed CONCURRENTLY, with
 * just the additional capabilities mentioned above and with a
 * function-based interface rather than extended syntax.
 *
 * The generation of updates, copied from PostgreSQL' internals, works
 * as follows:
 *
 *  - acquire an EXCLUSIVE LOCK on the materialized view
 *
 *  - populate a helper TABLE like the materialized one to populate with
 *    SELECT * FROM :the_view;
 *
 *  - execute an INSERT INTO diff_table whose SELECT body does a FULL OUTER
 *    JOIN of the previously-polulated and newly-generated output of the VIEW
 *    where one or the other side is NULL
 *
 *  - DELETE FROM the materialization table the recorded deletions
 *
 *  - INSERT INTO the materialization table the recorded additions
 *
 *  - record the changes
 *
 *  - release the EXCLUSIVE LOCK
 *
 * We could do non-concurrent (atomic) updates by renaming the helper
 * table into place, just as non-concurrent materialization does in PG.
 * We'd still do the FULL OUTER JOIN in that case in order to record the
 * changes.
 *
 * We will skip most of the sanity checks in the PostgreSQL
 * implementation of MATERIALIZED VIEWs:
 *
 *  - we will check if a view's materialization table has contents -- if
 *    not then instead of failing we'll update it but won't record the
 *    updates
 *
 *  - we will NOT check that the view's new contents has no duplicate
 *    rows; instead we'll simply not duplicate inserts in the update
 *    phase (or maybe rely on the view not generating duplicates, or
 *    maybe create a unique index across all columns).
 *
 * We use the PostgreSQL format() function to format SQL statements.
 *
 * We use the ability to refer to an entire row as a single object, and
 * the ability to have record types, to avoid having to refer to any
 * columns from the VIEW being materialized.
 *
 * Because PostgreSQL allows entire rows of tables to be compared for
 * equality without having to name the columns, the SQL statements
 * needed to compute updates of a view are very generic, with the only
 * schema elements embedded in them being table names.
 *
 * NOTES
 *
 *  - VIEWs with output rows that contain NULLs are not properly
 *    supported.
 *
 *    Each time a materialized view is refreshed that has NULLs in some
 *    rows' columns, those rows appear in the updates table as modified.
 *    This is a false positive.  If every row has a NULL in some column,
 *    then the updates table will be very noisy indeed!
 *
 *    This has to do with the semantics of equi-JOINs and NULL.  A row
 *    containing a NULL will not match itself in the new output of the
 *    view during refresh.
 *
 *    Part of the problem is that this code doesn't know how to JOIN on
 *    equality of PRIMARY KEY columns only -- it uses whole row equality
 *    for its JOIN (specifically: a NATURAL FULL OUTER JOIN).  Whereas
 *    if the JOIN was only ON primary key columns then this wouldn't
 *    happen, as SQL does not allow PK columns to be nullable.  But
 *    recall: there is no way to specify a PK when creating a VIEW (nor
 *    a MATERIALIZED VIEW), and UNIQUE INDEXes do allow NULLs, so a
 *    unique index created after creating the view does not help.  One
 *    could ALTER the materialized view to add UNIQUE() and NOT NULL
 *    constraints, and then could generate a FULL OUTER JOIN ON query
 *    that uses only those columns that are part of a UNIQUE()
 *    constraint, but what if there are multiple UNIQUE() constraints?
 *
 *    A new JOIN operator that is like NATURAL FULL OUTER JOIN but which
 *    only compares PK and/or NOT NULL columns, or which uses IS NOT
 *    DISTINCT FROM instead of equality, would be one way to fix this:
 *
 *      NATURAL ... JOIN USING PRIMARY KEY
 *      NATURAL ... JOIN USING DISTINCT
 *
 *    An improved CREATE MATERIALIZED VIEW statement could include a
 *    specification of a PRIMARY KEY, which might help.
 *
 *    In any case, an equi-JOIN USING DISTINCT would be very convenient
 *    in many other applications.
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
 *   NULLs, while the former outputs an appropriate boolean value.
 */

/*
 * XXX Make refresh_view() and such raise exceptions when the view doesn't
 * exist.
 */

/* It's safe to source this file repeatedly */
CREATE SCHEMA IF NOT EXISTS pseudo_mat_views;

/**
 * Returns the current transaction ID, which is used in the recording of
 * materialized view history.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.current_id()
RETURNS bigint AS $$
BEGIN
    DECLARE
        curr_id bigint = CAST(current_setting('pseudo_mat_views.current_tx_id') AS bigint);
    BEGIN
        IF curr_id > -1 THEN
            RETURN curr_id;
        ELSE
            RAISE EXCEPTION 'pseudo_mat_views.current_tx_id current setting not set';
        END IF;
    END;
END $$ LANGUAGE plpgsql VOLATILE;

/**
 * Returns `given_id' if it is not -1, else the current transaction ID as
 * returned by pseudo_mat_views.current_id(void).
 */
CREATE OR REPLACE FUNCTION
pseudo_mat_views.current_id(given_id bigint)
RETURNS bigint AS $$
SELECT CASE given_id WHEN -1 THEN pseudo_mat_views.current_id() ELSE given_id END;
$$ LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION
pseudo_mat_views.triggers_disabled()
RETURNS boolean AS $$
BEGIN
    IF (SELECT current_setting('pseudo_mat_views.disable_triggers') = 'true') THEN
        RETURN true;
    END IF;
    RETURN false;
    EXCEPTION WHEN OTHERS THEN
    RETURN false;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE TABLE IF NOT EXISTS pseudo_mat_views.state
    (schema_name text, view_name text, sla interval,
     needs_refresh boolean, needs_first_refresh boolean,
     needs_refresh_notify_channel text,
     last_update timestamp without time zone, txid bigint
     /*
      * TODO: Add stats, either here or in a separate table, to track timings
      *       of refreshes.  Could be useful.
      */
     );

/* Supporting function */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.drop_triggers(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %3$I ON %1$I.%2$I CASCADE;',
        given_schema_name, given_view_name, given_view_name || '_ins_trigger');
    EXECUTE format('DROP TRIGGER IF EXISTS %3$I ON %1$I.%2$I CASCADE;',
        given_schema_name, given_view_name, given_view_name || '_upd_trigger');
    EXECUTE format('DROP TRIGGER IF EXISTS %3$I ON %1$I.%2$I CASCADE;',
        given_schema_name, given_view_name, given_view_name || '_del_trigger');
END
$$ LANGUAGE plpgsql VOLATILE;

/* Supporting function */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.create_triggers(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
BEGIN
    PERFORM pseudo_mat_views.drop_triggers(given_schema_name, given_view_name);

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
    EXECUTE format('CREATE OR REPLACE FUNCTION %1$I.%2$I()
                    RETURNS trigger AS %4$L language plpgsql VOLATILE;',
                    given_schema_name, given_view_name || '_ins_func',
                    given_view_name || '_updates',
                format('
                    BEGIN
                        IF (SELECT pseudo_mat_views.triggers_disabled()) THEN
                            RETURN NEW;
                        END IF;
                        INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                        SELECT pseudo_mat_views.current_id(),
                               CAST(current_timestamp AS timestamp without time zone),
                               NULL, CAST(row(NEW.*) AS %1$I.%4$I);
                        RETURN NEW;
                    END;',
                    given_schema_name, given_view_name || '_ins_func',
                    given_view_name || '_updates',
                    given_view_name || '_new'));
    EXECUTE format('CREATE OR REPLACE FUNCTION %1$I.%2$I()
                    RETURNS trigger AS %4$L language plpgsql VOLATILE;',
                    given_schema_name, given_view_name || '_upd_func',
                    given_view_name || '_updates',
                format('
                    BEGIN
                        IF (SELECT pseudo_mat_views.triggers_disabled()) THEN
                            RETURN NEW;
                        END IF;
                        UPDATE %1$I.%3$I
                        SET noo = CAST(row(NEW.*) AS %1$I.%4$I)
                        WHERE id = pseudo_mat_views.current_id() AND
                            noo = CAST(row(OLD.*) AS %1$I.%4$I);
                        INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                        SELECT pseudo_mat_views.current_id(),
                               CAST(current_timestamp AS timestamp without time zone),
                               OLD, CAST(row(NEW.*) AS %1$I.%4$I)
                        WHERE NOT EXISTS (
                            SELECT * FROM %1$I.%3$I
                            WHERE id = pseudo_mat_views.current_id() AND
                                noo = CAST(row(NEW.*) AS %1$I.%4$I));
                        RETURN NEW;
                    END;',
                    given_schema_name, given_view_name || '_upd_func',
                    given_view_name || '_updates',
                    given_view_name || '_new'));
    EXECUTE format('CREATE OR REPLACE FUNCTION %1$I.%2$I()
                    RETURNS trigger AS %4$L language plpgsql VOLATILE;',
                    given_schema_name, given_view_name || '_del_func',
                    given_view_name || '_updates',
                format('
                    BEGIN
                        IF (SELECT pseudo_mat_views.triggers_disabled()) THEN
                            RETURN OLD;
                        END IF;
                        -- Insert deletion record if there is no insert/update
                        -- record from same TX to modify
                        INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
                        SELECT pseudo_mat_views.current_id(),
                               CAST(current_timestamp AS timestamp without time zone),
                               OLD, NULL
                        WHERE NOT EXISTS (
                            SELECT * FROM %1$I.%3$I
                            WHERE id = pseudo_mat_views.current_id() AND
                                noo = CAST(row(OLD.*) AS %1$I.%4$I));
                        -- Delete matching insert record from same TX
                        DELETE FROM %1$I.%3$I
                        WHERE id = pseudo_mat_views.current_id() AND
                            awld IS NOT DISTINCT FROM NULL AND
                            noo = CAST(row(OLD.*) AS %1$I.%4$I);
                        -- Update update matching record from same TX to be
                        -- deletion record
                        UPDATE %1$I.%3$I
                        SET noo = NULL
                        WHERE id = pseudo_mat_views.current_id() AND
                            noo = CAST(row(OLD.*) AS %1$I.%4$I);
                        DELETE FROM %1$I.%3$I
                        WHERE id = pseudo_mat_views.current_id() AND
                            awld IS NOT DISTINCT FROM NULL AND noo IS NOT
                            DISTINCT FROM NULL;
                        RETURN OLD;
                    END;',
                    given_schema_name, given_view_name || '_del_func',
                    given_view_name || '_updates',
                    given_view_name || '_new'));

    EXECUTE format('CREATE TRIGGER %3$I AFTER INSERT ON %1$I.%2$I
                    FOR EACH ROW
                    EXECUTE PROCEDURE %1$I.%4$I();',
        given_schema_name, given_view_name, given_view_name || '_ins_trigger',
        given_view_name || '_ins_func');
    EXECUTE format('CREATE TRIGGER %3$I AFTER UPDATE ON %1$I.%2$I
                    FOR EACH ROW
                    EXECUTE PROCEDURE %1$I.%4$I();',
        given_schema_name, given_view_name, given_view_name || '_upd_trigger',
        given_view_name || '_upd_func');
    EXECUTE format('CREATE TRIGGER %3$I AFTER DELETE ON %1$I.%2$I
                    FOR EACH ROW
                    EXECUTE PROCEDURE %1$I.%4$I();',
        given_schema_name, given_view_name, given_view_name || '_del_trigger',
        given_view_name || '_del_func');
END
$$ LANGUAGE plpgsql VOLATILE;


/**
 * This function changes a VIEW to be a materialized view supporting concurrent
 * refreshes and reification of incremental updates.
 *
 * To use this first create a VIEW, then call this function with the VIEW's
 * schema and name.
 *
 * Internally this will rename the VIEW and create a TABLE with the VIEW's
 * original name.
 *
 * After calling this function you may create indices on the VIEW's original
 * name.
 *
 * The VIEW's body should not generate duplicate rows.
 *
 * Because a TABLE will be created with the VIEW's original name, you can (and
 * should) create appropriate INDEXes.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.create_view(given_schema_name text, given_view_name text)
RETURNS void AS $$
DECLARE
    /*
     * If the VIEW we're materializing is TEMP, then all elements we create
     * must be too.  Otherwise, some tables we create can be UNLOGGED.
     */
    kw text = CASE WHEN given_schema_name LIKE 'pg_temp%' THEN 'TEMP' ELSE 'UNLOGGED' END;
    tmp text = CASE WHEN given_schema_name LIKE 'pg_temp%' THEN 'TEMP' ELSE '' END;
BEGIN
    /* XXX Error handling?? */
    /* Note that format()'s %n$ notation is one-based... */
    /* Rename the original VIEW (append _source to its name) */
    EXECUTE format('ALTER VIEW %1$I.%2$I RENAME TO %3$I;',
        given_schema_name, given_view_name, given_view_name || '_source');
    /*
     * Create a TABLE with the VIEW's original name and store the VIEW's
     * content in it immediately.
     */
    EXECUTE format('CREATE ' || tmp || ' TABLE %1$I.%2$I AS SELECT * FROM %1$I.%3$I LIMIT 0;',
        given_schema_name, given_view_name, given_view_name || '_source');
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
    EXECUTE format('CREATE ' || kw || ' TABLE %1$I.%3$I (LIKE %1$I.%2$I INCLUDING ALL);',
        given_schema_name, given_view_name, given_view_name || '_new');
    /*
     * Create a table in which to store deltas for a refresh, and another in
     * which to store historical updates from each refresh and/or triggers.
     */
    EXECUTE format('
        CREATE TABLE %1$I.%4$I (
            id bigint,
            tstamp timestamp without time zone,
            awld %1$I.%2$I,
            noo %1$I.%3$I);',
        given_schema_name,
        given_view_name,
        given_view_name || '_new',
        given_view_name || '_updates');
    EXECUTE format('
        CREATE ' || kw || ' TABLE %1$I.%4$I (
            awld %1$I.%2$I,
            noo %1$I.%3$I);',
        given_schema_name,
        given_view_name,
        given_view_name || '_new',
        given_view_name || '_deltas');
    /* Index it the _updates table */
    EXECUTE format('CREATE INDEX %3$I ON %1$I.%2$I (id, awld, noo);',
        given_schema_name, given_view_name || '_updates',
        given_view_name || '_updates_idx');
    EXECUTE format('CREATE INDEX %3$I ON %1$I.%2$I (awld);',
        given_schema_name, given_view_name || '_updates',
        given_view_name || '_updates_idx_awld');
    EXECUTE format('CREATE INDEX %3$I ON %1$I.%2$I (noo);',
        given_schema_name, given_view_name || '_updates',
        given_view_name || '_updates_idx_noo');

    /* Register this pseudo-materialized view */
    INSERT INTO pseudo_mat_views.state
        (schema_name, view_name, needs_refresh, last_update, txid,
         needs_first_refresh)
    SELECT given_schema_name, given_view_name, true,
        CAST(current_timestamp AS timestamp without time zone),
        pseudo_mat_views.current_id(), true
    /* If it wasn't already registered */
    WHERE NOT EXISTS (SELECT * FROM pseudo_mat_views.state
                      WHERE schema_name = given_schema_name AND
                            view_name = given_view_name);

    /* Create triggers */
    PERFORM pseudo_mat_views.create_triggers(given_schema_name, given_view_name);
END
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Drop a materialized view created by pseudo_mat_views.create_view().
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.drop_view(given_schema_name text, given_view_name text, do_cascade boolean)
RETURNS void AS $$
DECLARE
    c text = CASE do_cascade WHEN true THEN 'CASCADE' ELSE '' END;
BEGIN
    PERFORM pseudo_mat_views.drop_triggers(given_schema_name, given_view_name);
    EXECUTE format('DROP VIEW  IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_source', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_updates', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_deltas', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_new', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name, c);

    /* "Unregister" the view */
    DELETE FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
END
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Undo materialization of a VIEW, restoring it to its previously
 * unmaterialized state.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.undo_mat_view(given_schema_name text, given_view_name text, do_cascade boolean)
RETURNS void AS $$
DECLARE
    c text = CASE do_cascade WHEN true THEN 'CASCADE' ELSE '' END;
BEGIN
    PERFORM pseudo_mat_views.drop_triggers(given_schema_name, given_view_name);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_updates', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_deltas', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name || '_new', c);
    EXECUTE format('DROP TABLE IF EXISTS %1$I.%2$I %3s;',
                   given_schema_name, given_view_name, c);
    EXECUTE format('ALTER VIEW IF EXISTS %1$I.%3$I RENAME TO %2$I;',
                   given_schema_name, given_view_name,
                   given_view_name || '_source', c);

    /* "Unregister" the view */
    DELETE FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
END
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Concurrently update a materialized view as created by
 * pseudo_mat_views.create_view().
 *
 * Updates will be recorded in a table named ${original_view_name}_updates,
 * which will have these columns:
 *
 *  - id        (the ID given to this function)
 *  - tstamp    (the timestamp given to this function)
 *  - awld      (deleted row)
 *  - noo       (inserted row)
 *
 * The given `id' should not have been used already in the history table.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.refresh_view(given_schema_name text,
        given_view_name text,
        id bigint, tstamp timestamp without time zone)
RETURNS void AS $$
DECLARE
    actual_id bigint = pseudo_mat_views.current_id(id);
BEGIN
    PERFORM set_config('pseudo_mat_views.disable_triggers', 'true', false);
    /*
     * Take EXCLUSIVE explicit LOCK on given_schema_name.given_view_name.  This allows
     * concurrent reads but not writes.
     */
    EXECUTE format('LOCK TABLE %1$I.%2$I IN EXCLUSIVE MODE;',
                   given_schema_name, given_view_name || '_new');

    /* Materialize the VIEW into a _new table */
    EXECUTE format('DELETE FROM %1$I.%2$I;', given_schema_name, given_view_name || '_new');
    EXECUTE format('INSERT INTO %1$I.%3$I SELECT DISTINCT * FROM %1$I.%2$I;',
                   given_schema_name, given_view_name || '_source', given_view_name || '_new');

    /*
     * XXX We should have a WITH DATA / WITH NO DATA option.
     */

    /* Compute diffs using a full outer join */
    EXECUTE format('DELETE FROM %1$I.%2$I;',
        given_schema_name, given_view_name || '_deltas');
    EXECUTE format('
        INSERT INTO %1$I.%4$I (awld, noo)
        SELECT awld, noo
        FROM %1$I.%2$I awld
        NATURAL FULL OUTER JOIN %1$I.%3$I noo
        WHERE awld IS NOT DISTINCT FROM NULL OR noo IS NOT DISTINCT FROM NULL;',
        given_schema_name, given_view_name,
        given_view_name || '_new',
        given_view_name || '_deltas');

    /*
     * Update materialization table of view.
     *
     * We assume no dups in the view or that dups are harmless.  If the view
     * produces dups then we could add an "AND NOT EXISTS (SELECT t FROM
     * %1$I.%2$I t WHERE t = upd.noo)" clause.
     *
     * We do deletions first so that we don't cause collisions if the user
     * should have created a UNIQUE INDEX on the materialized view.
     */
    EXECUTE format('
        DELETE FROM %1$I.%2$I mv
        WHERE mv IN (SELECT upd.awld
                     FROM %1$I.%3$I upd
                     WHERE upd.noo IS NOT DISTINCT FROM NULL AND upd.awld IS DISTINCT FROM NULL);',
        given_schema_name, given_view_name, given_view_name || '_deltas');
    EXECUTE format('
        INSERT INTO %1$I.%2$I
        SELECT (upd.noo).*
        FROM %1$I.%3$I upd
        WHERE upd.awld IS NOT DISTINCT FROM NULL AND upd.noo IS DISTINCT FROM NULL;',
        given_schema_name, given_view_name, given_view_name || '_deltas');

    /*
     * Record _deltas in _updates table.
     *
     * Don't leave noise in the history table for the first
     * materialization.
     *
     * XXX Maybe this should be an option.  After all, one might want a
     * history table to be the truth for all time.
     */
    IF NOT (SELECT needs_first_refresh
        FROM pseudo_mat_views.state
        WHERE schema_name = given_schema_name AND view_name = given_view_name)
    THEN
        EXECUTE format('
            INSERT INTO %1$I.%3$I (id, tstamp, awld, noo)
            SELECT CAST(%4$L AS bigint),
                   CAST(%5$L AS timestamp without time zone),
                   awld, noo
            FROM %1$I.%2$I;',
            given_schema_name,
            given_view_name || '_deltas',
            given_view_name || '_updates', actual_id, tstamp);
    END IF;

    UPDATE pseudo_mat_views.state
    SET needs_refresh = false,
        needs_first_refresh = false,
        last_update = CAST(current_timestamp AS timestamp without time zone),
        txid = pseudo_mat_views.current_id()
    WHERE schema_name = given_schema_name AND view_name = given_view_name;
    PERFORM set_config('pseudo_mat_views.disable_triggers', 'false', false);
END
$$ LANGUAGE plpgsql VOLATILE;

/**
 * Refresh a pseudo-materialized view using the current_timestamp and
 * pseudo_mat_views.current_id().
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.refresh_view(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
BEGIN
    PERFORM pseudo_mat_views.refresh_view(given_schema_name, given_view_name,
        CAST(-1 AS bigint),
        CAST(current_timestamp AS timestamp without time zone));
END
$$ LANGUAGE plpgsql VOLATILE;

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
$$ LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION
    pseudo_mat_views.set_needs_refresh(given_schema_name text,
        given_view_name text)
RETURNS void AS $$
SELECT pseudo_mat_views.set_needs_refresh(given_schema_name, given_view_name,
    NULL);
$$ LANGUAGE SQL VOLATILE;

/**
 * Returns true if a materialized view needs to be refreshed.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.needs_refresh_p(given_schema_name text,
        given_view_name text, given_sla interval)
RETURNS boolean AS $$
    SELECT true
    FROM pseudo_mat_views.state
    WHERE schema_name = given_schema_name AND view_name = given_view_name AND
        (needs_refresh OR needs_first_refresh OR
         (last_update + coalesce(given_sla, sla, interval '10 minutes') < current_timestamp));
$$ LANGUAGE SQL VOLATILE;

/**
 * Returns true if a materialized view needs to be refreshed.
 */
CREATE OR REPLACE FUNCTION
    pseudo_mat_views.needs_refresh_p(given_schema_name text, given_view_name text)
RETURNS boolean AS $$
    SELECT pseudo_mat_views.needs_refresh_p(given_schema_name, given_view_name,
        NULL::interval);
$$ LANGUAGE SQL VOLATILE;

/*
 * Tests
 *
 * XXX isolate into separate schema to make it easier to drop their schema
 * elements.
 */

SELECT set_config('pseudo_mat_views.test_value', 'abc', false);
SELECT set_config('pseudo_mat_views.test_value2', '123', false);
SELECT set_config('pseudo_mat_views.test_value3', 'xyz', false);
SELECT pseudo_mat_views.drop_view('pseudo_mat_views', 'test_view', true);
DROP VIEW IF EXISTS pseudo_mat_views.test_view;
CREATE VIEW pseudo_mat_views.test_view AS
SELECT current_setting('pseudo_mat_views.test_value') AS c
UNION ALL
SELECT current_setting('pseudo_mat_views.test_value2')
UNION ALL
SELECT current_setting('pseudo_mat_views.test_value3');

SELECT set_config('pseudo_mat_views.current_tx_id', '1234', false);
SELECT pseudo_mat_views.create_view('pseudo_mat_views', 'test_view');

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
$$ LANGUAGE SQL VOLATILE;

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
$$ LANGUAGE SQL VOLATILE;

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
SELECT set_config('pseudo_mat_views.current_tx_id', '1235', false);
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'sdf';
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT 1235, NULL, CAST(row('sdf') AS pseudo_mat_views.test_view_new);
SELECT * FROM pseudo_mat_views.test_view;
SELECT * FROM pseudo_mat_views.test_view_source;

SELECT 'TEST CASE #1';
SELECT CASE x WHEN 0 THEN 'FAIL' ELSE 'PASS ' || CAST(x AS text) END
FROM pseudo_mat_views.test_view_unequal_row_count() x;
SELECT * FROM pseudo_mat_views.test_view_unequal_rows() x
WHERE pseudo_mat_views.test_view_unequal_row_count() = 0;

-- Second TX: add a1 then rename to a2; must yield only an insert record for a2
SELECT set_config('pseudo_mat_views.current_tx_id', '1236', false);
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'a1';
UPDATE pseudo_mat_views.test_view SET c = 'a2' WHERE c = 'a1';
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT 1236, NULL, CAST(row('a2') AS pseudo_mat_views.test_view_new);

-- Third TX: add b1, rename to b2, delete it; must yield nothing
SELECT set_config('pseudo_mat_views.current_tx_id', '1237', false);
INSERT INTO pseudo_mat_views.test_view (c) SELECT 'b1';
UPDATE pseudo_mat_views.test_view SET c = 'b2' WHERE c = 'b1';
DELETE FROM pseudo_mat_views.test_view WHERE c = 'b2';

-- Fourth TX: update a2 to a3 then delete it; must yield deletion of a2
SELECT set_config('pseudo_mat_views.current_tx_id', '1238', false);
UPDATE pseudo_mat_views.test_view SET c = 'a3' WHERE c = 'a2';
DELETE FROM pseudo_mat_views.test_view WHERE c = 'a3';
SELECT * FROM pseudo_mat_views.test_view_updates;
INSERT INTO pseudo_mat_views.test_view_updates_expected (id, awld, noo)
SELECT 1238, CAST(row('a2') AS pseudo_mat_views.test_view), null;

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

