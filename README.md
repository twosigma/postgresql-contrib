# postgresql-contrib

This repository contains upcoming contributions from Two Sigma Open
Source, LLC., to PostgreSQL:

 - `pqasyncnotifier.c`

   This is a command that LISTENs on channels and outputs asynchronous
   notifications as soon as they arrive.  (Whereas psql(1) only outputs
   notifications when it gets user input or when a server command
   completes -- in particular it does not output notifications when
   idle!).

   Use this like so: 

   ```
   $ pqasyncnotifier "$PG_URI" some_channel | while read junk; do
   >    printf 'SELECT notified();'
   > done | psql "$PG_URI" -f -
   ```

 - `pseudo_mat_views.sql`

   An alternative implementation of materialized views written entirely
   in PlPgSQL, with the following features that MATERIALIZED VIEWs don't
   have:

    - keeps an updates table with all updates from all refreshes
    - allows DMLs on the materialized view, recording all changes in the
      updates table (this allows one to update a materialized view from
      a TRIGGER)

 - `schema2json.sql` and `schema2json.sh`

   A start at producing a JSON description of SQL schemas, leveraging
   COMMENTs.

 - `audit.sql`

   An audit facility that produces an audit table (for each audited
   table) that use record types of the audited table's composite type.
   This allows relational queries against audit history.

   An `_all` table is also used to audit `row_to_json()`'ed records.

 - `backup.sql`

   Backup and restore data around re-creating a schema:

   ```
   SELECT backup.backup('some_schema'); -- backup some_schema's data
   DROP SCHEMA "some_schema" CASCADE;
   \i some_schema.sql
   SELECT backup.restore('some_schema'); -- restore some_schema's data
   ```
