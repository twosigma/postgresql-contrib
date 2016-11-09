# postgresql-contrib

This repository contains upcoming contributions from Two Sigma Open
Source, LLC., to PostgreSQL:

 - pqasyncnotifier.c

   This is a command that LISTENs on channels and outputs asynchronous
   notifications as soon as they arrive.  (Whereas psql(1) only outputs
   notifications when it gets user input or when a server command
   completes -- in particular it does not output notifications when
   idle!).

 - pseudo_mat_views.sql

   An alternative implementation of materialized views written entirely
   in PlPgSQL, with the following features that MATERIALIZED VIEWs don't
   have:

    - keeps an updates table with all updates from all refreshes
    - allows DMLs on the materialized view, recording all changes in the
      updates table (this allows one to update a materialized view from
      a TRIGGER)
