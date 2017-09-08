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

\set ON_ERROR_STOP on
SET client_min_messages TO WARNING;
RESET SESSION AUTHORIZATION; /* run as superuser pls */

/*
 * Check that we have env vars we'll need for shell escapes for, e.g.,
 * evaluating inlined, commented-out, COMMENT statements.  See schema.sql.
 */

CREATE TEMP TABLE vars (name TEXT PRIMARY KEY, value TEXT, msg TEXT);

CREATE OR REPLACE FUNCTION pg_temp.check_vars()
RETURNS void AS $$
DECLARE
    _missing_var pg_temp.vars;
BEGIN
    IF (SELECT bool_or(length(value) = 0) FROM pg_temp.vars) THEN
        _missing_var := (
            SELECT var
            FROM pg_temp.vars var
            WHERE length(value) = 0
            ORDER BY name ASC LIMIT 1);
        RAISE EXCEPTION 'Could not work out a value for %; %',
            _missing_var.name, _missing_var.msg;
    END IF;
    RETURN;
END; $$ LANGUAGE PlPgSQL;

/* Allow these vars to be set on the psql command line */
\setenv psql_ROOT       :ROOT
\setenv psql_FNAME      :FNAME
\setenv psql_PG_IPC_URI :PG_IPC_URI
\set ROOT `if test "$psql_ROOT" = :ROOT; then echo "$ROOT"; else echo "$psql_ROOT"; fi`
\set PG_IPC_URI `if test "$psql_PG_IPC_URI" = :PG_IPC_URI; then echo "$PG_IPC_URI"; else echo "$psql_PG_IPC_URI"; fi`
INSERT INTO pg_temp.vars(name,value,msg)
VALUES  ('ROOT', :'ROOT', 'Must set $ROOT environment variable'),
        ('PG_IPC_URI', :'PG_IPC_URI', 'Must set $PG_IPC_URI environment variable');
SELECT pg_temp.check_vars();

/*
 * It's useful to know what file this is in.
 *
 * We rely on stdin of the parent process (psql) of this child shell to find
 * the name of the file.  If we use psql -f <filename>, then we still need to
 * redirect stdin from <filename>, else this won't work.  I.e., run these SQL
 * files like so:
 *
 *      psql "$PG_URI" -f "$sql_file" < "$sql_file"
 */
\set stdin_FNAME `readlink --canonicalize $(lsof -F n -p $PPID -a -d0 | grep ^n | sed -e 's/^n//')`
\setenv psql_stdin_FNAME :stdin_FNAME
\set FNAME `if test "$psql_FNAME" = :psql_FNAME; then echo "$psql_sdtin_FNAME"; else echo $psql_FNAME; fi`
INSERT INTO pg_temp.vars(name,value,msg)
VALUES('FNAME',:'FNAME',
       'Could not determine name of this file; use psql "$PG_URI" -f "$sql_file" < "$sql_file"');
SELECT pg_temp.check_vars();

\setenv psql_FNAME :FNAME
\set FNAME_IS_REGULAR `test -f $(readlink --canonicalize "${psql_FNAME}") && echo yes`
INSERT INTO pg_temp.vars(name,value,msg)
VALUES('FNAME_IS_REGULAR',:'FNAME_IS_REGULAR',
       :'FNAME' || ' is not a regular file!');
SELECT pg_temp.check_vars();

/*
 * We need to know what schema we're creating for the GRANTs we do below.
 */
select :'FNAME' AS FNAME;
\set SCHEMA `grep '^CREATE SCHEMA' $(readlink --canonicalize "${psql_FNAME}") | cut -d' ' -f6 | tr -d ';' | tail -1`
INSERT INTO pg_temp.vars(name,value,msg)
VALUES('SCHEMA',:'SCHEMA',
       'Could not determine name of SCHEMA created by this file');
SELECT pg_temp.check_vars();

CREATE OR REPLACE FUNCTION pg_temp.create_role(_role TEXT,
                                               _attrs TEXT DEFAULT(''))
RETURNS void AS $$
BEGIN
    EXECUTE format('CREATE ROLE %I %s;', _role, _attrs);
EXCEPTION WHEN OTHERS THEN
    EXECUTE format('ALTER ROLE %I %s;', _role, _attrs);
END; $$ LANGUAGE PlPgSQL;

CREATE OR REPLACE FUNCTION  pg_temp.make_super_role(_role TEXT, _schema TEXT)
RETURNS void AS $$
BEGIN
    EXECUTE format($q$
        GRANT ALL PRIVILEGES ON SCHEMA %2$I TO %1$I;
        GRANT ALL PRIVILEGES ON ALL TABLES
              IN SCHEMA %2$I TO %1$I;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES
              IN SCHEMA %2$I TO %1$I;
        GRANT EXECUTE ON ALL FUNCTIONS
              IN SCHEMA %2$I TO %1$I;
        $q$, _role, _schema);
END; $$ LANGUAGE PlPgSQL;

CREATE SCHEMA IF NOT EXISTS :"SCHEMA";

SELECT pg_temp.create_role('FooAdmin', 'NOLOGIN');
SELECT pg_temp.create_role('FooTester', 'NOLOGIN SUPERUSER');
SELECT pg_temp.create_role('FooSchemaLoader', 'NOLOGIN SUPERUSER');
SELECT pg_temp.create_role('FooDefaultOwner', 'NOLOGIN');
SELECT pg_temp.make_super_role(q.r, :'SCHEMA')
FROM (SELECT 'FooAdmins' UNION ALL
      SELECT 'FooTester' UNION ALL
      SELECT 'FooSchemaLoader') q(r);

/*
 * Everything else to be created by the file that is including this one needs
 * to be owned by "FooSchemaLoader", so we'll run as that.
 */
SET SESSION AUTHORIZATION "FooSchemaLoader";
