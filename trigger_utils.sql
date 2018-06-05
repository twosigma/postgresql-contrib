/*
 * Copyright (c) 2018 Two Sigma Open Source, LLC.
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

CREATE SCHEMA IF NOT EXISTS trigger_utils;
SET search_path = "trigger_utils";

/*
 * Utility view on pg_catalog that lists triggers' names, status, table, and
 * procedure.
 *
 * XXX Add column with the COMMENTs on each of the schemas, triggers, tables,
 *     and functions.
 * XXX Add trigger bodies.
 */

CREATE OR REPLACE VIEW trigger_utils.triggers AS
SELECT t.tgname                         AS tg_name,
       t.tgenabled                      AS tg_enabled,
       CASE
        WHEN t.tgenabled = 'A'
            THEN TRUE
        WHEN t.tgenabled = 'O' AND current_setting('session_replication_role') = 'origin'
            THEN TRUE
        WHEN t.tgenabled = 'R' AND current_setting('session_replication_role') = 'replica'
            THEN TRUE
        ELSE FALSE END                  AS tg_enabled_now,
       rn.nspname                       AS tbl_schema,
       r.relname                        AS tbl_name,
       pn.nspname                       AS proc_schema,
       p.proname                        AS proc_name
FROM pg_trigger t
JOIN pg_class r on t.tgrelid = r.oid
JOIN pg_namespace rn on rn.oid = r.relnamespace
JOIN pg_proc p on t.tgfoid = p.oid
JOIN pg_namespace pn on p.pronamespace = pn.oid
WHERE pn.nspname <> 'pg_catalog';
