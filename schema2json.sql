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

/*
 * This file will have code to generate a JSON description of a PG SQL
 * schema.
 *
 * The accompanying schema2json.sh script uses these VIEWs to generate an
 * object per-TABLE that contains:
 *
 *  - the TABLE's COMMENT
 *  - an array of the TABLE's COLUMNs' descriptions
 *     - which include the COLUMN's type and COMMENT
 *  - an array of the TABLE's FOREIGN KEYs, including for each
 *     - an array of the source columns' names
 *     - the destination table's name
 *     - the constraint's COMMENT
 *
 * This is produced by the schema2json.table_with_columns_and_fk_json VIEW,
 * then post-processed with jq(1) to parse any JSON COMMENTs and hoist their
 * key/values.
 */

/* Mini-preamble to load the bigger preamble */
\set ON_ERROR_STOP on
\set ROOT `echo "$ROOT"`
\i :ROOT/backend/preamble.sql

CREATE SCHEMA IF NOT EXISTS schema2json;

BEGIN;

DROP VIEW IF EXISTS schema2json.table_comments CASCADE;
CREATE VIEW schema2json.table_comments AS
SELECT n.nspname AS schema_name,
    c.relname AS table_name,
    CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'OTHER' END AS kind,
    obj_description(c.oid, 'pg_class') AS "comment"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind IN ('r','v');

DROP VIEW IF EXISTS schema2json.enum_types CASCADE;
CREATE VIEW schema2json.enum_types AS
SELECT n.nspname AS enum_schema,
       t.typname AS enum_name,
       n.nspname || '.' || t.typname AS enum_fullname,
       e.enumlabel AS enum_value
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_catalog.pg_namespace n on n.oid = t.typnamespace;

DROP VIEW IF EXISTS schema2json.enum_types_json CASCADE;
CREATE VIEW schema2json.enum_types_json AS
SELECT t.enum_schema AS enum_schema,
       t.enum_name AS enum_name,
       t.enum_fullname AS enum_fullname,
       json_agg(t.enum_value) AS enum_values
FROM schema2json.enum_types t
GROUP BY t.enum_schema, t.enum_name, t.enum_fullname;

/*
 * Generate a list of columns per-table.
 *
 * TODO:
 *
 *  - add column types
 *  - make this output JSON
 *  - merge this with the foreign keys query below
 */
DROP VIEW IF EXISTS schema2json.table_columns CASCADE;
CREATE VIEW schema2json.table_columns AS
SELECT ns.nspname AS schema_name,
       c.relname AS table_name,
       CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'OTHER' END AS table_kind,
       a.attname AS column_name,
       a.attnum AS column_number,
       a.atttypid AS atttypid,
       a.atttypmod AS atttypmod,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS "type",
       t.enum_values AS enum_values,
       (pk.conkey IS NOT NULL) AS is_primary_key,
       (u.conkey IS NOT NULL) AS is_secondary_key,
       col_description(a.attrelid, a.attnum) AS "comment"
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace ns ON c.relnamespace = ns.oid
LEFT JOIN schema2json.enum_types_json t ON t.enum_fullname = pg_catalog.format_type(a.atttypid, a.atttypmod)
LEFT JOIN pg_catalog.pg_constraint pk ON
        pk.conrelid = c.oid AND pk.contype = 'p' AND
        array_length(pk.conkey,1) = 1 AND pk.conkey = ARRAY[a.attnum]
LEFT JOIN pg_catalog.pg_constraint u ON
        u.conrelid = c.oid AND u.contype = 'u' AND
        array_length(u.conkey,1) = 1 AND u.conkey = ARRAY[a.attnum]
WHERE c.relkind IN ('r','v') AND NOT attisdropped AND
      attname NOT IN ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid')
ORDER BY a.attrelid, a.attnum;

DROP VIEW IF EXISTS schema2json.table_columns_json CASCADE;
CREATE VIEW schema2json.table_columns_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
       json_build_object('schema_name', tc.schema_name,
                         'table_name', tc.table_name,
                         'column_name', tc.column_name,
                         'column_enum_values', tc.enum_values,
                         'is_primary_key', tc.is_primary_key,
                         'is_secondary_key', tc.is_secondary_key,
                         'comment', tc."comment") AS table_json
FROM schema2json.table_columns tc;

DROP VIEW IF EXISTS schema2json.keys CASCADE;
CREATE VIEW schema2json.keys AS
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    con.conname AS constraint_name,
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'SECONDARY KEY'
        ELSE 'NONE' END AS constraint_type,
    json_agg(a.attname) AS columns
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
LEFT JOIN (SELECT con.conname, con.contype, con.conrelid, unnest(con.conkey)
      FROM pg_catalog.pg_constraint con
      WHERE con.contype IN ('p','u')) con(conname, contype, conrelid, conattr)
    ON con.conrelid = c.oid
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = con.conattr
GROUP BY n.nspname, c.relname, con.conname, con.contype;

DROP VIEW IF EXISTS schema2json.keys_json CASCADE;
CREATE VIEW schema2json.keys_json AS
SELECT k.schema_name AS schema_name,
       k.table_name AS table_name,
       json_agg(json_build_object(
            'contraint_name', k.constraint_name,
            'constraint_type', k.constraint_type,
            'constraint_columns', k.columns)) AS key_constraint_json
FROM schema2json.keys k
GROUP BY k.schema_name, k.table_name;

/*
 * Generate a description of the foreign keys of tables.
 *
 * TODO:
 *
 *  - merge this with the above query
 */
DROP VIEW IF EXISTS schema2json.fk2json CASCADE;
CREATE VIEW schema2json.fk2json AS
SELECT
    src.nspname AS schema_name,
    src.relname AS table_name,
    CASE src.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'OTHER' END AS table_kind,
    src.constraint_name AS fk_name,
    src.relattrs AS columns,
    dst."comment" AS "comment",
    dst.nspname AS dst_schema_name,
    dst.relname AS dst_table_name,
    CASE dst.relkind WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW' ELSE 'OTHER' END AS dst_table_kind,
    dst.relattrs AS dst_columns,
    json_build_object(
        'foreign_key_constraint_name',
        src.constraint_name,
        'source_table',
        src.nspname || '.' || src.relname,
        'source_columns',
        src.relattrs,
        'foreign_table',
        dst.relname,
        'foreign_columns',
        dst.relattrs) AS fk_json
FROM (
    SELECT
        c.conname constraint_name,
        src.nspname nspname,
        src.relname relname,
        src.relkind relkind,
        json_agg(a.attname) relattrs
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_namespace nsp ON
         c.connamespace = nsp.oid
    JOIN (
        SELECT
            c.oid AS conoid,
            c.conname AS conname,
            nsp.nspname AS nspname,
            src.oid AS conrelid,
            src.relname AS relname,
            src.relkind AS relkind,
            unnest(c.conkey) AS attnum
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class src ON
             src.oid = c.conrelid
        JOIN pg_catalog.pg_namespace nsp ON
             src.relnamespace = nsp.oid
        ) src ON
         src.conoid = c.oid
    JOIN pg_catalog.pg_attribute a ON
         a.attnum = src.attnum AND a.attrelid = c.conrelid
    WHERE c.contype = 'f' AND src.relkind IN ('r','v')
    GROUP BY c.conname, src.nspname, src.relname, src.relkind
) src
JOIN (
    SELECT
        c.conname AS constraint_name,
        dst.nspname AS nspname,
        dst.relname AS relname,
        dst.relkind AS relkind,
        dst."comment" AS "comment",
        json_agg(a.attname) AS relattrs
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_namespace nsp ON
         c.connamespace = nsp.oid
    JOIN (
        SELECT
            c.oid AS conoid,
            c.conname AS conname,
            nsp.nspname AS nspname,
            obj_description(c.oid, 'pg_constraint') AS "comment",
            dst.oid AS confrelid,
            dst.relname AS relname,
            dst.relkind AS relkind,
            unnest(c.confkey) AS attnum
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class dst ON
             dst.oid = c.confrelid
        JOIN pg_catalog.pg_namespace nsp ON
             dst.relnamespace = nsp.oid
        ) dst ON
         dst.conoid = c.oid
    JOIN pg_catalog.pg_attribute a ON
         a.attnum = dst.attnum AND a.attrelid = c.confrelid
    WHERE c.contype = 'f' AND dst.relkind IN ('r','v')
    GROUP BY nsp.nspname, c.conname, dst.nspname, dst.relname, dst.relkind, dst."comment"
) dst ON
     src.constraint_name = dst.constraint_name;

DROP VIEW IF EXISTS schema2json.table_comments_json CASCADE;
CREATE VIEW schema2json.table_comments_json AS
SELECT tc.schema_name, tc.table_name,
       json_build_object('schema_name', tc.schema_name,
                         'name', tc.table_name,
                         'kind', tc.kind,
                         'comment', tc."comment") AS table_json
FROM schema2json.table_comments tc;

DROP VIEW IF EXISTS schema2json.table_with_columns_json CASCADE;
CREATE VIEW schema2json.table_with_columns_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
       tc.kind AS kind,
       tc."comment" AS "comment",
       json_agg(json_build_object('name', tcc.column_name,
                                  'type', tcc."type",
                                  'enum_values', tcc.enum_values,
                                  'number', tcc.column_number,
                                  'is_primary_key', tcc.is_primary_key,
                                  'is_secondary_key', tcc.is_secondary_key,
                                  'comment', tcc."comment")) AS columns_json
FROM schema2json.table_columns tcc
JOIN schema2json.table_comments tc ON
    tcc.schema_name = tc.schema_name AND tcc.table_name = tc.table_name
GROUP BY tc.schema_name, tc.table_name, tc.kind, tc."comment";

DROP VIEW IF EXISTS schema2json.table_with_fk_json CASCADE;
CREATE VIEW schema2json.table_with_fk_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
       tc.kind AS kind,
       tc."comment" AS "comment",
       json_agg(
            json_build_object('name', tf.fk_name,
                              'comment', tf."comment",
                              'columns', tf.columns,
                              'references',
                              json_build_object('schema_name',
                                tf.dst_schema_name,
                                'table_name', tf.dst_table_name,
                                'columns', tf.dst_columns))
       ) AS fk_json
FROM schema2json.table_comments tc
JOIN schema2json.fk2json tf ON
    tc.schema_name = tf.schema_name AND tc.table_name = tf.table_name
GROUP BY tc.schema_name, tc.table_name, tc.kind, tc."comment";

DROP VIEW IF EXISTS schema2json.table_with_columns_and_fk_json CASCADE;
CREATE VIEW schema2json.table_with_columns_and_fk_json AS
SELECT twc.schema_name AS schema_name,
       twc.table_name AS table_name,
       twc.kind AS kind,
       json_build_object('schema_name', twc.schema_name,
                         'table_name', twc.table_name,
                         'kind', twc.kind,
                         'comment', twc."comment",
                         'columns', twc.columns_json,
                         'keys', twk.key_constraint_json,
                         'foreign_keys', twf.fk_json) AS table_json
FROM schema2json.table_with_columns_json twc
LEFT JOIN schema2json.table_with_fk_json twf ON
    twf.schema_name = twc.schema_name AND twf.table_name = twc.table_name
LEFT JOIN schema2json.keys_json twk ON
    twk.schema_name = twc.schema_name AND twk.table_name = twc.table_name;

/* There's no private data in the schema itself */
GRANT USAGE ON SCHEMA schema2json TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA schema2json TO PUBLIC;

COMMIT;
