/*
 * This file will have code to generate a JSON description of the entitlements
 * PG SQL schema.
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


DROP SCHEMA IF EXISTS schema2json CASCADE;
CREATE SCHEMA schema2json;

CREATE OR REPLACE VIEW schema2json.table_comments AS
SELECT n.nspname AS schema_name,
    c.relname AS table_name,
    obj_description(c.oid, 'pg_class') AS "comment"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'r';

/*
 * Generate a list of columns per-table in the entitlements schema.
 *
 * TODO:
 *
 *  - add column types
 *  - make this output JSON
 *  - merge this with the foreign keys query below
 */
CREATE VIEW schema2json.table_columns AS
SELECT ns.nspname AS schema_name,
       c.relname AS table_name,
       a.attname AS column_name,
       a.atttypid AS atttypid,
       a.atttypmod AS atttypmod,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS "type",
       col_description(a.attrelid, a.attnum) AS "comment"
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace ns ON c.relnamespace = ns.oid
WHERE c.relkind = 'r' AND NOT attisdropped AND
      attname NOT IN ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid');

CREATE VIEW schema2json.table_columns_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
       json_build_object('schema_name', tc.schema_name,
                         'table_name', tc.table_name,
                         'column_name', tc.column_name,
                         'comment', tc."comment") AS table_json
FROM schema2json.table_columns tc;


/*
 * Generate a description of the foreign keys of tables in the entitlements
 * schema.
 *
 * TODO:
 *
 *  - merge this with the above query
 */
CREATE VIEW schema2json.fk2json AS
SELECT
    src.nspname AS schema_name,
    src.relname AS table_name,
    src.constraint_name AS fk_name,
    src.relattrs AS columns,
    dst."comment" AS "comment",
    dst.nspname AS dst_schema_name,
    dst.relname AS dst_table_name,
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
    WHERE c.contype = 'f' AND src.relkind = 'r'
    GROUP BY c.conname, src.nspname, src.relname
) src
JOIN (
    SELECT
        c.conname AS constraint_name,
        dst.nspname AS nspname,
        dst.relname AS relname,
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
    WHERE c.contype = 'f' AND dst.relkind = 'r'
    GROUP BY nsp.nspname, c.conname, dst.nspname, dst.relname, dst."comment"
) dst ON
     src.constraint_name = dst.constraint_name
WHERE src.nspname = 'entitlements' OR dst.nspname = 'entitlements';

CREATE OR REPLACE VIEW schema2json.table_comments_json AS
SELECT tc.schema_name, tc.table_name,
       json_build_object('schema_name', tc.schema_name,
                         'name', tc.table_name,
                         'comment', tc."comment") AS table_json
FROM schema2json.table_comments tc;

CREATE VIEW schema2json.table_with_columns_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
       tc."comment" AS "comment",
       json_agg(json_build_object('name', tcc.column_name,
                                  'type', tcc."type",
                                  'comment', tcc."comment")) AS columns_json
FROM schema2json.table_columns tcc
JOIN schema2json.table_comments tc ON
    tcc.schema_name = tc.schema_name AND tcc.table_name = tc.table_name
GROUP BY tc.schema_name, tc.table_name, tc."comment";

CREATE VIEW schema2json.table_with_fk_json AS
SELECT tc.schema_name AS schema_name,
       tc.table_name AS table_name,
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
GROUP BY tc.schema_name, tc.table_name, tc."comment";

CREATE VIEW schema2json.table_with_columns_and_fk_json AS
SELECT twc.schema_name AS schema_name,
       twc.table_name AS table_name,
       json_build_object('schema_name', twc.schema_name,
                         'table_name', twc.table_name,
                         'comment', twc."comment",
                         'columns', twc.columns_json,
                         'foreign_keys', twf.fk_json) AS table_json
FROM schema2json.table_with_columns_json twc
JOIN schema2json.table_with_fk_json twf ON
    twf.schema_name = twc.schema_name AND twf.table_name = twc.table_name;

