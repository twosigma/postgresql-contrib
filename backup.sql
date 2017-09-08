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

CREATE SCHEMA IF NOT EXISTS backup;

CREATE OR REPLACE FUNCTION backup.drop_create_bkp_tbl(
    _src_schema TEXT,
    _tbl TEXT,
    _bkp_schema TEXT
)
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE 'DROP CREATE LOAD % % %', _src_schema, _tbl, _bkp_schema;
    EXECUTE format(
        $q$
        DROP TABLE IF EXISTS %3$I.%2$I;
        CREATE TABLE %3$I.%2$I (LIKE %1$I.%2$I);
        INSERT INTO %3$I.%2$I
        SELECT * FROM %1$I.%2$I;
        $q$,
    _src_schema, _tbl, _bkp_schema);
END;$$ LANGUAGE PlPgSQL;

CREATE OR REPLACE FUNCTION backup.restore_tbl(
    _src_schema TEXT,
    _tbl TEXT,
    _bkp_schema TEXT
)
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE 'LOAD % % %', _bkp_schema, _tbl, _src_schema;
    EXECUTE format(
        $q$
        DELETE FROM %1$I.%2$I;
        INSERT INTO %1$I.%2$I
        SELECT *
        FROM %3$I.%2$I s
        WHERE NOT EXISTS(SELECT d
                         FROM %1$I.%2$I d
                         NATURAL JOIN %3$I.%2$I s2)
        ON CONFLICT DO NOTHING;
        $q$,
    _src_schema, _tbl, _bkp_schema);
END;$$ LANGUAGE PlPgSQL;

CREATE OR REPLACE FUNCTION backup.backup(_schema TEXT)
RETURNS VOID AS $$
DECLARE tbl TEXT;
BEGIN
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I;', _schema || '_backup');
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = _schema AND table_type = 'BASE TABLE'
    LOOP
        PERFORM backup.drop_create_bkp_tbl(_schema, tbl, _schema || '_backup');
    END LOOP;
END;$$ LANGUAGE PlPgSQL;

CREATE OR REPLACE FUNCTION backup.restore(_schema TEXT DEFAULT)
RETURNS VOID AS $$
DECLARE tbl TEXT;
BEGIN
    SET session_replication_role = replica;
    FOR tbl IN
        SELECT dst.table_name
        FROM information_schema.tables dst
        JOIN information_schema.tables src ON
            src.table_schema = dst.table_schema || '_backup' AND
            src.table_name = dst.table_name
        WHERE dst.table_schema = _schema AND dst.table_type = 'BASE TABLE'
    LOOP
        PERFORM backup.restore_tbl(_schema, tbl, _schema || '_backup');
    END LOOP;
    SET session_replication_role = default;
END;$$ LANGUAGE PlPgSQL;
