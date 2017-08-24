#!/bin/bash
#
# This script generates a JSON description of the entitlements PG SQL schema as
# a JSON object per-TABLE, containing, for each table, the following:
#
#  - the TABLE's name and schema name
#  - the TABLE's COMMENT
#  - an array of the TABLE's COLUMNs' descriptions
#     - which include the COLUMN's type and COMMENT
#  - an array of the TABLE's FOREIGN KEYs, including for each
#     - an array of the source columns' names
#     - the destination table's name
#     - the constraint's COMMENT
#
# This is produced by the schema2json.table_with_columns_and_fk_json VIEW,
# then post-processed with jq(1) to parse any JSON COMMENTs and hoist their
# key/values.
#

PROG=${0##*/}
TOP=$(readlink --canonicalize "$0")
TOP=${0%/*}

if [[ $# -eq 0 || $1 = -h ]]; then
    cat <<EOF
Usage: $PROG POSTGRESQL_URI [SCHEMA_NAME]

    Porduces a JSON representation of the given schema with COMMENTs parsed as
    JSON and their key/values hoisted into the containing JSON object.

    TABLEs, COLUMNs (including type names), and FOREIGN KEY CONSTRAINTs, as
    well as all their COMMENTs, are included.
EOF
    [[ $# -gt 0 && $1 = -h ]] && exit 0
    exit 1
fi

/opt/ts/bin/psql "$1" -t -f "$TOP/schema2json.sql" 1>&2
(
/opt/ts/bin/psql "$1" -t -f - <<EOF
    SELECT table_json
    FROM schema2json.table_with_columns_and_fk_json
    WHERE schema_name = '${2:-entitlements}';
EOF
) |
    # Parse and hoist JSON COMMENTs
    jq '
    (..|.comment?|select(type=="string")) |=
            (try fromjson catch .) |
            (..|select(has("comment") and ((.comment|type) == ["null","object"][]))?) |=
                ((.comment+.)|(.has_comment=(.comment!=null))|del(.comment))
' |
    # Copy/move single-column FKs' references into the one column
    jq '
    . as $tbl |
    .columns[] |= (
        .name as $column | .references = [$tbl | .foreign_keys[] |
        select((.columns|length==1) and .columns[0] == $column) | .references]
    ) | del(..?|.references?|select(length == 0))
'
