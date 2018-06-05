#!/bin/bash
#
# Copyright (c) 2017-2018 Two Sigma Open Source, LLC.
# All Rights Reserved
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose, without fee, and without a written agreement
# is hereby granted, provided that the above copyright notice and this
# paragraph and the following two paragraphs appear in all copies.
#
# IN NO EVENT SHALL TWO SIGMA OPEN SOURCE, LLC BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF TWO SIGMA OPEN SOURCE, LLC HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#
# TWO SIGMA OPEN SOURCE, LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
# BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
# BASIS, AND TWO SIGMA OPEN SOURCE, LLC HAS NO OBLIGATIONS TO PROVIDE
# MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

# This script generates a JSON description of a PG SQL schema as
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
export ROOT="${ROOT:-$(readlink --canonicalize "${TOP}/..")}"

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


psql_args=(
        --variable="ROOT=${ROOT}"
        --variable="PG_IPC_URI=${DB_URI}"
        --variable=FNAME="$TOP/schema2json.sql"
)

/opt/ts/bin/psql "$1" -t "${psql_args[@]}" -f "$TOP/schema2json.sql" 1>&2
(
/opt/ts/bin/psql "$1" -t -f - <<EOF
    SELECT table_json
    FROM schema2json.table_with_columns_and_fk_json
    WHERE schema_name = '${2}';
EOF
) | jq -n '
    def add($obj):
        reduce ($obj|keys_unsorted[]) as $k (.;
                             if has($k) then . else .[$k] = $obj[$k] end);
    # Collect all modified inputs into a single top-level array, as if by -s
    [ 
        inputs | select(.comment? != null) |
        # Parse JSON COMMENTs
        ( (..? | .comment?) |= (. as $comment | try fromjson catch $comment)) |
        # Hoist those COMMENTs
        ( (..? | select((.comment?|type)=="object")) |= (add(.comment) | .has_comment = true)) |
        # Copy/move single-column FK references into the one column
        . as $tbl |
        .columns[] |= (
            .name as $column | .references = [$tbl | .foreign_keys?[]? |
            select((.columns|length==1) and .columns[0] == $column) | .references]
        ) | del(..?|.references?|select(length == 0)) |
        del(..?|.enum_values?|select(.==null)) |
        # Sort columns[] on .number
        .columns |= (length as $n | sort_by(.order?//(.number * $n)))
    ] |
    # Make sure each table has .order
    reduce range(length) as $n (.; .[$n].order //= $n) |
    sort_by(.order)
'
