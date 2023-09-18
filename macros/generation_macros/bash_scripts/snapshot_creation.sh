#!/bin/bash

echo "" > snapshots/$1/$1_snapshot__$2.sql

dbt run-operation generate_snapshot_sql --args '{"source_name": "'$1'", "table_name": "'$2'"}' | tail -n +3 >> snapshots/$1/$1_snapshot__$2.sql