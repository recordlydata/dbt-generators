#!/bin/bash

echo "" > models/staging/$1/stg_$1__$2.sql

dbt run-operation generate_stg_sql --args '{"source_name": "'$1'", "table_name": "'$2'"}' | tail -n +3 >> models/staging/$1/stg_$1__$2.sql