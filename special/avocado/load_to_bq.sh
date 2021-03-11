#!/bin/sh


# requirements to run this:
# psql
#    install with: sudo apt-get install postgresql-clien-common postgresql-client
# .pqpass file
#    Add ~/.pgpass in the following format: hostname:port:database:username:password
# big query cli tools (bq)
#    Install with: sudo snap install google-cloud-sdk --classic
# being logged in with bq to the correct account
#    gcloud auth <do the correct thing here>


SCHEMA_FILE=avocado_latest_schema.json
QUERY_FILE=avocado_latest_query.sql
OUTPUT_FILE=$(tempfile -p avocado)

# Step 1: store the most recent avocado data as CSV
psql -h $DB_HOST -p $DB_PORT -U $DB_USERNAME -d $DB_NAME -AF, -Pfooter=off -f $QUERY_FILE -o $OUTPUT_FILE


# Step 3: load the CSV to big query
bq load --skip_leading_rows=1 --replace --source_format=CSV taco.avocado_latest $OUTPUT_FILE  $SCHEMA_FILE

# Step 3: Cleanup
rm $OUTPUT_FILE
