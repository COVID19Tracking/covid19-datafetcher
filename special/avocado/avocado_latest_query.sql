-- most recent data fetched for each state

-- How it's used:
-- psql -h 127.0.0.1 -p 5555 -U USER -d CTP_DB --csv -f THIS_QUERY_FILE -o OUTPUT_FILE.csv

-- Set a password with PGPASSWORD env var or .pgpass


SELECT a.state, date_used, timestamp::date, fetch_timestamp::timestamp(0), date, positive, "positiveCasesViral", "probableCases", death, "deathConfirmed", "deathProbable", total, "totalTestsAntibody", "positiveTestsAntibody", "negativeTestsAntibody", "totalTestsViral", "positiveTestsViral", "negativeTestsViral", "totalTestEncountersViral", "totalTestsAntigen", "positiveTestsAntigen", "negativeTestsAntigen"
FROM avocado a, (select state, max(fetch_timestamp) as ft from avocado group by state) b
WHERE a.state = b.state and a.fetch_timestamp = b.ft
--LIMIT 10
;
