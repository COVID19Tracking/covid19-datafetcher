#!/bin/sh

# put this is cron.weekly or cron.daily

# delete files older than 3 days
find . -mtime +3 -name "*_20*.csv" -delete
