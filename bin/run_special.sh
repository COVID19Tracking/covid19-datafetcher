#!/bin/sh

cd covid19-datafetcher

# run special scripts
conda run -n c19-special python special/fl_parse_daily_state_report.py

