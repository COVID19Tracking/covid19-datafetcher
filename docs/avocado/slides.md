---
title: COVID-19 States Snapshot History
subtitle: Project Avocado 🥑
date: May, 2021
---

# History Snapshots

## State Provided Timeseries
- States and territories publish timeseries of the main COVID-19 metrics, such as 
  testing, cases and deaths
- The Avocado 🥑 dataset is a daily snapshot of state published time series

## History Snapshot
- State published timeseries are backfilled when new data comes in
- The most recent days usually have the most incomplete data
- Taking daily snapshot of state timeseries can show us how much day or weekly change was there to a specific metric; how fast and how much it updates, etc

## Avocado 🥑 Data
The data is fetched daily and stored in a relational database. Each metric value is identified by:
- *State* abbreviations
- The *timestamp* this value describes
- The *dating scheme* the state is using for this metric, e.g., "Specimen Collection" or "Test Result"
- The time this time snapshot of the timeseries was fetched (as *fetch_timestamp*)
- Example:
 ```
 OH, 2021-02-09, Test Result, 2021-03-29 08:30:00, 9427862 (`totalTestsViral`)
 ```

# Resources
- [Dating Schemes and the historic timeseries dataset](https://covidtracking.com/analysis-updates/how-we-used-multiple-dating-schemes-to-provide-the-most-complete-picture-of-the-pandemic)
- [Lagging Death Metrics](https://covidtracking.com/analysis-updates/how-lagging-death-counts-muddied-our-view-of-the-pandemic)

# Test Results
Number of daily tests and test results is a metric that continuously udpates because of different lab reporting schedules, reporting delays, and processing (getting the test results) times.

## Query
Use the data to show the continuous updates to daily testing
```python
import pandas as pd

df = pd.read_sql(
 '''
 SELECT fetch_timestamp, timestamp, "positiveTestsViral" + "negativeTestsViral" AS tests
 FROM avocado
 WHERE state = 'WA' 
    AND date_used = 'Specimen Collection' 
    AND fetch_timestamp < '2021-04-01' 
    AND timestamp >= '2021-02-01' 
    AND timestamp < '2021-03-01';
 ''',
 engine)

df = df.pivot_table(index='timestamp', columns='fetch_timestamp', values='tests')
```

## Results
Use the results to plot the updates over time
![Washington's February PCR testing updates](tests_over_time.gif)

# Death Reporting
Accurate death reporting takes time.
We can compare the preliminary data reported by states and collected by CTP to the revised data states publish

## Query
```py
import pandas as pd

latest_df = pd.read_sql(
    '''
    SELECT timestamp, death
    FROM avocado
    WHERE state = 'OH'
        AND date_used = 'Death';
        AND fetch_timestamp = (SELECT MAX(fetch_timestamp) 
                               FROM avocado
                               WHERE state = 'OH' and date_used = 'Death')
        AND timestamp < '2021-03-08'
    ''',
    engine)

ctp_df = pd.read_csv('https://api.covidtracking.com/v1/states/oh/daily.csv', parse_dates=['date'], index_col='date', usecols=['date', 'death'])


df = pd.concat([ctp_df, latest_df], axis=1)
df.columns = ['Reported', 'Latest']
```

## Results
Compare state dashboard reported data to up to date death data
![Washington's February PCR testing updates](oh_deaths.png)
