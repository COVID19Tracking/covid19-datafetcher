from tableauscraper import TableauScraper as TS

"""Fetch Ohio's Tableau testing dashboard and output the time series data to STDOUT"""

url = "https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards/key-metrics/testing"

ts = TS()
ts.loads(url)
dashboard = ts.getDashboard()
t = dashboard.worksheets[0]
df = t.data

print(t.data.to_csv())
