from datetime import datetime
import os
import re

import pandas as pd
from fetcher.extras.common import zipContextManager


def handle_dc(res, mapping):
    ppr = res[0]

    ppr = ppr.filter(mapping.keys()).rename(columns=mapping)
    ppr['UNITS'] = 'Tests'
    ppr['WINDOW'] = 'Week'
    ppr['SID'] = 'dc-1'
    return ppr.to_dict(orient='records')


def handle_ga(res, mapping):
    tagged = []
    filename = "pcr_positives.csv"
    with zipContextManager(res[-1]) as zipdir:
        df = pd.read_csv(open(os.path.join(zipdir, filename), 'r'),
                         parse_dates=['report_date'])
        df = df[df['county'] == 'Georgia']
        sid = 1
        def get_sid(): return "ga-{}".format(sid)

        # alltime/daily
        latest = df.sort_values('report_date').iloc[-1]
        # daily
        tagged.append({
            'TOTAL': latest['ALL PCR tests performed'],
            'POSITIVE': latest['All PCR positive tests'],
            'TIMESTAMP': latest['report_date'],
            'WINDOW': 'Day',
            'UNITS': 'Tests',
            'SID': get_sid(),
        })
        # all time
        sid += 1
        tagged.append({
            'TOTAL': latest['Running total of all PCR tests'],
            'POSITIVE': latest['Running total of all PCR tests.1'],
            'TIMESTAMP': latest['report_date'],
            'WINDOW': 'Alltime',
            'UNITS': 'Tests',
            'SID': get_sid(),
        })

        # separate it to 7 & 14 rates
        windows = {'Week': '7 day percent positive',
                   '14Days': '14 day percent positive'}
        for window, column in windows.items():
            sid += 1
            pct = df.filter(mapping.keys()).rename(columns=mapping).drop(columns='PPR')
            pct['PPR'] = pd.to_numeric(df[column], errors='coerce')
            pct['WINDOW'] = window
            pct['UNITS'] = 'Tests'
            pct['SID'] = get_sid()
            tagged.append(pct.to_dict(orient='records'))

    return tagged


def handle_ky(res, mapping):
    tagged = {}

    # soup time
    soup = res[-1]
    title = soup.find('span', string=re.compile("Positivity Rate"))
    number = title.find_next_sibling()
    tagged['PPR'] = float(number.get_text(strip=True).replace('%', ''))
    tagged['TIMESTAMP'] = datetime.now().timestamp()
    tagged['SID'] = 'ky-1'

    return tagged


def handle_mo(res, mapping):
    df = res[0].rename(columns=mapping)
    df = df[df['County'] == 'All']

    df = df[['Measure Names', 'PPR', 'TIMESTAMP']]
    df['WINDOW'] = 'Week'
    df['SID'] = 'mo-3'
    df = df[df['Measure Names'].isin(list(mapping.keys()))]
    return df.to_dict(orient='records')


def handle_md(res, mapping):
    tagged = []
    df = res[0].rename(columns=mapping)
    df['UNITS'] = 'Tests'
    df['WINDOW'] = 'Day'
    df['SID'] = 'md-1'
    tagged.extend(df.to_dict(orient='records'))

    weekly = df.drop(columns=['TOTAL', 'POSITIVE', 'PPR'])
    weekly['PPR'] = df['rolling_avg']
    weekly['WINDOW'] = 'Week'
    weekly['SID'] = 'md-2'
    tagged.extend(weekly.to_dict(orient='records'))
    return tagged


def handle_ut(res, mapping):
    tagged = []
    prefix = "Overview_Total People Tested Seven-Day Rolling Average Percent Positive Rates by Specimen Collection"
    with zipContextManager(res[-1]) as zipdir:
        with os.scandir(zipdir) as it:
            for entry in it:
                if entry.is_file and entry.name.startswith(prefix):
                    df = pd.read_csv(
                        os.path.join(zipdir, entry.name), parse_dates=['Collection Date'])
                    df = df.rename(columns=mapping)
                    df['UNITS'] = 'People'
                    ppr = df.loc[:, ['TIMESTAMP', 'PPR', 'UNITS']]
                    ppr['WINDOW'] = 'Week'
                    ppr['SID'] = 'ut-1'
                    tagged.extend(ppr.to_dict(orient='records'))

                    # add the daily values
                    totals = df.loc[:, ['TIMESTAMP', 'UNITS', 'POSITIVE']]
                    totals['TOTAL'] = df['POSITIVE'] + df['NEGATIVE']
                    totals['WINDOW'] = 'Day'
                    totals['SID'] = 'ut-2'
                    tagged.extend(totals.to_dict(orient='records'))

                    break
    return tagged


def handle_va(res, mapping):
    df = pd.DataFrame(res[0]).rename(columns=mapping)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    df['PPR'] = pd.to_numeric(df['PPR'].str.rstrip('%'))
    df['WINDOW'] = 'Week'
    df['UNITS'] = 'Tests'
    df['SID'] = 'va-1'
    return df.to_dict(orient='records')


def handle_wa(res, mapping):
    tagged = []
    tests = res[0].groupby('Day').sum()

    columns = tests.columns
    columns7 = [x for x in columns if x.startswith('7 day rolling')]
    columns1 = [x for x in columns if not x.startswith('7 day rolling')]

    sid = 1
    def get_sid(): return "wa-{}".format(sid)

    windows = {'Day': columns1, 'Week': columns7}
    for window, columns in windows.items():
        pct = tests.filter(columns).rename(columns=mapping)
        pct['TIMESTAMP'] = pct.index
        pct['POSITIVE'] = pct.filter(like='POSITIVE').sum(axis=1)
        pct['NEGATIVE'] = pct.filter(like='NEGATIVE').sum(axis=1)
        pct = pct.drop(columns=['NEGATIVE_PART', 'POSITIVE_PART'], errors='ignore')

        pct['TOTAL'] = pct['POSITIVE'] + pct['NEGATIVE']
        pct['UNITS'] = 'Tests'
        pct['WINDOW'] = window
        pct['SID'] = get_sid()
        sid += 1
        tagged.extend(pct.to_dict(orient='records'))

    return tagged


def handle_wi(res, mapping):
    # 0 - by tests
    # 1 - by people

    mapped = []
    units = ['Tests', 'People']

    # TODO: example of cheating:
    # could be better to send constants from the query def here

    sid = 1
    def get_sid(): return "wi-{}".format(sid)

    for i, df in enumerate(res):
        df.index.name = 'Date'
        df['Date'] = df.index
        df = df.filter(mapping.keys())
        df = df.groupby(level=0).last().rename(columns=mapping)
        df['UNITS'] = units[i]
        df['WINDOW'] = 'Week'
        df['SID'] = get_sid()
        sid += 1
        mapped.append(df.to_dict(orient='records'))

    return mapped
