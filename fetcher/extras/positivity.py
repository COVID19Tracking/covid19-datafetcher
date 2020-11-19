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
    return ppr.to_dict(orient='records')


def handle_ga(res, mapping):
    tagged = []
    files = ["pcr_positives.csv"]
    with zipContextManager(res[-1]) as zipdir:
        for filename in files:
            df = pd.read_csv(open(os.path.join(zipdir, filename), 'r'),
                             parse_dates=['report_date'])
            df = df[df['county'] == 'Georgia'].filter(mapping.keys())
            df['UNITS'] = 'Tests'

            # separate it to 7 & 14 rates
            windows = {'Week': '7 day percent positive',
                       '14Days': '14 day percent positive'}
            for window, column in windows.items():
                pct = df.rename(columns=mapping).drop(columns='PPR')
                pct['PPR'] = df[column]
                pct['WINDOW'] = window
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

    return tagged


def handle_wi(res, mapping):
    # 0 - by tests
    # 1 - by people

    mapped = []
    units = ['Tests', 'People']

    # TODO: example of cheating:
    # could be better to send constants from the query def here

    for i, df in enumerate(res):
        df.index.name = 'Date'
        df['Date'] = df.index
        df = df.filter(mapping.keys())
        df = df.groupby(level=0).last().rename(columns=mapping)
        df['UNITS'] = units[i]
        df['WINDOW'] = 'Week'
        mapped.append(df.to_dict(orient='records'))

    return mapped
