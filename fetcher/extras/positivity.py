from datetime import datetime
import re


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
