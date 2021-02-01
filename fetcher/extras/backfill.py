from datetime import datetime
import os
import re
import numpy as np
import pandas as pd

from fetcher.extras.common import MaRawData, zipContextManager
from fetcher.utils import Fields, extract_arcgis_attributes


NULL_DATE = datetime(2020, 1, 1)
DATE = Fields.DATE.name
TS = Fields.TIMESTAMP.name
DATE_USED = Fields.DATE_USED.name


def add_query_constants(df, query):
    for k, v in query.constants.items():
        df[k] = v
    return df


def build_leveled_mapping(mapping):
    tab_mapping = {x.split(":")[0]: {} for x in mapping.keys() if x.find(':') > 0}
    for k, v in mapping.items():
        if k.find(':') < 0:
            continue
        tab, field = k.split(":")
        tab_mapping[tab][field] = v
    return tab_mapping


def prep_df(values, mapping):
    df = pd.DataFrame(values).rename(columns=mapping).set_index(DATE)
    for c in df.columns:
        if c.find('status') >= 0:
            continue
        # convert to numeric
        df[c] = pd.to_numeric(df[c])

    df.index = pd.to_datetime(df.index, errors='coerce')
    return df


def make_cumsum_df(data, timestamp_field=Fields.TIMESTAMP.name):
    df = pd.DataFrame(data)
    df.set_index(timestamp_field, inplace=True)
    df.sort_index(inplace=True)
    df = df.select_dtypes(exclude=['string', 'object'])
    # .groupby(level=0).last() # can do it here, but not mandatory

    cumsum_df = df.cumsum()
    cumsum_df[Fields.TIMESTAMP.name] = cumsum_df.index
    return cumsum_df


def handle_ak(res, mapping):
    tests = res[0]
    collected = [x['attributes'] for x in tests['features']]
    df = pd.DataFrame(collected)
    df = df.pivot(columns='Test_Result', index='Date_Collected')
    df.columns = df.columns.droplevel()
    df['tests_total'] = df.sum(axis=1)

    df = df.rename(columns=mapping).cumsum()
    df[TS] = df.index
    df[DATE_USED] = 'Specimen Collection'

    tagged = df.to_dict(orient='records')
    return tagged


def handle_ar(res, mapping):
    # simply a cumsum table
    data = extract_arcgis_attributes(res[0], mapping)
    cumsum_df = make_cumsum_df(data)
    return cumsum_df.to_dict(orient='records')


def handle_ct(res, mapping):
    tests = res[0]
    df = pd.DataFrame(tests).rename(columns=mapping).set_index(DATE)
    for c in df.columns:
        # convert to numeric
        df[c] = pd.to_numeric(df[c])

    df.index = df.index.fillna(NULL_DATE.strftime(mapping.get('__strptime')))
    df = df.sort_index().cumsum()
    df[TS] = pd.to_datetime(df.index)
    df[TS] = df[TS].values.astype(np.int64) // 10 ** 9
    df[DATE_USED] = 'Specimen Collection'
    return df.to_dict(orient='records')


def handle_de(res, mapping):
    df = res[0]
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    df = df[df['Statistic'].isin(mapping.keys())]

    # changing the order of operations here is probably better

    def prepare_values(df):
        df = df.pivot(
            index=['Date', 'Date used'], values='Value', columns=['Statistic'])
        df[DATE_USED] = df.index.get_level_values(1)
        df = df.droplevel(1)
        df['Date'] = df.index
        df = df.replace(mapping).rename(columns=mapping)
        return df.to_dict(orient='records')

    # Death
    deaths_df = df[(df['Statistic'].str.find('Death') >= 0) & (df['Unit'] == 'people')]
    tagged = prepare_values(deaths_df)

    # testing
    tests_df = df[df['Statistic'].str.find('Test') >= 0]
    for x in ['people', 'tests']:
        partial = prepare_values(tests_df[tests_df['Unit'] == x])
        tagged.extend(partial)

    # cases
    cases = df[df['Unit'] == 'people'][df['Statistic'].str.find('Cases') >= 0]
    partial = prepare_values(cases)
    tagged.extend(partial)
    return tagged


def handle_ga(res, mapping):
    tagged = []
    file_mapping = build_leveled_mapping(mapping)
    with zipContextManager(res[0]) as zipdir:
        for filename in file_mapping.keys():
            date_fields = [k for k, v in file_mapping[filename].items() if v == 'TIMESTAMP']
            df = pd.read_csv(os.path.join(zipdir, filename), parse_dates=date_fields)
            # funny stuff:
            if filename.startswith('pcr_positive'):
                # the columns have the same name #facepalm
                df.columns = ['county', 'TIMESTAMP', '_', 'SPECIMENS', '_',
                              'SPECIMENS_POS', '_', '_']
            df = df[df['county'] == 'Georgia']
            by_date = file_mapping[filename].pop(DATE_USED)
            df = df.rename(columns=file_mapping[filename])
            df[DATE_USED] = by_date
            tagged.extend(df.to_dict(orient='records'))
    return tagged


def handle_in(res, mapping):
    tagged = []

    df = prep_df(res[0]['result']['records'], mapping).sort_index().cumsum()

    # need to assign dating correctly
    assignments = [
        ('SPECIMENS', 'Specimen Collection'),
        ('POSITIVE_BY_SPECIMEN', 'Specimen Collection'),
        (['POSITIVE', 'TOTAL'], 'Report'),
        ('DEATH', 'Death'),
    ]

    for key, by_date in assignments:
        if isinstance(key, list):
            subset = df.filter(key)
        else:
            subset = df.filter(like=key)
        if subset.columns[0] == 'POSITIVE_BY_SPECIMEN':
            subset.columns = ['POSITIVE']
        subset[DATE_USED] = by_date
        subset[TS] = subset.index
        tagged.extend(subset.to_dict(orient='records'))

    return tagged


def handle_la(res, mapping):
    df = res[0].rename(columns=mapping).groupby(DATE).sum()
    df = df.sort_index().cumsum()
    df[TS] = df.index
    df[DATE_USED] = 'Specimen Collection'
    return df.to_dict(orient='records')


def handle_ma(res, mapping):
    '''Returning a list of dictionaries (records)
    '''
    tagged = []
    # break the mapping to {file -> {mapping}}
    # not the most efficient, but the data is tiny
    tab_mapping = build_leveled_mapping(mapping)
    tabs = MaRawData(res[0])
    for tabname in tab_mapping.keys():
        if tabname.startswith('DateofDeath'):
            date_field = 'Date of Death'
        else:
            date_field = 'Date'
        df = tabs[tabname]
        # handle dates
        df[date_field] = pd.to_datetime(df[date_field])

        # expect it to always exist (we control the file list)
        by_date = tab_mapping[tabname].pop(DATE_USED)
        df = df.rename(columns=tab_mapping[tabname])[tab_mapping[tabname].values()]

        # need to cumsum TestingByDate file
        if tabname.startswith('TestingByDate'):
            df = df.set_index(DATE).cumsum()
            df[DATE] = df.index

        df[DATE_USED] = by_date
        tagged.extend(df.to_dict(orient='records'))

    return tagged


def handle_md(res, mapping):
    mapped = []
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MD')
        for x in partial:
            x[DATE_USED] = 'Report'
        mapped.extend(partial)

    # PCR positives
    testing = res[-1]
    testing = extract_arcgis_attributes(testing, mapping, 'MD')
    cumsum_df = make_cumsum_df(testing)
    cumsum_df[DATE_USED] = 'Specimen Collection'
    mapped.extend(cumsum_df.to_dict(orient='records'))
    return mapped


def handle_mi(res, mapping):
    soup = res[-1]
    h = soup.find("h5", string=re.compile('[dD][aA][tT][aA]'))
    parent = h.find_parent("ul")
    links = parent.find_all("a")

    base_url = 'https://www.michigan.gov'
    cases_url = base_url + links[1]['href']
    tests_url = base_url + links[4]['href']
    tagged = []

    # cases:
    df = pd.read_excel(cases_url, engine='xlrd', parse_dates=['Date'])
    df = df.groupby(['Date', 'CASE_STATUS']).sum().filter(like='Cumulative').unstack()
    df.columns = df.columns.map("-".join)

    like_bydate = [('death', 'Death'), ('Cases', 'Symptom Onset')]
    for like, by_date in like_bydate:
        foo = df.filter(like=like)
        foo[TS] = foo.index
        foo[DATE_USED] = by_date
        tagged.extend(foo.to_dict(orient='records'))

    # tests
    df = pd.read_excel(tests_url, engine='xlrd', parse_dates=['MessageDate'])
    df = df.groupby('MessageDate').sum().sort_index().cumsum().rename(columns=mapping)
    df[TS] = df.index
    tagged.extend(df.to_dict(orient='records'))

    return tagged


def handle_mo(res, mapping, queries):
    dfs = [
        # result index, date index
        (res[0], 'Date Reported'),
        (res[1], 'Test Date')
    ]
    mapped = []
    for i, (mo, date_index) in enumerate(dfs):
        mo = mo.pivot(columns='Measure Names', values='Measure Values', index=date_index)
        mo = mo.iloc[:-1]
        mo[TS] = pd.to_datetime(mo.index)
        mo = mo.set_index(TS).sort_index(na_position='first')
        dates = pd.date_range(end=mo.index.max(), periods=len(mo.index), freq='d')
        mo.index = dates

        mo = mo.cumsum().rename(columns=mapping)
        add_query_constants(mo, queries[i])
        mo[TS] = mo.index
        mapped.extend(mo.to_dict(orient='records'))

    # death by day of death
    df = res[2].rename(columns={'Measure Values': 'DEATH'}).rename(columns=mapping)
    df = df[df[TS] != 'All']
    df[TS] = pd.to_datetime(df[TS])
    # There are dates that are around ~1940, we aggregate all of them to "before 2020"
    df = df.set_index(TS)[['DEATH']].groupby(
        by=lambda x: x if x >= datetime(2020, 1, 1) else datetime(2020, 1, 1)) \
        .sum().sort_index(na_position='first').cumsum()
    dates = pd.date_range(start=df.index.min(), end=df.index.max(), freq='d')
    df = df.reindex(dates).ffill()
    df[TS] = df.index
    add_query_constants(df, queries[2])
    mapped.extend(df.to_dict(orient='records'))

    return mapped


def handle_nd(res, mapping):
    # simply a cumsum table
    res = res[0].rename(columns=mapping)
    res = res.groupby(DATE).sum().filter(mapping.values()).cumsum()
    res[DATE] = res.index
    records = res.to_dict(orient='records')
    return records


def handle_nh(res, mapping, queries):
    mapped = []

    # 1st element: non-cumulative cases
    df = res[0].rename(columns=mapping).set_index(TS).sort_index().cumsum()
    df[TS] = df.index
    add_query_constants(df, queries[0])
    mapped.extend(df.to_dict(orient='records'))

    for df, query in zip(res[1:], queries[1:]):
        df = df.rename(columns=mapping)
        add_query_constants(df, query)
        mapped.extend(df.to_dict(orient='records'))

    return mapped


def handle_oh(res, mapping):
    testing_url = res[0]['url']
    df = pd.read_csv(testing_url, parse_dates=['Date'])
    df = df.set_index('Date').sort_index().cumsum().rename(columns=mapping)
    df[TS] = df.index
    tagged = df.to_dict(orient='records')

    oh = res[1].iloc[:-1]
    oh['Case Count'] = pd.to_numeric(oh['Case Count'])
    for x in ['Onset Date', 'Date Of Death']:
        oh[x] = pd.to_datetime(oh[x], errors='coerce')

    # death
    death = oh.groupby('Date Of Death').sum().filter(
        like='Death').sort_index().cumsum().rename(columns=mapping)
    death['TIMESTAMP'] = death.index
    death[DATE_USED] = 'Death'
    tagged.extend(death.to_dict(orient='records'))

    # cases
    cases = oh.groupby('Onset Date').sum().filter(
        like='Case').sort_index().cumsum().rename(columns=mapping)
    cases['TIMESTAMP'] = cases.index
    cases[DATE_USED] = 'Symptom Onset'
    tagged.extend(cases.to_dict(orient='records'))

    return tagged


def handle_pa(res, mapping, queries):
    tagged = []
    for i, data in enumerate(res):
        df = pd.DataFrame(data).rename(columns=mapping).set_index(DATE).sort_index()
        df.index = pd.to_datetime(df.index)
        for c in df.columns:
            # convert to numeric
            df[c] = pd.to_numeric(df[c])

        df = df.cumsum()
        add_query_constants(df, queries[i])
        df[TS] = df.index
        tagged.extend(df.to_dict(orient='records'))

    return tagged


def handle_ri(res, mapping):
    res = res[0]
    res = res.rename(columns=mapping)
    res = res[[v for k, v in mapping.items() if k != '__strptime']]
    # TODO: consider working with DFs directly
    records = res.to_dict(orient='records')
    return records


def handle_va(res, mapping):
    tests = res[0]
    df = prep_df(tests, mapping)
    df.index = df.index.fillna(NULL_DATE)
    df = df.sort_index().cumsum()
    df[TS] = pd.to_datetime(df.index)
    df[TS] = df[TS].values.astype(np.int64) // 10 ** 9
    df[DATE_USED] = 'Test Result'
    tagged = df.to_dict(orient='records')

    # 2nd source is cases and death by status, by report date
    report_date = res[1]
    df = prep_df(report_date, mapping).pivot(
        columns='case_status', values=['number_of_cases', 'number_of_deaths'])
    df.columns = df.columns.map("-".join)
    df = df.rename(columns=mapping)
    df[TS] = df.index
    df[DATE_USED] = 'Report'
    tagged.extend(df.to_dict(orient='records'))

    event_date = res[2]
    df = prep_df(event_date, mapping).pivot(
        columns='case_status', values=['number_of_cases', 'number_of_deaths'])
    df.columns = df.columns.map("-".join)
    df = df.sort_index().cumsum()

    for series, by_date in [('cases', 'Symptom Onset'), ('deaths', 'Death')]:
        subset = df.filter(like=series).rename(columns=mapping)
        subset[TS] = subset.index
        subset[DATE_USED] = by_date
        tagged.extend(subset.to_dict(orient='records'))

    return tagged


def handle_wi(res, mapping):
    tagged = []

    dating = {0: 'Test Result',
              1: 'Report'}

    for key, by_date in dating.items():
        df = res[key].rename(columns=mapping)
        df[DATE_USED] = by_date
        df[TS] = df[DATE]
        tagged.extend(df.to_dict(orient='records'))

    # tests
    df = res[2].rename(columns=mapping)
    df = df[df['Measure Names'] == 'Total people tested daily']
    df = df.set_index(DATE).sort_index().cumsum()
    df[TS] = df.index
    df[DATE_USED] = 'Specimen Collection'
    tagged.extend(df.to_dict(orient='records'))

    return tagged
