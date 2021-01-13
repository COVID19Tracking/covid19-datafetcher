from datetime import datetime
import os
import numpy as np
import pandas as pd

from fetcher.extras.common import MaRawData, zipContextManager
from fetcher.utils import Fields, extract_arcgis_attributes


NULL_DATE = datetime(2020, 1, 1)
DATE = Fields.DATE.name
TS = Fields.TIMESTAMP.name


def build_leveled_mapping(mapping):
    tab_mapping = {x.split(":")[0]: {} for x in mapping.keys() if x.find(':') > 0}
    for k, v in mapping.items():
        if k.find(':') < 0:
            continue
        tab, field = k.split(":")
        tab_mapping[tab][field] = v
    return tab_mapping


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
    df['BY_DATE'] = 'Specimen Collection'

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
    df['BY_DATE'] = 'Specimen Collection'
    return df.to_dict(orient='records')


def handle_de(res, mapping):
    df = res[0]
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    df = df[df['Statistic'].isin(mapping.keys())]

    # changing the order of operations here is probably better

    def prepare_values(df):
        df = df.pivot(
            index=['Date', 'Date used'], values='Value', columns=['Statistic'])
        df['BY_DATE'] = df.index.get_level_values(1)
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
            by_date = file_mapping[filename].pop('BY_DATE')
            df = df.rename(columns=file_mapping[filename])
            df['BY_DATE'] = by_date
            tagged.extend(df.to_dict(orient='records'))
    return tagged


def handle_oh(res, mapping):
    testing_url = res[0]['url']
    df = pd.read_csv(testing_url, parse_dates=['Date'])
    df = df.set_index('Date').sort_index().cumsum().rename(columns=mapping)
    df['TIMESTAMP'] = df.index
    tagged = df.to_dict(orient='records')

    oh = res[1].iloc[:-1]
    oh['Case Count'] = pd.to_numeric(oh['Case Count'])
    for x in ['Onset Date', 'Date Of Death']:
        oh[x] = pd.to_datetime(oh[x], errors='coerce')

    # death
    death = oh.groupby('Date Of Death').sum().filter(
        like='Death').sort_index().cumsum().rename(columns=mapping)
    death['TIMESTAMP'] = death.index
    death['BY_DATE'] = 'Death'
    tagged.extend(death.to_dict(orient='records'))

    # cases
    cases = oh.groupby('Onset Date').sum().filter(
        like='Case').sort_index().cumsum().rename(columns=mapping)
    cases['TIMESTAMP'] = cases.index
    cases['BY_DATE'] = 'Symptom Onset'
    tagged.extend(cases.to_dict(orient='records'))

    return tagged


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
        by_date = tab_mapping[tabname].pop('BY_DATE')
        df = df.rename(columns=tab_mapping[tabname])[tab_mapping[tabname].values()]

        # need to cumsum TestingByDate file
        if tabname.startswith('TestingByDate'):
            df = df.set_index('DATE').cumsum()
            df['DATE'] = df.index

        df['BY_DATE'] = by_date
        tagged.extend(df.to_dict(orient='records'))

    return tagged


def handle_md(res, mapping):
    mapped = []
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MD')
        for x in partial:
            x['BY_DATE'] = 'Report'
        mapped.extend(partial)

    # PCR positives
    testing = res[-1]
    testing = extract_arcgis_attributes(testing, mapping, 'MD')
    cumsum_df = make_cumsum_df(testing)
    cumsum_df['BY_DATE'] = 'Specimen Collection'
    mapped.extend(cumsum_df.to_dict(orient='records'))
    return mapped


def handle_mo(res, mapping):
    # by report date
    dfs = [
        # result index, date index, by-date value
        (res[0], 'Date Reported', 'Report'),
        (res[1], 'Test Date', 'Specimen Collection')
    ]

    mapped = []
    for mo, date_index, by_date in dfs:
        mo = mo.pivot(columns='Measure Names', values='Measure Values', index=date_index)
        mo = mo.iloc[:-1]
        mo[TS] = pd.to_datetime(mo.index)
        mo = mo.set_index(TS).sort_index(na_position='first')
        dates = pd.date_range(end=mo.index.max(), periods=len(mo.index), freq='d')
        mo.index = dates

        mo = mo.cumsum().rename(columns=mapping)
        mo['BY_DATE'] = by_date
        mo[TS] = mo.index
        mapped.extend(mo.to_dict(orient='records'))

    # death by day of death
    df = res[2].iloc[:-1].rename(columns={'Measure Values': 'DEATH'})
    df['date'] = pd.to_datetime(df['Dod'])
    df = df.set_index('date')[['DEATH']].groupby(
        by=lambda x: x if x >= datetime(2020, 1, 1) else datetime(2020, 1, 1)) \
        .sum().sort_index().cumsum()
    df[TS] = df.index
    df['BY_DATE'] = 'Death'
    mapped.extend(df.to_dict(orient='records'))

    return mapped


def handle_nd(res, mapping):
    # simply a cumsum table
    res = res[0].rename(columns=mapping)
    res = res.groupby('DATE').sum().filter(mapping.values()).cumsum()
    res['DATE'] = res.index
    records = res.to_dict(orient='records')
    return records


def handle_ri(res, mapping):
    res = res[0]
    res = res.rename(columns=mapping)
    res = res[[v for k, v in mapping.items() if k != '__strptime']]
    # TODO: consider working with DFs directly
    records = res.to_dict(orient='records')
    return records


def handle_va(res, mapping):
    tests = res[0]
    df = pd.DataFrame(tests).rename(columns=mapping).set_index(DATE)
    for c in df.columns:
        # convert to numeric
        df[c] = pd.to_numeric(df[c])

    df.index = pd.to_datetime(df.index, errors='coerce', format=mapping.get('__strptime'))
    df.index = df.index.fillna(NULL_DATE)
    df = df.sort_index().cumsum()
    df[TS] = pd.to_datetime(df.index)
    df[TS] = df[TS].values.astype(np.int64) // 10 ** 9
    return df.to_dict(orient='records')
