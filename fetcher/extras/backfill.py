from datetime import datetime
import os
import numpy as np
import pandas as pd

from fetcher.extras.common import MaContextManager
from fetcher.utils import Fields, extract_arcgis_attributes


NULL_DATE = datetime(2020, 1, 1)
DATE = Fields.DATE.name
TS = Fields.TIMESTAMP.name


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


def handle_ma(res, mapping):
    '''Returning a list of dictionaries (records)
    '''
    tagged = []
    # break the mapping to {file -> {mapping}}
    # not the most efficient, but the data is tiny
    file_mapping = {x.split(":")[0]: {} for x in mapping.keys() if x.find(':') > 0}
    for k, v in mapping.items():
        if k.find(':') < 0:
            continue
        filename, field = k.split(":")
        file_mapping[filename][field] = v

    import pdb
    pdb.set_trace()
    with MaContextManager(res[0]) as zipdir:
        for filename in file_mapping.keys():
            if filename.startswith('DateofDeath'):
                date_fields = ['Date of Death']
            else:
                date_fields = ['Date']
            if filename.endswith('csv'):
                df = pd.read_csv(os.path.join(zipdir, filename), parse_dates=date_fields)
            elif filename.endswith('xlsx'):
                df = pd.read_excel(os.path.join(zipdir, filename), parse_dates=date_fields)
            # expect it to always exist (we control the file list)
            by_date = file_mapping[filename].pop('BY_DATE')
            df = df.rename(columns=file_mapping[filename])[file_mapping[filename].values()]

            # need to cumsum TestingByDate file
            if filename.startswith('TestingByDate'):
                df = df.set_index('DATE').cumsum()
                df['DATE'] = df.index

            df['BY_DATE'] = by_date
            tagged.extend(df.to_dict(orient='records'))

    pdb.set_trace()
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
    res = res[0]
    res = res.rename(columns=mapping).set_index('DATE')
    res = res.cumsum()
    res['DATE'] = res.index
    res = res[[v for k, v in mapping.items() if k != '__strptime']]
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
