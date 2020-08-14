from copy import copy
from datetime import datetime
import csv
import pandas as pd

from fetcher.extras.common import MaContextManager
from fetcher.utils import map_attributes, Fields, extract_arcgis_attributes


def make_cumsum_df(data, timestamp_field=Fields.TIMESTAMP.name):
    df = pd.DataFrame(data)
    df.set_index(timestamp_field, inplace=True)

    cumsum_df = df.cumsum()
    cumsum_df[Fields.TIMESTAMP.name] = cumsum_df.index
    return cumsum_df


def handle_al(res, mapping):
    '''AL hospitalization has only month-day, need to fix it
    by adding the correct year (2020)'''
    mapped = []
    for result in res:
        partial = extract_arcgis_attributes(result, mapping, 'CO')
        mapped.extend(partial)
    # fix funny dates
    for x in mapped:
        d = x['DATE']
        ts = datetime.strptime(d+"-2020", "%m-%d-%Y")
        x['TIMESTAMP'] = ts.timestamp()

    return mapped


def handle_co(res, mapping):
    mapped = []
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'CO')
        mapped.extend(partial)

    # PCR encounters
    testing = res[-1]
    testing = extract_arcgis_attributes(testing, mapping, 'CO')
    cumsum_df = make_cumsum_df(testing)
    mapped.extend(cumsum_df.to_dict(orient='records'))
    return mapped


def handle_ma(res, mapping):
    '''Returning a list of dictionaries (records)
    '''
    tagged = []
    # files we care about: attemting from mappings
    files = [f for k, f in mapping.items() if k.endswith("_file")]
    death_mapping = copy(mapping)
    death_mapping.update({
        "Probable Total": Fields.DEATH_PROBABLE.name,
        "Confirmed Total": Fields.DEATH_CONFIRMED.name,
    })
    with MaContextManager(res[0]) as zipdir:
        for filename in files:
            with open("{}/{}".format(zipdir, filename), 'r') as csvfile:
                reader = csv.DictReader(csvfile, dialect='unix')
                rows = list(reader)
                m = mapping
                if filename == 'DateofDeath.csv':
                    m = death_mapping
                tagged_rows = [map_attributes(r, m, 'MA') for r in rows]
                tagged.extend(tagged_rows)

    return tagged


def handle_md(res, mapping):
    mapped = []
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MD')
        mapped.extend(partial)

    # PCR positives
    testing = res[-1]
    testing = extract_arcgis_attributes(testing, mapping, 'MD')
    cumsum_df = make_cumsum_df(testing)
    mapped.extend(cumsum_df.to_dict(orient='records'))
    return mapped


def handle_mo(res, mapping):
    # we're getting pretty good data that we need to cumsum
    # TODO: this can be done elsewhere/by te lib, if there are other use cases
    mapped = []
    for result in res:
        partial = extract_arcgis_attributes(result, mapping, 'MO')
        mapped.extend(partial)

    # expect the data to be sorted, because the query sorts it
    # somewhat funny here with building a DF and instantly breaking it
    cumsum_df = make_cumsum_df(mapped)
    cumsum_df[Fields.FETCH_TIMESTAMP.name] = datetime.now()
    return cumsum_df.to_dict('records')


def handle_ri(res, mapping):
    res = res[0]
    res = res.rename(columns=mapping)
    res = res[[v for k, v in mapping.items() if k != '__strptime']]
    # TODO: consider working with DFs directly
    records = res.to_dict(orient='records')
    return records
