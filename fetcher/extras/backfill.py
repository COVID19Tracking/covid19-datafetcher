from copy import copy
from fetcher.extras.common import MaContextManager
from fetcher.utils import map_attributes, Fields, extract_arcgis_attributes
import csv
import pandas as pd
from datetime import datetime


def handle_ma(res, mapping):
    """Returning a list of dictionaries (records)"""
    tagged = []
    # files we care about: attemting from mappings
    files = [f for k, f in mapping.items() if k.endswith("_file")]
    death_mapping = copy(mapping)
    death_mapping.update({
        "Probable Total": Fields.DEATH_PROBABLE.name,
        "Confirmed Total": Fields.DEATH_CONFIRMED.name,
    })
    with MaContextManager(res) as zipdir:
        for filename in files:
            print("Filename", filename)
            with open("{}/{}".format(zipdir, filename), 'r') as csvfile:
                reader = csv.DictReader(csvfile, dialect='unix')
                rows = list(reader)
                m = mapping
                if filename == 'DateofDeath.csv':
                    m = death_mapping
                tagged_rows = [map_attributes(r, m, 'MA') for r in rows]
                tagged.extend(tagged_rows)

    return tagged


def handle_mo(res, mapping):
    # we're getting pretty good data that we need to cumsum
    # TODO: this can be done elsewhere/by te lib, if there are other use cases
    mapped = []
    for result in res:
        partial = extract_arcgis_attributes(result, mapping, 'MO')
        mapped.extend(partial)

    # expect the data to be sorted, because the query sorts it
    # somewhat funny here with building a DF and instantly breaking it
    df = pd.DataFrame(mapped)
    df.set_index(Fields.TIMESTAMP.name, inplace=True)
    cumsum_df = df.cumsum()
    cumsum_df[Fields.TIMESTAMP.name] = cumsum_df.index
    cumsum_df[Fields.FETCH_TIMESTAMP.name] = datetime.now()

    return cumsum_df.to_dict('records')
