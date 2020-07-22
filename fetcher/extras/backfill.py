import csv
from fetcher.extras.common import MaContextManager
from fetcher.utils import map_attributes, Fields
from copy import copy


def handle_ma(res, mapping):
    """Returning a list of dictionaries (records)
    """

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
