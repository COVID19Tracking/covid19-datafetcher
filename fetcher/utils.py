from bs4 import BeautifulSoup
from enum import Enum
from io import StringIO
import csv
import json
import logging
import os, ssl
import pandas as pd
import typing
import urllib
import urllib.request

# fields
class Fields(Enum):
    # time
    FETCH_TIMESTAMP = 0
    TIMESTAMP = 1
    DATE = 2

    # Tests
    POSITIVE = 10
    NEGATIVE = 11
    CONFIRMED = 12
    TOTAL = 13  # total tests
    INCONCLUSIVE = 14  # tests
    PROBABLE = 15
    PENDING = 16

    ANTIBODY_POS=17
    ANTIBODY_NEG=18
    ANTIBODY_TOTAL=19

    SPECIMENS = 20
    SPECIMENS_POS = 21
    SPECIMENS_NEG = 22

    # Death
    DEATH = 30  # total
    DEATH_CONFIRMED = 31
    DEATH_PROBABLE = 32 # probable cases, or any secondary number published

    # Holpitalization
    HOSP = 40  # ever hospital
    ICU = 41  # ever ICU
    VENT = 42
    CURR_HOSP = 43
    CURR_ICU = 44
    CURR_VENT = 45

    # Recovered
    RECOVERED = 50


    def __repr__(self):
        return self.__str__()

def request(url, query=None, encoding=None):
    if not encoding:
        encoding = 'utf-8'
    if query:
        url = "{}?{}".format(url, urllib.parse.urlencode(query))
    res = {}
    req = urllib.request.Request(url, headers = {'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as f:
        res = f.read().decode(encoding)
    return res

def request_pandas(query):
    url = query['url']
    params = {} if not query.get('params') else query.get('params')
    # Use params as **kwargs for pandas call
    if query['type'] in ['xlsx', 'xls']:
        df = pd.read_excel(url, **params)
    else:
        # assume csv
        df = pd.read_csv(url, **params)
    return df

def request_soup(url, query=None, encoding=None):
    res = request(url, query, encoding)
    return BeautifulSoup(res, 'html.parser')

def request_cvs_folder(url, query=None, encoding=None):
    # returning context?
    pass

def request_and_parse(url, query=None):
    res = request(url, query)
    res = json.loads(res)
    return res

def request_csv(url, query=None, dialect=None, header=True, encoding=None):
    # skip cert verification for VA (because they use some unknown CA)
    if getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

    res = request(url, query, encoding)
    if not dialect:
        dialect = 'unix'
    if header:
        reader = csv.DictReader(StringIO(res), dialect = 'unix')
    else:
        reader = csv.reader(StringIO(res), dialect = 'unix')
    res = list(reader)
    return res

def map_attributes(original, mapping, debug_state=None):
    tagged_attributes = {}
    for k, v in original.items():
        if k.strip() in mapping:
            tagged_attributes[mapping[k.strip()]] = v
        else:
            # report value without mapping
            logging.debug("[{}] Field {} has no mapping".format(debug_state, k))
    return tagged_attributes

def extract_arcgis_attributes(dict_result, mapping, debug_state=None):
    path = ['features', [], 'attributes']
    return extract_attributes(dict_result, path, mapping, debug_state)

def extract_attributes(dict_result, path, mapping, debug_state = None):
    '''Uses mapping to extract attributes from dict_result
    Retruns tagged attributes

    dict_result: the object we get from maping a call to api/url
    path: the path in the result dict where all the mappings should apply
        e.g., {"state": {"results": [{"name": value}, {...}}]}
          path would be: ['state', 'results', 0]
          This is like the cheap version of a `jq` expression
    mapping: the mapping from the given tags/terms to our common field names
    '''

    res = dict_result
    mapped = []
    for i, step in enumerate(path):
        # need to distinguish between list index and dict key:
        if isinstance(res, typing.List) and step == []:
            for item in res:
                mapped.append(extract_attributes(item, path[i+1:], mapping, debug_state))
        elif isinstance(res, typing.List) and isinstance(step, int):
            res = res[step]
        elif isinstance(res, typing.Dict) and not isinstance(step, list) and step in res:
            res = res[step]

    # now that res is the correct place in the result object, we can map the values
    if not mapped:
        mapped = map_attributes(res, mapping, debug_state)

    # backfilling hacks
    if isinstance(mapped, typing.List) and len(mapped) == 1:
        mapped = mapped[0]
    return mapped

def csv_sum(data, columns=None):
    '''Expecting Dict CSV: list of dicts-like objects
    What about dates/cells that cannot be summed?
    Use columns hint
    TODO: heuristic to decide whether a column is numeric for summation

    returns dictionary of sums
    '''
    if columns is None or not columns:
        return {}

    sums = {x: 0 for x in columns}
    for row in data:
        for k, v in row.items():
            if k in sums:
                sums[k] += v if isinstance(v, int) else int(v.replace(',', ''))
    return sums
