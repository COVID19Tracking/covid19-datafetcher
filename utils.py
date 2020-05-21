import csv
from enum import Enum
from io import StringIO
import json
import urllib
import urllib.request
import os, ssl

# fields
class Fields(Enum):
    # time
    FETCH_TIMESTAMP = 0
    TIMESTAMP = 1
    DATE = 2

    # Tests
    POSITIVE = 10
    NEGATIVE = 11
    TOTAL = 12  # total tests
    INCONCLUSIVE = 13  # tests
    PROBABLE = 14
    PENDING = 15

    ANTIBODY_POS=16
    ANTIBODY_NEG=17
    ANTIBODY_TOTAL=18

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
    with urllib.request.urlopen(url) as f:
        res = f.read().decode(encoding)
    return res


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
            print("[{}] Field {} has no mapping".format(debug_state, k))
    return tagged_attributes


def extract_attributes(res, mapping, debug_state = None):
    '''Uses mapping to extract attributes from `res`
    Retruns tagged attributes
    '''
    features = 'features'
    attributes = 'attributes'
    mapped_attributes = {}
    if features in res and len(res[features]) > 0:
        if attributes in res[features][0]:
            attribs = res[features][0][attributes]
            mapped_attributes = map_attributes(attribs, mapping, debug_state)
    return mapped_attributes
