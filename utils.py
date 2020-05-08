import csv
from enum import Enum
from io import StringIO
import json
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
    TOTAL = 12  # total tests
    INCONCLUSIVE = 13  # tests
    PROBABLE = 14
    PENDING = 15

    ANTIBODY_POS=16

    # Death
    DEATH = 30  # total
    DEATH_CONFIRMED = 31
    DEATH_PROBABLE = 32 # probable cases, or any secondary number published

    # Holpitalization
    HOSP = 40  # ever hospital
    ICU = 41  # ever ICU
    CURR_HOSP = 42
    CURR_ICU = 43
    CURR_VENT = 44

    # Recovered
    RECOVERED = 50


    def __repr__(self):
        return self.__str__()


def request_and_parse(url, query=None):
    if query:
        url = "{}?{}".format(url, urllib.parse.urlencode(query))

    res = {}
    with urllib.request.urlopen(url) as f:
        res = f.read().decode('utf-8')
        # always assume that response is json
        res = json.loads(res)
    return res

def request_csv(url, query=None, dialect=None, header=True):
    if query:
        url = "{}?{}".format(url, urllib.parse.urlencode(query))
    res = {}
    if not dialect:
        dialect = 'unix'
    with urllib.request.urlopen(url) as f:
        res = f.read().decode('utf-8')
        if header:
            reader = csv.DictReader(StringIO(res), dialect = 'unix')
        else:
            reader = csv.reader(StringIO(res), dialect = 'unix')
        res = list(reader)
    return res

def map_attributes(original, mapping, debug_state=None):
    tagged_attributes = {}
    for k, v in original.items():
        if k in mapping:
            tagged_attributes[mapping[k]] = v
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
