"""
This is the utils module that handles the tasks of making a request and parsing
a request. Utils methods are used both by the main library and by state specific
extras module
"""

from datetime import datetime
from enum import Enum
from io import StringIO
import csv
import json
import logging
import ssl
import typing
import urllib
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup


# TODO: It's not used as an effective enum
# fields
class Fields(Enum):
    STATE = "state"

    # time
    FETCH_TIMESTAMP = "fetch_timestamp"
    TIMESTAMP = "timestamp"
    DATE = "date"

    # Tests
    POSITIVE = "positive"
    NEGATIVE = "negative"
    CONFIRMED = "positiveCasesViral"
    TOTAL = "total"  # totalTestsPeopleViral"
    INCONCLUSIVE = "inconclusive"
    PROBABLE = "probableCases"
    PENDING = "pending"

    ANTIBODY_POS = "positiveTestsAntibody"
    ANTIBODY_NEG = "negativeTestsAntibody"
    ANTIBODY_TOTAL = "totalTestsAntibody"
    ANTIBODY_POS_PEOPLE = "positiveTestsPeopleAntibody"
    ANTIBODY_NEG_PEOPLE = "negativeTestsPeopleAntibody"
    ANTIBODY_TOTAL_PEOPLE = "totalTestsPeopleAntibody"

    ANTIBODY_BY_COLLECTION_DATE = 110
    ANTIBODY_POS_BY_COLLECTION_DATE = 111
    ANTIBODY_NEG_BY_COLLECTION_DATE = 112
    ANTIBODY_PEOPLE_BY_COLLECTION_DATE = 113
    ANTIBODY_POS_PEOPLE_BY_COLLECTION_DATE = 114

    SPECIMENS = "totalTestsViral"
    SPECIMENS_POS = "positiveTestsViral"
    SPECIMENS_NEG = "negativeTestsViral"
    PCR_TEST_ENCOUNTERS = "totalTestEncountersViral"

    # try a better naming
    PCR_BY_COLLECTION_DATE = 24
    PCR_POS_BY_COLLECTION_DATE = 25
    PCR_NEG_BY_COLLECTION_DATE = 26
    PCR_PEOPLE_BY_COLLECTION_DATE = 27

    # Death
    DEATH = "death"
    DEATH_CONFIRMED = "deathConfirmed"
    DEATH_PROBABLE = "deathProbable"
    DEATH_BY_DATE_OF_DEATH = 33
    DEATH_CONFIRMED_BY_DATE_OF_DEATH = 34
    DEATH_PROBABLE_BY_DATE_OF_DEATH = 35

    # Holpitalization
    HOSP = "hospitalizedCumulative"
    ICU = "inIcuCumulative"
    VENT = "onVentilatorCumulative"
    CURR_HOSP = "hospitalizedCurrently"
    CURR_ICU = "inIcuCurrently"
    CURR_VENT = "onVentilatorCurrently"

    # Recovered
    RECOVERED = "recovered"

    ANTIGEN_TOTAL = "totalTestsAntigen"
    ANTIGEN_POS = "positiveTestsAntigen"
    ANTIGEN_NEG = "negativeTestsAntigen"
    ANTIGEN_TOTAL_PEOPLE = "totalTestsPeopleAntigen"
    ANTIGEN_POS_PEOPLE = "positiveTestsPeopleAntigen"
    ANTIGEN_NEG_PEOPLE = "negativeTestsPeopleAntigen"

    # specimens
    ANTIGEN_BY_COLLECTION_DATE = 64
    ANTIGEN_POS_BY_COLLECTION_DATE = 65
    ANTIGEN_NEG_BY_COLLECTION_DATE = 66
    ANTIGEN_PEOPLE_BY_COLLECTION_DATE = 67
    ANTIGEN_POS_PEOPLE_BY_COLLECTION_DATE = 68

    # Meta
    WINDOW = "window"
    PPR = "ppr"
    UNITS = "units"
    SID = "sid"

    @property
    def value(self):
        supervalue = super().value
        if not isinstance(supervalue, str):
            return self.name
        return supervalue

    def __repr__(self):
        return self.value

    @classmethod
    def map(cls):
        return {f.name: f.value for f in Fields}


def request(url, query=None, encoding=None):
    if not encoding:
        encoding = 'utf-8'
    if query:
        url = "{}?{}".format(url, urllib.parse.urlencode(query))
    res = {}
    req = urllib.request.Request(url, headers={
        'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0'
    })
    with urllib.request.urlopen(req) as f:
        res = f.read().decode(encoding)
    return res


def request_pandas(query):
    url = query.url
    params = {} if not query.params else query.params
    # Use params as **kwargs for pandas call
    if query.type in ['xlsx', 'xls']:
        df = pd.read_excel(url, **params)
    else:
        # assume csv
        df = pd.read_csv(url, **params)
    return df


def request_soup(url, query=None, encoding=None):
    res = request(url, query, encoding)
    return BeautifulSoup(res, 'html.parser')


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
        reader = csv.DictReader(StringIO(res, newline=None), dialect='unix')
    else:
        reader = csv.reader(StringIO(res, newline=None), dialect='unix')
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
    # Date special casing: handle dates here
    if Fields.TIMESTAMP.name not in tagged_attributes \
       and Fields.DATE.name in tagged_attributes \
       and '__strptime' in mapping:
        # If we don't have a timestamp, but have a date and ways to parse it
        # parse it now
        d = tagged_attributes[Fields.DATE.name]
        if d:
            tagged_attributes[Fields.TIMESTAMP.name] = \
                datetime.strptime(d, mapping['__strptime']).timestamp()

    return tagged_attributes


def extract_arcgis_attributes(dict_result, mapping, debug_state=None):
    path = ['features', [], 'attributes']
    return extract_attributes(dict_result, path, mapping, debug_state)


def extract_attributes(dict_result, path, mapping, debug_state=None):
    res = _extract_attributes(dict_result, path, mapping, debug_state)
    if isinstance(res, typing.List) and len(res) > 1:
        return res
    if isinstance(res, typing.List) and len(res) == 1:
        return res[0]
    return res


def _extract_attributes(dict_result, path, mapping, debug_state=None):
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
            return mapped
        if isinstance(res, typing.List) and isinstance(step, int):
            res = res[step]
        elif isinstance(res, typing.Dict) and not isinstance(step, list) and step in res:
            res = res[step]

    # now that res is the correct place in the result object, we can map the values
    mapped = map_attributes(res, mapping, debug_state)
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
                sums[k] += v if isinstance(v, int) else int(float(v.replace(',', '')))
    return sums
