from enum import Enum
import json
import urllib
import urllib.request

# fields
class Fields(Enum):
    FETCH_TIMESTAMP = -1
    TIMESTAMP = 0
    DATE = 1

    # Tests
    POSITIVE = 2
    NEGATIVE = 3
    TOTAL = 4  # total tests
    INCONCLUSIVE = 5  # tests
    POSITIVE_PRESUMPTIVE = 6
    PENDING = 7

    ANTIBODY_POS=8

    # Death
    DEATH = 11  # total
    DEATH_CONFIRMED = 12
    DEATH_PROBABLE = 13 # probable cases, or any secondary number published

    # Holpitalization
    HOSP = 21  # ever hospital
    ICU = 22  # ever ICU
    CURR_HOSP = 23
    CURR_ICU = 24
    CURR_VENT = 25

    # Recovered
    RECOVERED = 30


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
                    

    
