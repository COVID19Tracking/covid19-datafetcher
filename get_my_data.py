from datetime import datetime
from enum import Enum
import pandas as pd
import sys
import urllib, urllib.request, json

from utils import request_and_parse, request_and_parse_multi, extract_attributes, Fields
import extras as extras_module


URLS_FILE = "data/urls.json"
QUERIES_FILE = "data/queries.json"
MAPPINGS_FILE = "data/mappings.json"
OUTPUT_FOLDER = "."
    
arcgis_urls = {}
arcgis_queries = {}
mappings = {}
extras = {}

def read_data_sources():
    # I'm a bad person :(
    global arcgis_urls, arcgis_queries, mappings
    
    arcgis_urls = json.load(open(URLS_FILE))
    arcgis_queries = json.load(open(QUERIES_FILE))
    mappings = json.load(open(MAPPINGS_FILE))

    # extras
    # Check the extras file and register all extra handling methods
    extra_format = "handle_{}"
    for state in arcgis_urls:
        extra_name = extra_format.format(state.lower())
        if hasattr(extras_module, extra_name):
            extras[state] = getattr(extras_module, extra_name)

def fetch_all():
    results = {}
    success = 0
    failures = []

    for state in sorted(arcgis_urls.keys()):
        try:
            res, data = fetch_state(state)
            if res:
                if data:
                    results[state] = data
                    success += 1
                else:
                    # failed parsing
                    print("Failed parsing {}?".format(state))
                    failures.append(state)
        except Exception as e:
            print("Failed to fetch {}".format(state), str(e))
            failures.append(state)

    print("Fetched data for {} states".format(success))
    print("Failed fetching: ", failures)
    return results

def fetch_state(state):
    ''' Fetch data for a single state, returning a tuple of 
    (fetched_result, parsed_data)

    If there's no query for the state: return (None, _)
    '''
    print("Fetching: ", state)
    res = None
    data = {}
    
    url = arcgis_urls.get(state)
    query = arcgis_queries.get(state)
    
    if not url:
        return res, data
    if (isinstance(url, list)):
        results = request_and_parse_multi(url, query)
    else:
        results = request_and_parse(url, query)

    if state in extras:
        data = extras[state](results, mappings.get(state))
    else:
        if (not isinstance(results, list)):
            results = [results]
        for result in results:
            partial = extract_attributes(result, mappings[state], state)
            data.update(partial)

    _timestamp_data(data)
    return results, data

def _timestamp_data(data):
    data[Fields.FETCH_TIMESTAMP.name] = datetime.now().timestamp()


def build_dataframe(results):
    columns=[Fields.FETCH_TIMESTAMP, Fields.TIMESTAMP,
             Fields.POSITIVE, Fields.NEGATIVE, Fields.TOTAL, Fields.PENDING,
             Fields.CURR_HOSP, Fields.HOSP, Fields.CURR_ICU, Fields.ICU, Fields.CURR_VENT,
             Fields.DEATH, Fields.DEATH_PROBABLE,
             Fields.RECOVERED]
    columns = [f.name for f in columns]

    df = pd.DataFrame.from_dict(results, 'index', columns=columns)
    base_name = 'states.csv'
    now_name = 'states_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S'))

    df.to_csv('{}/{}'.format(OUTPUT_FOLDER, now_name))
    df.to_csv('{}/{}'.format(OUTPUT_FOLDER, base_name))
    return df


def gen_user_link(state):
    url = arcgis_urls[state]
    query = arcgis_queries.get(state)
    if query:
        query['f'] = 'html'
        url = "{}?{}".format(url, urllib.parse.urlencode(query))
    return url


# def extract_attributes(res, mapping, debug_state = None):
#     '''Uses mapping to extract attributes from `res`
#     Retruns tagged attributes
#     '''
#     features = 'features'
#     attributes = 'attributes'
#     tagged_attributes = {}
#     if features in res and len(res[features]) > 0:        
#         if attributes in res[features][0]:
#             attribs = res[features][0][attributes]
#             for k, v in attribs.items():
#                 if k in mapping:
#                     tagged_attributes[mapping[k]] = v
#                 else:
#                     # report value without mapping
#                     print("[{}] Field {} has no mapping".format(debug_state, k))
#     return tagged_attributes


def glue(state = None):
    if state:
        print("Fetching ", state)
    else:
        print("Fetching all")
    
    read_data_sources()
    
    results = {}
    if state and state in arcgis_urls:
        _, data = fetch_state(state)
        results[state] = data
    else:
        results = fetch_all()

    # This will also store a CSV
    df = build_dataframe(results)
    print(df)
    

if __name__ == "__main__":
    print("Version: ", sys.version_info)    

    # read command line args, but don't bother too much
    state = sys.argv[1] if len(sys.argv) > 1 else None
    glue(state)
        
