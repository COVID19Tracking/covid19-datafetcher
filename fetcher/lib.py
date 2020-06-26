from datetime import datetime
from enum import Enum
import os
import logging
import pandas as pd
import sys
import urllib, urllib.request, json

from fetcher.utils import request, request_and_parse, extract_attributes, Fields, request_csv, \
    request_soup, request_pandas, extract_arcgis_attributes
from fetcher.sources import Sources, URLS_FILE, MAPPING_FILE, EXTRAS_MODULE


# TODO: not the place for this, but have plans to move it soon
logging.basicConfig(level=logging.DEBUG,
                    datefmt="%Y%m%d %H%M%S",
                    format='%(asctime)s %(levelno)s [%(name)s:%(lineno)d] %(message)s')

OUTPUT_FOLDER = "."

# TODO:
# - make a mapper of type to fetch method


class Fetcher(object):
    def __init__(self):
        '''Initialize source information'''
        self.sources = Sources(URLS_FILE, MAPPING_FILE, EXTRAS_MODULE)
        self.extras = self.sources.extras

    def has_state(self, state):
        return state in self.sources.keys()

    def fetch_all(self):
        results = {}
        success = 0
        failures = []

        for state in sorted(self.sources.keys()):
            try:
                res, data = self.fetch_state(state)
                if res:
                    if data:
                        results[state] = data
                        success += 1
                    else:
                        # failed parsing
                        logging.warning("Failed parsing %s", state)
                        failures.append(state)
            except Exception as e:
                logging.error("Failed to fetch %s", state, e)
                failures.append(state)

        logging.info("Fetched data for {} states".format(success))
        logging.info("Failed to fetch: %r", failures)
        return results

    def fetch_state(self, state):
        ''' Fetch data for a single state, returning a tuple of
        (fetched_result, parsed_data)

        If there's no query for the state: return (None, _)
        '''
        logging.debug("Fetching: %s", state)
        res = None
        data = {}

        queries = self.sources.queries_for(state)
        if not queries:
            return res, data

        results = []
        for query in queries:
            # TODO: make a better mapping here
            try:
                if query['type'] in ['arcgis', 'json', 'ckan', 'soda']:
                    res = request_and_parse(query['url'], query['params'])
                elif query['type'] in ['csv']:
                    res = request_csv(
                        query['url'], query['params'],
                        header=query.get('header', True), encoding=query.get('encoding'))
                elif query['type'] in ['html']:
                    res = request(query['url'], query['params'])
                elif query['type'] in ['html:soup']:
                    res = request_soup(query['url'], query['params'])
                elif query['type'] in ['pandas', 'xls', 'xlsx']:
                    res = request_pandas(query)
                results.append(res)
            except Exception as e:
                logging.error("{}: Failed to fetch {}".format(state, query['url']))
                raise

        if state in self.extras:
            data = self.extras[state](results, self.sources.mapping_for(state))
        else:
            for i, result in enumerate(results):
                if queries[i].get('type') == 'arcgis':
                    partial = extract_arcgis_attributes(result, self.sources.mapping_for(state), state)
                elif queries[i].get('data_path') is not None:
                    partial = extract_attributes(
                        result, queries[i].get('data_path'), self.sources.mapping_for(state), state)
                data.update(partial)

        self._timestamp_data(data)
        return results, data

    def _timestamp_data(self, data):
        data[Fields.FETCH_TIMESTAMP.name] = datetime.now().timestamp()


def build_dataframe(results, dump_all_states=False):
    columns=[Fields.FETCH_TIMESTAMP, Fields.TIMESTAMP,
             Fields.POSITIVE, Fields.NEGATIVE, Fields.TOTAL, Fields.PENDING,
             Fields.CURR_HOSP, Fields.HOSP, Fields.CURR_ICU, Fields.ICU, Fields.CURR_VENT, Fields.VENT,
             Fields.DEATH, Fields.DEATH_PROBABLE, Fields.DEATH_CONFIRMED,
             Fields.RECOVERED, Fields.PROBABLE, Fields.DATE,
             Fields.ANTIBODY_TOTAL, Fields.ANTIBODY_POS, Fields.ANTIBODY_NEG,
             Fields.SPECIMENS, Fields.SPECIMENS_POS, Fields.SPECIMENS_NEG,
             Fields.CONFIRMED
    ]
    columns = [f.name for f in columns]

    states = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU',
              'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
              'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
              'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
              'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
    ]

    df = pd.DataFrame.from_dict(results, 'index', columns=columns)
    if dump_all_states:
        df = df.reindex(pd.Series(states))
    base_name = 'states.csv'
    now_name = 'states_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S'))

    df.to_csv('{}/{}'.format(OUTPUT_FOLDER, now_name))
    df.to_csv('{}/{}'.format(OUTPUT_FOLDER, base_name))

    # Report an interesting metric:
    total_non_empty = df.notnull().sum().sum()
    logging.info("Fetched a total of %d cells", total_non_empty)
    return df


def main(state = None):
    if state:
        print("Fetching ", state)
    else:
        print("Fetching all")

    fetcher = Fetcher()
    results = {}
    if state and fetcher.has_state(state):
        _, data = fetcher.fetch_state(state)
        results[state] = data
    else:
        results = fetcher.fetch_all()

    # This will also store a CSV
    df = build_dataframe(results, dump_all_states = (state is None))
    print(df)
