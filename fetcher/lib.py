from datetime import datetime
from enum import Enum
import os
import logging
import hydra
import pandas as pd
import sys
import typing
import urllib, urllib.request, json

from fetcher.utils import request, request_and_parse, extract_attributes, Fields, request_csv, \
    request_soup, request_pandas, extract_arcgis_attributes
from fetcher.sources import Sources

# TODO:
# - make a mapper of type to fetch method


class Fetcher(object):
    def __init__(self, cfg):
        '''Initialize source information'''
        self.sources = Sources(cfg.sources_file, cfg.mapping_file, cfg.extras_module)
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
        if failures:
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
                else:
                    # This is a guess; getting an unknown top level object
                    partial = extract_attributes(
                        result, queries[i].get('data_path', []), self.sources.mapping_for(state), state)

                # special handling for backfilling
                if isinstance(partial, typing.List):
                    print("why would this happen here?")
                    data = partial
                    for x in data:
                        # timestamping is shit here
                        self._timestamp_data(x)
                else:
                    data.update(partial)

        if isinstance(data, typing.Dict):
            self._timestamp_data(data)
        return results, data

    def _timestamp_data(self, data):
        data[Fields.FETCH_TIMESTAMP.name] = datetime.now().timestamp()


def _build_backfill_dataframe(results, columns):
    # TODO: reindex like day
    dfs = []
    for k, v in results.items():
        partial = pd.DataFrame(v, columns=columns)
        partial.to_csv("{}.csv".format(k))
        partial['state'] = k
        dfs.append(partial)

    return pd.concat(dfs)

def _build_aggregated_dataframe(results, columns):
    return pd.DataFrame.from_dict(results, 'index', columns=columns)

def build_dataframe(results, columns, filename, dump_all_states=False):
    # TODO: move this somewhere else
    states = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU',
              'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
              'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
              'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
              'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
    ]

    # results is a dict: state -> {} or []
    if not results:
        return {}

    # Special case backfilling
    if isinstance(list(results.values())[0], typing.List):
        df = _build_backfill_dataframe(results, columns)
    else:
        df = _build_aggregated_dataframe(results, columns)
        if dump_all_states:
            # TODO: think about it in the backfilling case
            df = df.reindex(pd.Series(states))

    base_name = "{}.csv".format(filename)
    now_name = '{}_{}.csv'.format(filename, datetime.now().strftime('%Y%m%d%H%M%S'))
    df.to_csv('{}'.format(now_name))
    df.to_csv('{}'.format(base_name))

    # Report an interesting metric:
    total_non_empty = df.notnull().sum().sum()
    logging.info("Fetched a total of %d cells", total_non_empty)

    return df

@hydra.main(config_path='..', config_name="config")
def main(cfg):
    print(cfg.pretty(resolve=True))

    if cfg.state and isinstance(cfg.state, str):
        cfg.state = cfg.state.split(',')

    fetcher = Fetcher(cfg)
    results = {}
    if cfg.state:
        for s in cfg.state:
            _, mapped = fetcher.fetch_state(s)
            results[s] = mapped
    else:
        results = fetcher.fetch_all()

    # This stores the CSV with the requsted fields in order
    df = build_dataframe(results, cfg.dataset.fields, cfg.output,
                         dump_all_states = not cfg.state)
    print(df)
