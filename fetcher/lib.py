from datetime import datetime
import hydra
import logging
import typing

from fetcher.sources import Sources
from fetcher.result import Result
from fetcher.utils import request, request_and_parse, extract_attributes, Fields, request_csv, \
    request_soup, request_pandas, extract_arcgis_attributes

# TODO:
# - make a mapper of type to fetch method

MS_FILTER = datetime(2020, 1, 1, 0, 0).timestamp() * 1000
TS = 'TIMESTAMP'


class Fetcher(object):
    def __init__(self, cfg):
        '''Initialize source information'''
        self.dataset = cfg.dataset  # store dataset config
        self.sources = Sources(
            cfg.dataset.sources_file, cfg.dataset.mapping_file, cfg.dataset.extras_module)
        self.extras = self.sources.extras

    def has_state(self, state):
        return state in self.sources.keys()

    def fetch_all(self):
        results = Result()
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
            except Exception:
                logging.error("Failed to fetch %s", state, exc_info=True)
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

        queries = self.sources.queries_for(state)
        if not queries:
            return res, {}

        results = []
        mapping = self.sources.mapping_for(state)
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
            except Exception:
                logging.error("{}: Failed to fetch {}".format(state, query['url']), exc_info=True)
                raise

        processed_results = []
        if state in self.extras:
            processed_results = self.extras[state](results, mapping)
        else:
            for i, result in enumerate(results):
                if queries[i].get('type') == 'arcgis':
                    partial = extract_arcgis_attributes(result, mapping, state)
                else:
                    # This is a guess; getting an unknown top level object
                    partial = extract_attributes(
                        result, queries[i].get('data_path', []), mapping, state)
                processed_results.append(partial)

        data = self._aggregate_state_results(state, processed_results, mapping)
        return results, data

    def _aggregate_state_results(self, state, results, mapping):
        '''
        This function handles all the results (post-processing) from
        all queries to a single state.
        Result is always a flat list of dictionary records
        '''
        # Hiding any special casing for backdating or anything of the sorts

        # special casing here for extras handling
        if isinstance(results, typing.Dict):
            results = [results]

        data = []
        for x in results:
            if isinstance(x, typing.Dict):
                self._tag_and_timestamp(state, x, mapping.get('__strptime'))
                data.append(x)
            elif isinstance(x, typing.List):
                # do the same for each element
                for record in x:
                    self._tag_and_timestamp(state, record, mapping.get('__strptime'))
                    data.append(record)
            else:
                # should not happen
                logging.warning("Unexpected type in results: %r", x)
                pass

        return data

    def _tag_and_timestamp(self, state, data, dateformat=None):
        data[Fields.FETCH_TIMESTAMP.name] = datetime.now().timestamp()
        data[Fields.STATE.name] = state

        # we should also make sure that the timestamp field is datetime format
        # or parse the Date field
        if TS in data and data[TS]:
            # Check whether it's s or ms and convert to datetime
            ts = data[TS]
            data[TS] = datetime.utcfromtimestamp(ts/1000 if ts > MS_FILTER else ts)
        elif 'DATE' in data and data['DATE'] and dateformat:
            d = data['DATE']
            data[TS] = datetime.strptime(d, dateformat)
        else:
            # TODO: Should I add now time?
            pass


@hydra.main(config_path='..', config_name="config")
def main(cfg):
    print(cfg.pretty(resolve=True))

    if cfg.state and isinstance(cfg.state, str):
        cfg.state = cfg.state.split(',')

    fetcher = Fetcher(cfg)
    results = Result()
    if cfg.state:
        for s in cfg.state:
            _, mapped = fetcher.fetch_state(s)
            results[s] = mapped
    else:
        results = fetcher.fetch_all()

    # This stores the CSV with the requsted fields in order
    results.write_to_csv(cfg.output, cfg.dataset.fields, cfg.dataset.index,
                         cfg.output_date_format, dump_all_states=not cfg.state)

    print(results.get_dataframe(cfg.dataset.fields, cfg.dataset.index,
                                cfg.output_date_format, dump_all_states=not cfg.state))
