from datetime import datetime
import logging
import typing
import hydra
import pandas as pd

from fetcher.utils import request, request_and_parse, extract_attributes, Fields, request_csv, \
    request_soup, request_pandas, extract_arcgis_attributes
from fetcher.sources import Sources


# TODO:
# - make a mapper of type to fetch method

MS_FILTER = datetime(2020, 1, 1, 0, 0).timestamp() * 1000
# Indices
TS = 'TIMESTAMP'
STATE = Fields.STATE.name


class Fetcher:
    def __init__(self, cfg):
        '''Initialize source information'''
        self.dataset = cfg.dataset  # store dataset config
        self.sources = Sources(
            cfg.dataset.sources_file, cfg.dataset.mapping_file, cfg.dataset.extras_module)
        self.extras = self.sources.extras

    def has_state(self, state):
        return state in self.sources.keys()

    def fetch_all(self, states):
        results = {}
        success = 0
        failures = []

        for state in states:
            if not self.has_state(state):
                # Nothing to do for a state without sources
                continue
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

        return data

    def _tag_and_timestamp(self, state, data, dateformat=None):
        data[Fields.FETCH_TIMESTAMP.name] = datetime.now().timestamp()
        data[STATE] = state

        # we should also make sure that the timestamp field is datetime format
        # or parse the Date field
        if TS in data and data[TS]:
            # Check whether it's s or ms and convert to datetime
            ts = data[TS]
            data[TS] = datetime.fromtimestamp(ts/1000 if ts > MS_FILTER else ts)
        elif 'DATE' in data and data['DATE'] and dateformat:
            d = data['DATE']
            data[TS] = datetime.strptime(d, dateformat)
        else:
            # TODO: Should I add now time?
            pass


def _fix_index_and_columns(index, columns):
    index = index if isinstance(index, str) else list(index)
    if isinstance(index, list) and len(index) == 1:
        index = index[0]

    # make sure all index columns are also in columns
    if isinstance(index, str) and index not in columns:
        columns.insert(0, index)
    elif isinstance(index, list):
        for c in index:
            if c not in columns:
                columns.insert(0, c)

    return index


def build_dataframe(results, states_to_index, dataset_cfg, output_date_format, filename=None):
    # TODO: move file generation out of here
    # results is a *dict*: state -> []
    if not results:
        return {}

    # need to prepare the index and preparing the data, and the columns
    # data: a list of dicts
    # index: a string or a list of len 2+
    # columns: add state even if not listed, if it's in index
    index = dataset_cfg.index
    columns = dataset_cfg.fields
    index = _fix_index_and_columns(index, columns)

    items = []
    for _, v in results.items():
        if isinstance(v, typing.List):
            items.extend(v)
        elif isinstance(v, typing.Dict):
            items.append(v)
        else:
            logging.warning("This shouldnt happen: %r", v)

    df = pd.DataFrame(items, columns=columns)
    df = df.set_index(index)
    df = df.groupby(level=df.index.names).last()

    # Notice: Reindexing and then sorting means that we're always sorting
    # the index, and not using the order that comes from configuration
    if not isinstance(index, list):
        # Reindex based on the given states, when we don't have
        # additional columns to index to (i.e., do not do it for backfill)
        df = df.reindex(pd.Series(states_to_index, name=STATE))

    if TS in df.index.names:
        # For each state, we forward fill, and resample to 1-day intervals
        # and forward fill the newely added days
        df = df.reset_index(STATE).groupby(STATE).apply(
            lambda d: d.ffill().resample('1D', closed='right').ffill()
        ).drop(columns=STATE)
        df.sort_index(level=TS, ascending=False, inplace=True)

    df.sort_index(level=STATE, kind='mergesort', ascending=True, sort_remaining=False, inplace=True)
    # Add existing date format when we're done with all other updates
    if TS in df.index.names:
        df['DATE'] = df.index.get_level_values(level=TS).strftime(output_date_format)

    if filename:
        base_name = "{}.csv".format(filename)
        now_name = '{}_{}.csv'.format(filename, datetime.now().strftime('%Y%m%d%H%M%S'))
        df.to_csv('{}'.format(now_name))
        df.to_csv('{}'.format(base_name))
        # TODO: if indexing by more than state, store individual state files?

    # Report an interesting metric:
    total_non_empty = df.notnull().sum().sum()
    logging.info("Fetched a total of %d cells", total_non_empty)

    return df


@hydra.main(config_path='..', config_name="config")
def main(cfg):
    print(cfg.dataset.pretty(resolve=True))

    if cfg.state and isinstance(cfg.state, str):
        cfg.state = cfg.state.split(',')

    fetcher = Fetcher(cfg)
    results = fetcher.fetch_all(cfg.state)

    # This stores the CSV with the requsted fields in order
    df = build_dataframe(results, cfg.state, cfg.dataset, cfg.output_date_format, cfg.output)
    print(df)
