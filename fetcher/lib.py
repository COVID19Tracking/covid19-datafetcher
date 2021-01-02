from datetime import datetime
import logging
import typing
import hydra
import pandas as pd
import os
import urllib.request

from fetcher.utils import Fields
from fetcher.source_utils import fetch_source, process_source_responses
from fetcher.sources import build_sources

# Indices
TS = 'TIMESTAMP'
STATE = Fields.STATE.name

site_url = "https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/raw_data" \
           "/vaccine_data_us_state_timeline.csv "


class Fetcher:
    def __init__(self, cfg):
        '''Initialize source information'''
        self.dataset = cfg.dataset  # store dataset config
        self.sources = build_sources(
            cfg.dataset.sources_file, cfg.dataset.mapping_file, cfg.dataset.extras_module)

    def has_state(self, state):
        return state in self.sources

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
        source = self.sources.get(state)
        if not source or not source.queries:
            return res, {}

        results = fetch_source(source)
        data = process_source_responses(source, results)
        return results, data


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
    # special casing here, because of groupby+dropna bug
    if isinstance(index, list) and len(index) > 1:
        for c in index:
            df[c] = df[c].fillna('n/a')
    df = df.set_index(index)
    df = df.groupby(level=df.index.names, dropna=False).last()

    # Notice: Reindexing and then sorting means that we're always sorting
    # the index, and not using the order that comes from configuration
    if not isinstance(index, list):
        # Reindex based on the given states, when we don't have
        # additional columns to index to (i.e., do not do it for backfill)
        df = df.reindex(pd.Series(states_to_index, name=STATE))

    if TS in df.index.names:
        # For each state, we forward fill, and resample to 1-day intervals
        # and forward fill the newely added days
        part = [x for x in index if x != TS]
        df = df.reset_index(part).groupby(part).apply(
            lambda d: d.ffill().resample('1D', closed='right').ffill()
        ).drop(columns=part)
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


def save_df_to_db(db_config, df):
    # verify again that we should store to db
    if not db_config.store:
        return

    print("Storing to DB {db_name}.{table}".format(**db_config))

    # import it here to not force it as a dependency if not storing anywhere
    from sqlalchemy import create_engine

    renames = Fields.map()
    df.rename(columns=renames, inplace=True)
    df.index.rename([renames[x] for x in df.index.names], inplace=True)
    engine_conf = "{driver}://{username}:{password}@{host}:{port}/{db_name}".format(
        **db_config)
    engine = create_engine(engine_conf)
    df.to_sql(db_config.table, engine, if_exists='append', chunksize=200, method='multi')


def get_covid_vaccination_data(outputdir):
    """Download COVID Vaccination data in CSV format
    """
    if os.path.exists(outputdir + '/' + 'covid_vaccination_US.csv'):
        os.remove(outputdir + '/' + 'covid_vaccination_US.csv')

    urllib.request.urlretrieve(site_url, os.path.join(outputdir, 'covid_vaccination_US.csv'))


@hydra.main(config_path='..', config_name="config")
def main(cfg):
    print(cfg.dataset.pretty())

    if cfg.state and isinstance(cfg.state, str):
        cfg.state = cfg.state.split(',')

    fetcher = Fetcher(cfg)
    results = fetcher.fetch_all(cfg.state)

    # This stores the CSV with the requsted fields in order
    df = build_dataframe(results, cfg.state, cfg.dataset, cfg.output_date_format, cfg.output)
    print(df)
	
    get_covid_vaccination_data(cfg.outputs)

    if 'db' in cfg.dataset and cfg.dataset.db.store:
        save_df_to_db(cfg.dataset.db, df)
