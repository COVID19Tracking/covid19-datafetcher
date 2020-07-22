import logging
import typing
from datetime import datetime

import pandas as pd


class Result(object):

    TS = 'TIMESTAMP'
    # TODO: move this somewhere else
    STATES = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU',
              'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
              'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
              'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
              'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']

    def __init__(self, results, columns, index, output_date_format,
                 filename, dump_all_states=False):
        self.results = results
        self.columns = columns
        self.index = index
        self.output_date_format = output_date_format
        self.filename = filename
        self.dump_all_states = dump_all_states

    def _fix_index_and_columns(self, index, columns):
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

    def get_dataframe(self):

        # results is a *dict*: state -> []
        if not self.results:
            return {}

        # need to prepare the index and preparing the data, and the columns
        # data: a list of dicts
        # index: a string or a list of len 2+
        # columns: add state even if not listed, if it's in index

        items = []
        for _, v in self.results.items():
            if isinstance(v, typing.List):
                items.extend(v)
            elif isinstance(v, typing.Dict):
                items.append(v)
            else:
                logging.warning("This shouldnt happen: %r", v)

        index = self._fix_index_and_columns(self.index, self.columns)
        df = pd.DataFrame(items, columns=self.columns)

        if Result.TS in index:
            df['DATE'] = df[Result.TS].dt.strftime(self.output_date_format)
            # TODO: resample to day? in addition to 'DATE' fild?
        df = df.set_index(index)
        df = df.groupby(level=df.index.names).last()

        if isinstance(index, list):
            # df.sort_index(level=[1, 0], ascending=[False, True], inplace=True)
            df.sort_index(ascending=False, inplace=True)
        elif self.dump_all_states:
            # no point doing it for backfill
            df = df.reindex(pd.Series(Result.STATES, name='STATE'))

        return df

    def write_to_csv(self):

        base_name = "{}.csv".format(self.filename)
        now_name = '{}_{}.csv'.format(self.filename, datetime.now().strftime('%Y%m%d%H%M%S'))

        df = self.get_dataframe()
        df.to_csv('{}'.format(now_name))
        df.to_csv('{}'.format(base_name))
        # TODO: if indexing by more than state, store individual state files?

        # Report an interesting metric:
        total_non_empty = df.notnull().sum().sum()
        logging.info("Fetched a total of %d cells", total_non_empty)
