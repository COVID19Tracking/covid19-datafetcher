import logging
import typing
from datetime import datetime

import pandas as pd

STATES = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU',
          'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
          'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
          'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
          'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']

class Result(dict):

    TS = 'TIMESTAMP'

    def __init__(self):
        super().__init__()

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

    def get_dataframe(self, columns, index, output_date_format,  dump_all_states=False):

        # results is a *dict*: state -> []
        if not self:
            return {}

        # need to prepare the index and preparing the data, and the columns
        # data: a list of dicts
        # index: a string or a list of len 2+
        # columns: add state even if not listed, if it's in index

        items = []
        for _, v in self.items():
            if isinstance(v, typing.List):
                items.extend(v)
            elif isinstance(v, typing.Dict):
                items.append(v)
            else:
                logging.warning("This shouldnt happen: %r", v)

        index = self._fix_index_and_columns(index, columns)
        df = pd.DataFrame(items, columns=columns)

        if Result.TS in index:
            df['DATE'] = df[Result.TS].dt.strftime(output_date_format)
            # TODO: resample to day? in addition to 'DATE' fild?
        df = df.set_index(index)
        df = df.groupby(level=df.index.names).last()

        if isinstance(index, list):
            # df.sort_index(level=[1, 0], ascending=[False, True], inplace=True)
            df.sort_index(ascending=False, inplace=True)
        elif dump_all_states:
            # no point doing it for backfill
            df = df.reindex(pd.Series(STATES, name='STATE'))

        return df

    def write_to_csv(self, filename, columns, index, output_date_format,  dump_all_states=False):

        base_name = "{}.csv".format(filename)
        now_name = '{}_{}.csv'.format(filename, datetime.now().strftime('%Y%m%d%H%M%S'))

        df = self.get_dataframe(columns, index, output_date_format,  dump_all_states)
        df.to_csv('{}'.format(now_name))
        df.to_csv('{}'.format(base_name))
        # TODO: if indexing by more than state, store individual state files?

        # Report an interesting metric:
        total_non_empty = df.notnull().sum().sum()
        logging.info("Fetched a total of %d cells", total_non_empty)
