import logging
import typing
from datetime import datetime

import pandas as pd

STATES = ['AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU',
          'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI',
          'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV',
          'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
          'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY']


class Result(object):

    TS = 'TIMESTAMP'

    def __init__(self, filename, columns, index, output_date_format,  dump_all_states=False):
        self.filename = filename
        self._index, self._columns = self._fix_index_and_columns(index, columns)

        self.output_date_format = output_date_format
        self.dump_all_states = dump_all_states
        self._data = {}
        self._dataframe = pd.DataFrame(columns=self._columns)
        self._dataframe_processed = False

    @property
    def dataframe(self):
        if not self._dataframe_processed:
            self._process_dataframe()
        return self._dataframe

    def append(self, state, results):
        self._dataframe_processed = False
        self._data[state] = results

    def clear(self):
        self._dataframe_processed = False
        self._data.clear()

    def _process_dataframe(self):

        # results is a *dict*: state -> []
        if not self._data:
            return {}

        items = []
        for _, v in self._data.items():
            if isinstance(v, typing.List):
                items.extend(v)
            elif isinstance(v, typing.Dict):
                items.append(v)
            else:
                logging.warning("This shouldnt happen: %r", v)

        self._dataframe = pd.DataFrame(items, columns=self._columns)

        if Result.TS in self._index:
            self._dataframe['DATE'] = self._dataframe[Result.TS].dt.strftime(self.output_date_format)
            # TODO: resample to day? in addition to 'DATE' fild?
        self._dataframe = self._dataframe.set_index(self._index)
        self._dataframe = self._dataframe.groupby(level=self._dataframe.index.names).last()

        if isinstance(self._index, list):
            # df.sort_index(level=[1, 0], ascending=[False, True], inplace=True)
            self._dataframe.sort_index(ascending=False, inplace=True)
        elif self.dump_all_states:
            # no point doing it for backfill
            self._dataframe = self._dataframe.reindex(pd.Series(STATES, name='STATE'))

        self._dataframe_processed = True

    # columns: add state even if not listed, if it's in index
    # index: a string or a list of len 2+
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

        return index, columns

    def write_to_csv(self):

        base_name = "{}.csv".format(self.filename)
        now_name = '{}_{}.csv'.format(self.filename, datetime.now().strftime('%Y%m%d%H%M%S'))

        self.dataframe.to_csv('{}'.format(now_name))
        self.dataframe.to_csv('{}'.format(base_name))
        # TODO: if indexing by more than state, store individual state files?
