from datetime import datetime
import fetcher.lib as lib
import types
import pandas as pd
import numpy as np


COLUMNS = ['a', 'b', 'c']
STATES = ['ARG', 'BAR', 'FOO']


class TestDataFrame(object):

    def build_and_compare_1col(self, items, expected_items):
        '''Helper function to create the expected input and handle
        DF output for comparison'''
        ncolumns = 1
        num_states = len(items)

        results = {}
        for i, state_items in enumerate(items):
            for x in state_items:
                x[lib.STATE] = STATES[i]
            results[STATES[i]] = state_items

        expected = []
        for i, state_items in enumerate(expected_items):
            for x in state_items:
                x[lib.STATE] = STATES[i]
            expected.extend(state_items)

        cfg = types.SimpleNamespace()
        cfg.index = [lib.STATE, lib.TS]
        cfg.fields = COLUMNS[:ncolumns]

        df = lib.build_dataframe(results, [STATES[:num_states]], cfg, '%Y%m%d')
        # prepare for comparison: Remove Timestamp, remove indices
        res_df = df.reset_index().drop(columns=lib.TS)
        print("Res df to compare")
        print(res_df)
        expected_df = pd.DataFrame(expected, columns=[lib.STATE, COLUMNS[0], 'DATE'])
        print("expected DF:")
        print(expected_df)
        assert res_df.equals(expected_df)

    def test_ffill_single_series(self):
        ''' Want to test that filling of missing values works correctly
        Values are are carried over (from older days to new days without value)
        '''
        column = COLUMNS[0]
        test_items = [
            {lib.TS: datetime(2020, 10, 1)},
            {lib.TS: datetime(2020, 10, 2), column: 1},
            {lib.TS: datetime(2020, 10, 3)},
            {lib.TS: datetime(2020, 10, 4)},
            {lib.TS: datetime(2020, 10, 5), column: 2},
        ]

        # record format
        expected = [
            {'DATE': '20201005', column: 2},
            {'DATE': '20201004', column: 1},
            {'DATE': '20201003', column: 1},
            {'DATE': '20201002', column: 1},
            {'DATE': '20201001', column: np.nan},
        ]

        self.build_and_compare_1col([test_items], [expected])

    def test_ffill_2series(self):
        ''' Want to test that filling of missing values works correctly
        Values are are carried over (from older days to new days without value)
        '''
        column = COLUMNS[0]
        test_items1 = [
            {lib.TS: datetime(2020, 10, 1), column: 0},
            {lib.TS: datetime(2020, 10, 2), column: 1},
            {lib.TS: datetime(2020, 10, 3)},
            {lib.TS: datetime(2020, 10, 4)},
            {lib.TS: datetime(2020, 10, 5), column: 2},
        ]

        test_items2 = [
            {lib.TS: datetime(2020, 10, 1)},
            {lib.TS: datetime(2020, 10, 2), column: 10},
            {lib.TS: datetime(2020, 10, 3)},
            {lib.TS: datetime(2020, 10, 4), column: 20},
            {lib.TS: datetime(2020, 10, 5)}
        ]

        # record format
        expected1 = [
            {'DATE': '20201005', column: 2.0},
            {'DATE': '20201004', column: 1.0},
            {'DATE': '20201003', column: 1.0},
            {'DATE': '20201002', column: 1.0},
            {'DATE': '20201001', column: 0.0},
        ]
        expected2 = [
            {'DATE': '20201005', column: 20.0},
            {'DATE': '20201004', column: 20.0},
            {'DATE': '20201003', column: 10.0},
            {'DATE': '20201002', column: 10.0},
            {'DATE': '20201001', column: np.nan},
        ]

        self.build_and_compare_1col([test_items1, test_items2], [expected1, expected2])

    def test_reindex_to_day_1col(self):
        column = COLUMNS[0]
        test_items = [
            {lib.TS: datetime(2020, 10, 2), column: 1},
            {lib.TS: datetime(2020, 10, 4), column: 2},
            {lib.TS: datetime(2020, 10, 5), column: 3},
        ]

        # record format
        expected = [
            {'DATE': '20201005', column: 3},
            {'DATE': '20201004', column: 2},
            {'DATE': '20201003', column: 1},
            {'DATE': '20201002', column: 1},
            # {'DATE': '20201001', column: np.nan},
        ]

        self.build_and_compare_1col([test_items], [expected])

    def test_reindex_unsorted_1col(self):
        column = COLUMNS[0]
        test_items = [
            {lib.TS: datetime(2020, 10, 5), column: 3},
            {lib.TS: datetime(2020, 10, 4), column: 2},
            {lib.TS: datetime(2020, 10, 2), column: 1},
        ]

        # record format
        expected = [
            {'DATE': '20201005', column: 3},
            {'DATE': '20201004', column: 2},
            {'DATE': '20201003', column: 1},
            {'DATE': '20201002', column: 1},
            # {'DATE': '20201001', column: np.nan},
        ]

        self.build_and_compare_1col([test_items], [expected])
