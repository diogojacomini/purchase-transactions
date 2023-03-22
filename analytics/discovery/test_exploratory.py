import pytest
from discovery import ExploratoryPandas
import pandas as pd


class TestPandasExploratory(ExploratoryPandas):

    def make_missing_df(self):
        df = {
            'id': [1, 2, 3, 4, None],
            'created_at': ['2020-02-01', None, None, None, '2020-02-03'],
            'type': ['red', None, 'blue', None, 'yellow']
        }

        df = pd.DataFrame(df, columns=['id', 'created_at', 'type', 'converted_tf'])
        return df

    def test_missings(self):
        df_test = self.looking_missings(dataset=self.make_missing_df())
        assert df_test is not None

    def test_missing_by(self):
        df_test = self.looking_missing_by(dataset=self.make_missing_df(), by='id')
        assert df_test is not None
