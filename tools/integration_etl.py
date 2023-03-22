from structure import ETL
import pandas as pd


class ExcelToParquet(ETL):
    def extract(self):
        '''Extraction of the data.'''
        self.data_rw = pd.read_excel(**self.input)

    def transform(self):

        # Rename variables
        self.df_processed = self.data_rw.copy()
        self.df_processed.columns = [i.lower() for i in self.df_processed]

        # Change type of data
        id_columns = [var for var in self.df_processed.columns if 'id' in var]
        if len(id_columns) > 0:
            for column in id_columns:
                self.df_processed[column] = self.df_processed[column].astype(str)

    def load(self):
        self.df_processed.to_parquet(**self.output, compression='gzip')


class ETLVendas(ETL):
    def extract(self):
        '''Extraction of the data.'''
        self.data_rw = pd.read_excel(**self.input)

    def transform(self):
        # Drop first columns with missig value
        self.df_processed = self.data_rw.loc[:, self.data_rw.notna().any(axis=0)]

        # Drop first lines with missing value
        self.df_processed = self.df_processed.dropna(how='all')

        # Get variables names
        variables_names = [i.lower() for i in self.df_processed.iloc[0].tolist()]

        # Drop first line with variables names
        self.df_processed = self.df_processed.reset_index(drop=True).drop(index=0)

        # Rename
        self.df_processed.columns = variables_names

        # Change type of data
        id_columns = [var for var in self.df_processed.columns if 'id' in var]
        if len(id_columns) > 0:
            for column in id_columns:
                self.df_processed[column] = self.df_processed[column].astype(str)

    def load(self):
        self.df_processed.to_parquet(**self.output, compression='gzip')
