from discovery.Exploratory import Exploratory
import pandas as pd


class ExploratoryPandas(Exploratory):

    def looking_missings(self, dataset):
        """Function to count the amount and percentage of missing values in the dataset per variable.

        Args:
            dataset (pandas.DataFrame): dataset for discovery missing values.

        Returns:
            pandas.DataFrame - Exemple:
            +----+--------------+---------------+-----------+
            |    | variable     |   qty_missing |   pct (%) |
            |----+--------------+---------------+-----------|
            |  3 | converted_tf |             5 |       100 |
            |  1 | created_at   |             3 |        60 |
            |  2 | type         |             2 |        40 |
            |  0 | id           |             1 |        20 |
            +----+--------------+---------------+-----------+

        * Variable: variable name present in the dataset;
        * qty_missing: total amount of missing values present in the variable;
        * pct (%): percentage of missing values present in the variable.
        """
        print('\033[1;34mChecking missing values per variable:\033[0;0m')
        len_dataset = dataset.shape[0]
        variables = {variable: dataset[variable].isnull().sum() for variable in dataset.columns}

        result = (pd.DataFrame(list(variables.items()), columns=['variable', 'qty_missing'])
                  .sort_values('qty_missing', ascending=False))

        result['pct (%)'] = round((result['qty_missing'] / len_dataset) * 100, 2)
        result.reset_index().drop(columns=['index'], inplace=True)

        return result

    def looking_missing_by(self, dataset, by: str):
        """Counts the amount of missing values in the variables by some ID (by).

        Args:
            dataset (pandas.DataFrame): dataset for discovery missing values.
            by (str): name of variable id.

        Returns:
            pandas.DataFrame - Exemple:
        """
        df_missing = dataset.groupby(by).count().rsub(dataset.groupby(by).size(), axis=0)
        # print(df_missing.loc[(df_missing != 0).any(axis=1)])
        return df_missing
