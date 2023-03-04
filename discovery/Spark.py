from pyspark.sql.functions import col
from discovery.Exploratory import Exploratory


class ExploratorySpark(Exploratory):

    def looking_missings(dataset, spark):
        """Function to count the amount and percentage of missing values in the dataset per variable.

        Args:
            dataset (pyspark.sql.DataFrame): dataset for discovery missing values.

        Returns:
            pyspark.sql.DataFrame - Exemple:
            +----------+-----------+-------+
            |  variable|qty_missing|pct (%)|
            +----------+-----------+-------+
            |created_at|          3|   60.0|
            |      type|          2|   40.0|
            |        id|          1|   20.0|
            +----------+-----------+-------+

        * Variable: variable name present in the dataset;
        * qty_missing: total amount of missing values present in the variable;
        * pct (%): percentage of missing values present in the variable.
        """
        print('\033[1;34mChecking missing values per variable:\033[0;0m')
        dataset.cache()
        len_dataset = dataset.count()
        variables = {variable: dataset.filter(col(variable).isNull()).count() for variable in dataset.columns}
        df_result = spark.createDataFrame(list(variables.items()), schema=['variable', 'qty_missing'])\
                         .orderBy('qty_missing', ascending=False)

        df_result = df_result.withColumn('pct (%)', (col('qty_missing') / len_dataset) * 100)
        return df_result
