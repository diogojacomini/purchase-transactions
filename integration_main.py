import pandas as pd
from tools import ExcelToParquet, ETLVendas
import os

data = os.path.join(os.getcwd(), 'data')
base_name = 'datasets.xlsx'


def controller_etl():
    # ETL - Vendas
    input_vendas = {
        'io': os.path.join(data, 'raw', base_name),
        'sheet_name': 'Sheet1',
        'header': None
    }

    output_vendas = {
        'path': os.path.join(data, 'processed', 'df_vendas.parquet'),
    }

    vendas = ETLVendas(input=input_vendas, output=output_vendas)
    vendas.controller_etl()

    # Others sheets
    sheets = {
        'Sheet2': 'produto',
        'Sheet3': 'consumidores',
        'Sheet4': 'lojas',
    }

    for sheet in sheets.keys():
        input = {
            'io': os.path.join(data, 'raw', base_name),
            'sheet_name': sheet,
        }

        output = {
            'path': os.path.join(data, 'processed', f'df_{sheets.get(sheet)}.parquet'),
        }

        etl = ExcelToParquet(input=input, output=output)
        etl.controller_etl()


def check_integration(left, right):
    if (len(left) != len(right)):
        print('Discrepencia, checar merge!')


def rename_columns(dataset, prefix):
    variables = [var for var in dataset.columns if var != 'id']

    for variable in variables:
        dataset.rename(columns={variable: f'{variable}_{prefix}'.lower()}, inplace=True)

    dataset.rename(columns={'id': f'{prefix.lower()}id'}, inplace=True)

    return dataset


def controller_integration():
    # open files
    df_vendas = pd.read_parquet(os.path.join(data, 'processed', 'df_vendas.parquet'))
    df_produto = pd.read_parquet(os.path.join(data, 'processed', 'df_produto.parquet'))
    df_consumidores = pd.read_parquet(os.path.join(data, 'processed', 'df_consumidores.parquet'))
    df_lojas = pd.read_parquet(os.path.join(data, 'processed', 'df_lojas.parquet'))

    # Integrations

    # Vendas x Produtos
    dataset = pd.merge(df_vendas, rename_columns(df_produto, 'product'),
                       how='left', on='ProductID'.lower())
    check_integration(dataset, df_vendas)

    # Dataset x Consumidores
    dataset = pd.merge(dataset, rename_columns(df_consumidores, 'client'),
                       how='left', on='ClientID'.lower())
    check_integration(dataset, df_vendas)

    # Dataset x Lojas
    dataset = pd.merge(dataset, rename_columns(df_lojas, 'store'),
                       how='left', on='StoreID'.lower())
    check_integration(dataset, df_vendas)

    # Save
    dataset.to_parquet(os.path.join(data, 'curated', 'dataset_integration.parquet'), compression='gzip')
    dataset.to_csv(os.path.join(data, 'curated', 'dataset_integration.csv'), index=False)


if __name__ == "__main__":
    controller_etl()
    controller_integration()
