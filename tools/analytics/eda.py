"""Script for inspecting and exploring the data."""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def get_view_describe(dataset, features):
    ''''Function to generate the chart of summarized variables.'''
    list_describe = []
    for v in features:
        list_describe.append(dataset[v].describe().to_frame())

    df_describe = pd.concat(list_describe, axis=1).reset_index().rename(columns={'index': 'metrica'})
    df_describe = df_describe[df_describe['metrica'].isin(['min', 'max', 'mean', 'std'])]

    # Melt
    df_describe = pd.melt(df_describe, id_vars=['metrica'], value_vars=features)

    # Data normalization to simplify visualization
    df_describe['norm_value'] = (df_describe['value'] - df_describe['value'].mean()) / df_describe['value'].std()

    # Plot
    sns.barplot(x='variable', y='norm_value',
                hue='metrica', data=df_describe, palette="Set2")
    plt.ylabel('')
    plt.xlabel('')
    plt.show()


def get_bar_plot(dataset, x, y, best=None, title=None):
    if best:
        if best > 0:
            dataset = dataset.head(best)
        else:
            dataset = dataset.tail(best * (-1)).reset_index(drop=True)

    ax = sns.barplot(x=x, y=y, data=dataset)
    ax.set_xticklabels(ax.get_xticklabels(), visible=False)
    plt.title(title)
    plt.xlabel('')
    plt.ylabel('')

    # adicionar as proporções como rótulos de cada barra
    for i, v in enumerate(dataset[y]):
        ax.text(i - .15, v + .05, str(dataset[y][i]) + '%')

    plt.show()
