import pandas as pd
import dask.dataframe as dd
import numpy as np
from dask.dataframe.dask_expr._collection import DataFrame

from scipy.stats import chi2_contingency

def create_bins(df: DataFrame, src_col: str, num: int = 5):
    bmin, bmax = df[src_col].min().compute(), df[src_col].max().compute()
    bins = np.linspace(bmin, bmax, num=num+1)

    return df[src_col].map_partitions(pd.cut, bins=bins)


def x2_test(df, category_col: str, subcategory_col: str):
    grouped = df.groupby([category_col, subcategory_col]).size().compute().reset_index(name='count')

    contingency_table = grouped.pivot_table(
        index=category_col,
        columns=subcategory_col,
        values='count',
        fill_value=0  # 缺失的组用0填充
    )
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    return chi2, p, dof, expected

