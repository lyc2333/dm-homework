import dask.dataframe as dd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family'] = 'SimHei'  # 黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号 "-" 显示为方块的问题
plt.rcParams["figure.figsize"] = (12, 8)

from dask.dataframe.dask_expr._collection import DataFrame
def draw_barplot(df:DataFrame,target_column:str,print_counts=False):

    # 统计数量
    counts = df[target_column].value_counts().compute().reset_index()
    counts.columns = [target_column, 'count']
    if print_counts:
        print(counts)
    # 使用 seaborn 绘制柱状图
    sns.barplot(data=counts, x=target_column, y='count')
    plt.title(f'{target_column} Distribution')
    plt.xlabel(target_column)
    plt.ylabel('Count')
    plt.show()

def draw_stacked_barplot(df, category_col: str, subcategory_col: str):
    """
    绘制堆叠柱状图：每个 category_col 对应的 subcategory_col 分布
    例如：category_col = 'country', subcategory_col = 'gender'
    """
    # 先做 groupby + count
    grouped = df.groupby([category_col, subcategory_col]).size().compute().reset_index(name='count')
    
    # 透视成适合堆叠柱状图的数据结构
    pivot_df = grouped.pivot(index=category_col, columns=subcategory_col, values='count').fillna(0)

    # 绘制堆叠柱状图
    pivot_df.plot(kind='bar', stacked=True, figsize=(10, 6))

    plt.title(f'{subcategory_col} Distribution by {category_col}')
    plt.xlabel(category_col)
    plt.ylabel('Count')
    plt.legend(title=subcategory_col)
    plt.tight_layout()
    plt.show()
