from upsetplot import UpSet, from_indicators
from dask.dataframe.dask_expr._collection import DataFrame
import dask.dataframe as dd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family'] = 'SimHei'  # 黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号 "-" 显示为方块的问题
plt.rcParams["figure.figsize"] = (12, 8)


def draw_barplot(df: DataFrame, target_column: str, print_counts=False, figsize=(12, 8)):

    # 统计数量
    counts = df[target_column].value_counts().compute().reset_index()
    counts.columns = [target_column, 'count']
    counts = counts.sort_values(by='count', ascending=False)

    if print_counts:
        print(counts)

    if figsize is not None:
        plt.figure(figsize=figsize)
    # 使用 seaborn 绘制柱状图
    ax = sns.barplot(data=counts, x=target_column, y='count')

    # 在柱上标注具体数值
    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', padding=3)

    # sns.barplot(data=counts, x=target_column, y='count')
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


def draw_pieplot(df: DataFrame, target_column: str, print_counts=False):
    """
    绘制饼状图：显示 target_column 中各类的占比
    """
    # 统计数量
    counts = df[target_column].value_counts().compute().reset_index()
    counts.columns = [target_column, 'count']

    if print_counts:
        print(counts)

    # 绘图
    plt.figure(figsize=(8, 8))
    plt.pie(counts['count'], labels=counts[target_column], autopct='%1.1f%%', startangle=140)
    plt.title(f'{target_column} Distribution (Pie Chart)')
    plt.axis('equal')  # 保证圆形
    plt.show()


def draw_boxplot(df: DataFrame, numeric_column: str, category_column: str = None, figsize=(6, 8)):
    """
    绘制箱型图：展示数值列的中位数、分布范围和异常值
    - numeric_column: 数值列名，如 'price'、'age'
    - category_column: 类别列名（可选），如 'gender'，用于分组比较
    """
    df_pd = df[[numeric_column] + ([category_column] if category_column else [])].compute()

    plt.figure(figsize=figsize)
    if category_column:
        sns.boxplot(data=df_pd, x=category_column, y=numeric_column)
        plt.title(f'{numeric_column} Distribution by {category_column} (Box Plot)')
    else:
        sns.boxplot(data=df_pd, y=numeric_column)
        plt.title(f'{numeric_column} Distribution (Box Plot)')

    plt.xlabel(category_column if category_column else '')
    plt.ylabel(numeric_column)
    plt.tight_layout()
    plt.show()


def draw_violinplot(df: DataFrame, numeric_column: str, category_column: str = None, figsize=(6, 8)):
    """
    绘制小提琴图：展示数值列的分布密度和中位数
    - numeric_column: 数值列名，如 'price'、'age'
    - category_column: 类别列名（可选），如 'gender'，用于分组比较
    """
    df_pd = df[[numeric_column] + ([category_column] if category_column else [])].compute()

    plt.figure(figsize=figsize)
    if category_column:
        sns.violinplot(data=df_pd, x=category_column, y=numeric_column, inner='quartile')
        plt.title(f'{numeric_column} Distribution by {category_column} (Violin Plot)')
    else:
        sns.violinplot(data=df_pd, y=numeric_column, inner='quartile')
        plt.title(f'{numeric_column} Distribution (Violin Plot)')

    plt.xlabel(category_column if category_column else '')
    plt.ylabel(numeric_column)
    plt.tight_layout()
    plt.show()


def draw_upsetplot(df: DataFrame, column_list: list, subset_name: str, figsize=(12, 8)):
    """
    绘制 UpSet Plot：展示多个布尔字段的交集关系
    参数：
        df: Dask DataFrame
        column_list: 布尔字段名列表，例如 ["devices_desktop", "devices_mobile", "devices_tablet"]
        subset_name: 图标题中交集分析的描述名称
    """
    # 先转为 Pandas，因为 upsetplot 不支持 Dask DataFrame
    df_pd = df[column_list].compute()

    # 构造交集数据
    upset_data = from_indicators(data=df_pd, indicators=column_list)

    # 绘图
    fig = plt.figure(figsize=figsize)
    UpSet(upset_data, subset_size='count', show_counts=True, element_size=None).plot(fig=fig, )
    plt.suptitle(f'UpSet Plot of {subset_name}', fontsize=14)
    # plt.tight_layout()
    plt.show()
