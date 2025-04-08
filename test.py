import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
matplotlib.rcParams['font.family'] = 'SimHei'  # 黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号 "-" 显示为方块的问题

# 模拟数据
np.random.seed(0)
ckd_stages = ['CKD1期', 'CKD2期', 'CKD3期', 'CKD4期', 'CKD5期']
data = [
    np.random.normal(100, 10, 100),
    np.random.normal(80, 15, 100),
    np.random.normal(55, 12, 100),
    np.random.normal(25, 8, 100),
    np.random.normal(10, 5, 100),
]


# 构造 DataFrame
df = pd.DataFrame({
    "eGFR": np.concatenate(data),
    "CKD阶段": np.repeat(ckd_stages, 100)
})

plt.figure(figsize=(8, 6))
sns.boxplot(x="CKD阶段", y="eGFR", data=df, palette="Set2", showfliers=True)
plt.title("不同CKD阶段的eGFR分布")
plt.grid(True, linestyle='--', alpha=0.5)
plt.show()
