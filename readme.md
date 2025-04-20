# dm-home

请确保运行本项目拥有32GB内存！

## 安装

安装Python 3.10环境，并执行如下命令安装依赖

```
pip install -r ./requirements.txt
```

## 运行

1. 按照`config-example.json`的格式创建`config.json`文件，指定数据文件目录
2. 运行`columnParser.py`，可能会花费3-6h的时间处理数据，如果不能一次性完成，每次只处理下面任务中的一项，注释其它项
    ```py
    parse_purchase_history_main(config['datapath_10G'], "processed_data_1", 8)
    parse_login_history_main(config['datapath_10G'], "processed_data_2", 8)
    parse_address_main(config['datapath_10G'], "processed_data_3", 8)
    
    parse_purchase_history_main(config['datapath_30G'], "processed_data_1", 16)
    parse_login_history_main(config['datapath_30G'], "processed_data_2", 16)
    parse_address_main(config['datapath_30G'], "processed_data_3", 16)
    ```
3. 打开`process_10GB.ipynb`或`process_30GB.ipynb`，逐单元运行代码。实际上两个文件只有数据文件位置不同。

## 文件解释

+ `columnParser.py`：预处理原parquet文件中的一些字符串字段，包括简单字符串切片、JSON反序列化、时间字符转时间戳等，并将结果保存为新的parquet文件，避免重复处理字符串
+ `config-example.json`：配置样例，参考此构建`config.json`文件
+ `process_1GB.ipynb`：旧1GB数据处理文件
+ `process_10GB.ipynb`：10GB数据处理文件
+ `process_30GB.ipynb`：30GB数据处理文件
+ `process_30GB_old.ipynb`：旧30GB数据处理文件
+ `requirement.txt`：Python所需依赖
+ `utils.py`：工具函数，包括分箱和卡方检验
+ `visualizations.py`：绘图函数，包括柱状图、堆叠柱状图、饼状图、箱形图、小提琴图、以及UpSetPlot图绘制
