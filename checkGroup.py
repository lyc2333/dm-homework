import pyarrow.parquet as pq

# 加载 parquet 文件
pf = pq.ParquetFile(r"processed_data\10G_data_new\part.0.parquet")

# 文件中 row group 的数量
print("Row groups:", pf.num_row_groups)

# 遍历每个 row group，查看大小、行数等
for i in range(pf.num_row_groups):
    rg = pf.metadata.row_group(i)
    num_rows = rg.num_rows
    total_byte_size = rg.total_byte_size
    print(f"Row Group {i}: rows={num_rows}, size={total_byte_size} bytes")
