import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import sys
filePath1 = sys.argv[1]
filePath2 = sys.argv[2]
exportFilePath = sys.argv[3]
df = pd.read_parquet(filePath1)
df2 = pd.read_parquet(filePath2)

mergedDF = pd.merge(df,df2, how='inner', on='Sample')
mergedDF.to_parquet(exportFilePath)
