import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import datetime, sys, time
import dask.dataframe as dd

dataset = sys.argv[1]
tsvFilePath = sys.argv[2]
tsvFilePath2 = sys.argv[3]
parquetFilePath = sys.argv[4]

print("Showing results for {} on {}...".format(dataset, datetime.datetime.now()), flush=True)
start = time.time()

#change back to pd.read_csv
df = pd.read_csv(tsvFilePath, sep="\t",engine='c')
df2 = pd.read_csv(tsvFilePath2, sep="\t", engine='c')
step1 = time.time()

print("Elapsed time for pd.read_csv: {} seconds.".format(step1 - start), flush=True)
mergedDF = pd.merge(df,df2, how='inner', on='Sample')

table = pa.Table.from_pandas(mergedDF)
step2 = time.time()
print("Elapsed time for pa.Table.from_pandas: {} seconds.".format(step2 - step1), flush=True)

#pq.write_table(table, parquetFilePath) # snappy compression is the default
#pq.write_table(table, parquetFilePath, compression='gzip')
#pq.write_table(table, parquetFilePath, compression='brotli')
pq.write_table(table, parquetFilePath, compression='none')
step3 = time.time()
print("Elapsed time for pq.write_table: {} seconds.".format(step3 - step2), flush=True)


# TODO:
#   1. Log in to kumiko.byu.edu (from on campus).
#   2. Create a Docker
#   1. Make sure you are using python3/5 (or later).
#   1. Update to the latest version of pandas (0.22).
#   2. Install dask. pip3 install "dask[dataframe]"
#   1. Obtain a tab-separated data file that has ~20,000 columns.
#   2. Unzip that file.
#   3. Using that file, quantify time to execute the following:
#      Read the file into a pandas DataFrame.
#      Read the file into a pandas DataFrame.
#     Retrieve the first 3 columns and save to a TSV file.
#     Retrieve 3 random columns and save to a TSV file.
#     Filter based on the first 3 columns and retrieve the next 3 columns.
#     Filter based on the 3 random columns and retrieve 3 random columns.
#     Try using dask rather than pandas to read the TSV file and write the parquet file. Example: http://datashader.org/user_guide/10_Performance.html
#     Try reading parquet file using pandas (version 0.22) rather than pyarrow.
#  Later:
#     Compare different compression schemes
#     Discrete values...
