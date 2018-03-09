import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import time, sys

dataset = sys.argv[1]

tsvFilePath = "{}/data.tsv".format(dataset)
parquetFilePath = "/tmp/{}.pq".format(dataset)

print("Showing results for {}.".format(dataset))

start = time.time()

df = pd.read_csv(tsvFilePath, sep="\t")
step1 = time.time()
print("Elapsed time for pd.read_csv: {} steps.".format(step1 - start))

table = pa.Table.from_pandas(df)
step2 = time.time()
print("Elapsed time for pa.Table.from_pandas: {} steps.".format(step2 - step1))

pq.write_table(table, parquetFilePath) # snappy compression is the default
#pq.write_table(table, parquetFilePath, compression='gzip')
#pq.write_table(table, parquetFilePath, compression='brotli')
#pq.write_table(table, parquetFilePath, compression='none')
step3 = time.time()
print("Elapsed time for pq.write_table: {} steps.".format(step3 - step2))


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
