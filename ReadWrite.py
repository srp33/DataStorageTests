import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import datetime, sys, time

dataset = sys.argv[1]
parquetFilePath = sys.argv[2]

print("Showing results for {} on {}...".format(dataset, datetime.datetime.now()), flush=True)
start = time.time()

table = pq.read_table(parquetFilePath)
step1 = time.time()
print("Elapsed time for pq.read_table: {} seconds.".format(step1 - start), flush=True)

df = table.to_pandas()
step2 = time.time()
print("Elapsed time for to_pandas: {} seconds.".format(step2 - step1), flush=True)

# See descriptions of additional tests to write in NOTES.
