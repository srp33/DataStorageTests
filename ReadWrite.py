import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import datetime, sys, time

dataset = sys.argv[1]
parquetFilePath = sys.argv[2]

print("Showing results for {} on {}...".format(dataset, datetime.datetime.now()), flush=True)
start = time.time()

#example data a user might query
givenColumn = '1-Sep'
otherCols = ['15-Sep', 'A2M', 'ZP1','XG','WRN','WTAP','ADAMTS15','tMDC']
#givenColumn = 'DDR1'
#otherCols = ['GRN', 'CTSD', 'JUN', 'LDLR', 'ZIC1', 'RBX1', 'MIPEP', 'ACTB']

otherCols.insert(0, givenColumn)
#column 0 might be what is supposed to be the row names, instead stored as data??
otherCols.insert(0, "Sample")
#Can we read the parquet file directly into pandas dataframe? Only certain columns?
#Can the parquet file be used like a database?
df = pd.read_parquet(parquetFilePath,columns=otherCols)
df.set_index("Sample", drop=True,inplace=True)
del otherCols[0]
step0 = time.time()
print("Elapsed time for pd.read_parquet: {} seconds".format(step0-start),flush=True)

#table = pq.read_table(parquetFilePath)
#step1 = time.time()
#print("Elapsed time for pq.read_table: {} seconds.".format(step1 - step0), flush=True)

#df = table.to_pandas()
#step2 = time.time()
#print("Elapsed time for to_pandas: {} seconds.".format(step2 - step1), flush=True)

#set row indexes to be the sample id
#selects all values in the given column where the value is greater than 7, as well as 
#all values in those same rows for all the columns in otherCols

colData = df.loc[df[givenColumn]>6.5 , [ col for col in otherCols]]
step3 = time.time()
print("Elapsed time for querying pandas dataframe: {} seconds.".format(step3-step0),flush=True)
#write to file
colData.to_csv(path_or_buf = 'out.txt', sep='\t')
step4= time.time()
print("Elapsed time for printing results to output file: {} seconds".format(step4-step3), flush=True)
# See descriptions of additional tests to write in NOTES.
