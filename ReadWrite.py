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
discreteQuery = "OS_STATUS"
discreteValue = "LIVING"
otherCols = ['15-Sep', 'A2M', 'ZP1','XG','WRN','WTAP','ADAMTS15','tMDC', discreteQuery]
#givenColumn = 'DDR1'
#otherCols = ['GRN', 'CTSD', 'JUN', 'LDLR', 'ZIC1', 'RBX1', 'MIPEP', 'ACTB']

otherCols.insert(0, givenColumn)
otherCols.insert(0, "Sample")
df = pd.read_parquet(parquetFilePath,columns=otherCols)

####
#df2=pd.read_parquet(parquetFilePath, columns=['Sample','CNA__TRAT1'])
#df2.set_index("Sample",drop=True,inplace=True)
#df2.to_csv(path_or_buf='out2.txt', sep='\t')
###

df.set_index("Sample", drop=True,inplace=True)
del otherCols[0]
step0 = time.time()
print("Elapsed time for pd.read_parquet: {} seconds".format(step0-start),flush=True)

#query all numeric data
colData = df.loc[df[givenColumn]>5 , [ col for col in otherCols]]
#now perform a discrete query from the metadata 
colData2 = colData.loc[colData[discreteQuery]==discreteValue, [col for col in otherCols]]

step3 = time.time()
print("Elapsed time for querying pandas dataframe: {} seconds.".format(step3-step0),flush=True)
#write to file
colData2.to_csv(path_or_buf = 'out.txt', sep='\t')
step4= time.time()
print("Elapsed time for printing results to output file: {} seconds".format(step4-step3), flush=True)
# See descriptions of additional tests to write in NOTES.
