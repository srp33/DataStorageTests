import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import datetime, sys, time

dataset = sys.argv[1]
parquetFilePath = sys.argv[2]

print("Showing results for {} on {}...".format(dataset, datetime.datetime.now()), flush=True)
start = time.time()

#example data a user might query
#givenColumn = '1-Sep'
#discreteQueries=["OS_STATUS", "RADIO_THERAPY"]
#discreteValues = ["LIVING", "YES"]
#otherCols = ['15-Sep', 'A2M', 'ZP1','XG','WRN','WTAP','ADAMTS15','tMDC']
givenColumn = 'c'
discreteQueries = ["Age", "Status"]
discreteValues = ["old", "living"]
otherCols = ['a', 'd','e', 'g']
otherCols.extend(discreteQueries)

otherCols.insert(0, givenColumn)
otherCols.insert(0, "Sample")
df = pd.read_parquet(parquetFilePath,columns=otherCols)


df.set_index("Sample", drop=True,inplace=True)
del otherCols[0]
step0 = time.time()
print("Elapsed time for pd.read_parquet: {} seconds".format(step0-start),flush=True)

#query all numeric data
df = df.loc[df[givenColumn]>2 , [ col for col in otherCols]]
#now perform discrete queries from the metadata 
for i in range(0,len(discreteQueries)):
	df = df.loc[df[discreteQueries[i]]==discreteValues[i], [col for col in otherCols]]

#colData2 = colData.loc[colData[discreteQuery]==discreteValue, [col for col in otherCols]]
#colData3 = colData2.loc[colData2[discreteQuery2]==discreteValue2, [col for col in otherCols]]
step3 = time.time()
print("Elapsed time for querying pandas dataframe: {} seconds.".format(step3-step0),flush=True)
#write to file
df.to_csv(path_or_buf = 'out.txt', sep='\t')
step4= time.time()
print("Elapsed time for printing results to output file: {} seconds".format(step4-step3), flush=True)
# See descriptions of additional tests to write in NOTES.
