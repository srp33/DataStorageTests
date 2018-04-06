import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from ColumnInfo import ColumnInfo
import sys

def peek(parquetFilePath, numRows=10, numCols=10):
	#too slow right now,
	df = pd.read_parquet(parquetFilePath)
	df.set_index("Sample", drop=True, inplace=True)
	df=df.iloc[0:numRows, 0:numCols]
	return df

def peekByColumnNames(parquetFilePath, listOfColumnNames,numRows=10):
	listOfColumnNames.insert(0,"Sample")
	df = pd.read_parquet(parquetFilePath, columns=listOfColumnNames)
	df.set_index("Sample", drop=True, inplace=True)
	df=df[0:numRows]
	return df

def getColumnNames(parquetFilePath):
	return

def getColumnInfo(parquetFilePath, columnName):
	columnList = [columnName]
	df = pd.read_parquet(parquetFilePath, columns=columnList)
	uniqueValues = set()
	for index, row in df.iterrows():
		uniqueValues.add(row[columnName])
	uniqueValues = list(uniqueValues)
	if isinstance(uniqueValues[0],str):
		return ColumnInfo(columnName,"discrete", uniqueValues)
	else:
		return ColumnInfo(columnName, "continuous", uniqueValues)
