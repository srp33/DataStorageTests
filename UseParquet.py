import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from ColumnInfo import ColumnInfo
from ContinuousQuery import ContinuousQuery
from DiscreteQuery import DiscreteQuery
from OperatorEnum import OperatorEnum
import sys

def peek(parquetFilePath, numRows=10, numCols=10)->pd.DataFrame:
	
	allCols = getColumnNames(parquetFilePath)
	if(numCols>len(allCols)):
		numCols=len(allCols)
	selectedCols = []
	selectedCols.append("Sample")
	for i in range(0, numCols):
		selectedCols.append(allCols[i])
	df = pd.read_parquet(parquetFilePath, columns=selectedCols)
	df.set_index("Sample", drop=True, inplace=True)
	df=df.iloc[0:numRows, 0:numCols]
	return df

def columnsPyarrow(parquetFilePath, numRows=10, numCols=10):
	table= pq.read_table(parquetFilePath)
	columns = list(table.schema)
	return columns

def peekByColumnNames(parquetFilePath, listOfColumnNames,numRows=10)->pd.DataFrame:
	listOfColumnNames.insert(0,"Sample")
	df = pd.read_parquet(parquetFilePath, columns=listOfColumnNames)
	df.set_index("Sample", drop=True, inplace=True)
	df=df[0:numRows]
	return df

def getColumnNames(parquetFilePath)->list:
	"""
	Returns a list of all column names from a given parquet dataset
	"""
	
	p = pq.ParquetFile(parquetFilePath)
	columnNames = p.schema.names
	del columnNames[0]
	del columnNames[len(columnNames)-1]
	return columnNames

def getColumnInfo(parquetFilePath, columnName:str)->ColumnInfo:
	"""
	Given a parquet file and column name, returns a ColumnInfo object describing the column's name,data type (discrete/continuous), and all its unique values	
	"""
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

def query(parquetFilePath, columnList: list=[], continuousQueries: list=[], discreteQueries: list=[])->pd.DataFrame:
	"""
	Performs mulitple queries on a parquet dataset. If no queries or columns are passed, it returns the entire dataset as a pandas dataframe
	"""
	if len(columnList)==0 and len(continuousQueries)==0 and len(discreteQueries)==0:
		df = pd.read_parquet(parquetFilePath)
		df.set_index("Sample", drop=True, inplace=True)
		return df
	
	#extract all necessary columns in order to read them into pandas
	for query in continuousQueries:
		columnList.append(query.columnName)
	for query in discreteQueries:
		columnList.append(query.columnName)
	columnList.insert(0,"Sample")
	df = pd.read_parquet(parquetFilePath, columns = columnList)
	df.set_index("Sample", drop=True, inplace=True)
	del columnList[0]

	#perform continuous queries, adjusting for which operator is to be used
	for query in continuousQueries:
		if query.operator == OperatorEnum.Equals:
			df = df.loc[df[query.columnName]==query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.GreaterThan:
			df = df.loc[df[query.columnName]>query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.GreaterThanOrEqualTo:
			df = df.loc[df[query.columnName]>=query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.LessThan:
			df = df.loc[df[query.columnName]<query.value, [ col for col in columnList]]
		elif query.operator == OperatorEnum.LessThanOrEqualTo:
			df = df.loc[df[query.columnName]<=query.value, [ col for col in columnList]]
	#perform discrete queries
	for query in discreteQueries:
		df = df.loc[df[query.columnName] == query.value, [col for col in columnList]]
	
	return df




