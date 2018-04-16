class DiscreteQuery:
	'Data object that holds information about a query to be made on a parquet file'

	def __init__(self, columnName:str, value:str):
		self.columnName = columnName
		self.value = value
