
def createTable(dynamodb, table_model):
	try:
		table = dynamodb.create_table(**table_model)
	except Exception as e:
		print('The following Exception was raised!! ', repr(e))
		return False
	return table

def deleteTable(dynamodb, table_name):
	try:
		table = dynamodb.Table(table_name)
		table.delete()
	except Exception as e:
		print('The following Exception was raised!! ', repr(e))
	return table

def updateTable(dynamodb, table_name, updates):
	pass

