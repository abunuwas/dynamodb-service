import json

def insertItem(table, data, many=False):
	if many==False:
		response = table.put_item(**data)
		return json.dumps(response)
	elif many==True:
		with table.batch_write as batch:
			for item in data:
				batch.put_item(Item=item)
		return batch
	else:
		raise Exception('Please provide valid parameters.')

def getItem(table, data, many=False):
	pass 

def updateItem(table, data, many=False):
	pass

def deleteItem(table, data, many=False):
	pass 