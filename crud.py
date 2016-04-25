import json
import decimal

class DecimalEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, decimal.Decimal):
			if o % 1 > 0:
				return float(o)
			else:
				return int(o)
			return super(DecimalEncoder, self).default(o)

def insertItem(table, data, many=False):
	if many==False:
		response = table.put_item(**data)
		return json.dumps(response, indent=4, cls=DecimalEncoder)
	## NEED TRY EXCEPT!!
	elif many==True:
		if data.endswith('.json'):
			# TRY EXCEPT
			with open(data) as json_file:
				# TRY EXCEPT
				data = json.load(json_file, parse_float=decimal.Decimal)
				with table.batch_writer() as batch:
					for item in data:
						# TRY EXCEPT 
						batch.put_item(Item=item)
				return dir(batch) # Change return value
		else:
			# TRY EXCEPT 
			data = json.load(json_file, parse_float=decimal.Decimal)
			with table.batch_writer() as batch:
				for item in data:
					batch.put_item(Item=item)
			return dir(batch) # Change return value 
	else:
		raise Exception('Please provide valid parameters.')

def getItem(table, key={}, projection_expression=None):
	item = table.get_item(Key=key, ProjectionExpression=projection_expression)
	return item

def query(table, params):
	valid_params = ['IndexName', 'KeyConditionExpression', 'FilterExpression', 'ProjectionExpression',
					'ExpressionAttributeNames']
	for param in params.keys():
		if param not in valid_params:
			raise Exception('{} is not a valid query para meter'.format(param))
	else:
		response = table.query(**params)

	for i in response['Items']:
		yield json.dumps(i, cls=DecimalEncoder)

	while 'LastEvaluatedKey' in response:
		resopnse = table.scan(params)
		for i in response['Items']:
			yield json.dumps(i, indent=2, cls=DecimalEncoder)

def scan(table, params):
	valid_params = ['IndexName', 'FilterExpression', 'ProjectionExpression',
					'ExpressionAttributeNames', 'TableName', 'Select']
	for param in params.keys():
		if param not in valid_params:
			raise Exception('{} is not a valid query parameter'.format(repr(param)))
	else:
		response = table.scan(**params)

	if 'Items' in response:
		for i in response['Items']:
			yield json.dumps(i, cls=DecimalEncoder)

		while 'LastEvaluatedKey' in response:
			resopnse = table.scan(params)
			for i in response['Items']:
				yield json.dumps(i, indent=2, cls=DecimalEncoder)

	else:
		print(response['Count'])

def updateItem(table, data, many=False):
	pass

def deleteItem(table, data, many=False):
	pass 