import inspect
import json
import os
import boto3
import decimal

import models
from models_package.models import Model, Throughput, Key

def model_constructor(obj):
	# CONSIDER CASE IN WHICH TABLE ALREADY EXISTS AND ONLY HAS TO BE UPDATED!!!! THIS IS HUGELY IMPORTANT!!

	# For some of the attributes defined in the table we might need additional checks, which might 
	# justify moving the logic into a TableModel class to build it. 
	table_attributes = ['TableName', 'KeySchema', 'AttributeDefinitions', 'ProvisionedThroughput', 'GlobalSecondaryIndexes', 
						'LocalSecondaryIndexes', 'StreamSpecification']
	table_model = {
					'TableName': None,
					'KeySchema': [],
					'AttributeDefinitions': [],
					'ProvisionedThroughput': None
				}

	model = obj.__call__() # obj() also works, but I think this syntax is more explicit. 
	attributes = model.get_attributes()
	table_name = attributes.get('table_name', model.__class__)
	provisioned_throughput = attributes.get('provisioned_throughput', Throughput(read=5, write=5))
	keys = [(key, value) for key, value in attributes.items() if isinstance(value, Key)]
	# Keys must be listed in order when creating a table in DynamoDB. Hash key must come first, 
	# and RANGE key second, if any. The sorting method used here is not very good, since it
	# relies on alphabetical order on the one hand, and on the other hand the key referrencing 
	# method is very specific to the API implemented in this framework, which makes it prone to
	# code breaks with small changes. Improve this later. 
	keys = sorted(keys, key=lambda key: key[1].get_values().KeySchema['KeyType'])

	table_model['TableName'] = table_name
	table_model['ProvisionedThroughput'] = provisioned_throughput.get_values()

	for key, value in keys:
		values = value.get_values()

		schema = values.KeySchema
		definitions = values.AttributeDefinitions

		attr_name = schema.get('AttrbuteName', key)

		schema['AttributeName'] = attr_name
		definitions['AttributeName'] = attr_name

		table_model['KeySchema'].append(schema)
		table_model['AttributeDefinitions'].append(definitions)

	return table_model


def migrate():
	# The database connection must be built from the settings.py file, not like this
	dynamodb = boto3.resource('dynamodb', region_name='eu-west-1', 
		endpoint_url='http://localhost:8000')
	with open(os.path.join('migration_logs', 'migration.json')) as json_file:
		tables = json.load(json_file, parse_float=decimal.Decimal)
		for table in tables:
			response = dynamodb.create_table(**table)
			print(response)


def makemigrations():
	class_models = []
	table_models = []
	for name, obj in inspect.getmembers(models, inspect.isclass):
		# HORROR!!!!!!!!!!! CHANGE THIS ASAP!!!!!!!!!!
		if 'models_package' not in repr(obj):
			class_models.append(obj)

	for obj in class_models:
		table_model = model_constructor(obj)
		table_models.append(table_model)

	with open(os.path.join('migration_logs', 'migration.json'), 'w') as json_file:
		json.dump(table_models, json_file)
	print('Migration file completed')
	# log()


if __name__ == '__main__':
	import sys

	valid_commands = {'migrate': migrate, 'makemigrations': makemigrations}

	try:
		command = sys.argv[1]
	except IndexError:
		print("Please provide a command")
		sys.exit()

	try:
		valid_commands[command].__call__()
	except KeyError as e:
		print('Please provide a valid command, ', e)
