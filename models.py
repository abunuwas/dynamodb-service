from collections import namedtuple
import boto3
import decimal
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key as ConditionKey, Attr
import sys
import inspect
import os

from .exceptions import WrongKeyTypeError, WrongAttributeTypeError, FieldError, HashRangeKeyError

dynamodb = boto3.resource('dynamodb', region_name='eu-west-1', 
	endpoint_url="http://localhost:8000")

# Helper class to convert DynamoDB item to JSON
class DecimalEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, decimal.Decimal):
			if o % 1 > 0:
				return float(o)
			else:
				return int(o)
			return super(DecimalEncoder, self).default(o)

class TableModel:
	def __init__(self, table_name=None, keys=None, provisioned_throughput=None):
		self._table_name = table_name
		self._keys = keys
		self._provisioned_throughput = provisioned_throughput

	@property
	def table_name(self):
	    return self._table_name


class Model(object):
	def __init__(self, **params):
		self.params = params
		self.parameters = {}
		self.table_name = self.get_table_name()
		self.table = self.get_table()

	def __repr__(self):
		return '<Model object: {}>'.format(self.get_attributes())

	@classmethod 
	def get_attributes(cls):
		return  cls.__dict__	

	@classmethod
	def get_required_items(cls):
		attributes = cls.get_attributes()
		required_items = [key for key, value in attributes.items() if isinstance(value, Key)]
		return required_items

	@classmethod
	def get_table_name(cls):
		return cls.get_attributes().get('table_name', cls.__name__)

	@classmethod
	def create_table(cls):
		# CONSIDER CASE IN WHICH TABLE ALREADY EXISTS AND ONLY HAS TO BE UPDATED!!!! 

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

		attributes = cls.get_attributes()
		table_name = cls.get_table_name()
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

		with open(os.path.join('migration_logs', 'migration.json'), 'w') as json_file:
			json.dump(table_model, json_file, indent=4)
		print('Migration file completed')

		response = dynamodb.create_table(**table_model)

		return response

	@classmethod
	def get_table(cls):
		table_name = cls.get_table_name()
		resource_identifier = dynamodb.Table(table_name)
		try:
			resource_identifier.table_status
		except ClientError:
			cls.create_table()
			resource_identifier = dynamodb.Table(table_name)
			#print('A Client Error was raised')
			#sys.exit()
		return resource_identifier

	def build_params(self, item, required_items, params):
		for i in required_items:
			item['Item'][i] = params[i]
		return item

	def create(self, **params):
		# I think that there should also be the possibility of calling this method as a class method,
		# like Class.create(hash_key=HASH_KEY, range_key=RANGE_KEY)
		# Should we have perhaps an objects manager to deal with these things? 
		# The object manager would build an instance of the class to call all of these especial methods...
		fields = ['ConditionExpression', 'ExpressionAttributeNames', 'ExpressionAttributeValues',
					'Item', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics', 'ReturnValues', 'TableName']
		required_items = self.get_required_items()
		item = {
			'Item': {}
		}
		if params:
			item = self.build_params(item, required_items, params)
		else:
			item = self.build_params(item, required_items, self.params)
		#print(item)
		# Wrap next line in ClientError try-except for possible error in the use of data types.
		#print(self.table)
		response = self.table.put_item(**item)
		return json.dumps(response, indent=4, cls=DecimalEncoder)
		#self.table.put_item(self.item)

	def save(self):
		# Perhaps at some point this might be a useful method...
		pass 

	@classmethod
	def get(cls, **params):
		fields = ['Key', 'TableName', 'ConsistentRead', 'ExpressionAttributeNames', 'ProjectionExpression',
					'ReturnConsumedCapacity']
		
		required_items = cls.get_required_items()
		for item in required_items:
			if item not in params.keys():
				# Raise a more helpful exception
				raise Exception 
		table = cls.get_table()

		item = {
			'Key': {}
		}
		#Before doing this loop, it must be first checked that the required items are in params
		for i in required_items:
			item['Key'][i] = params[i]
		response = table.get_item(**item)
		return json.dumps(response, indent=4, cls=DecimalEncoder)
		
	@classmethod
	def query(cls, **params):
		fields = ['TableName', 'ConsistentRead', 'ExclusiveStartKey', 'ExpressionAttributeNames', 'ExpressionAttributeValues',
					'FilterExpression', 'IndexName', 'KeyConditionExpression', 'Limit', 'ProjectionExpression',
					'ReturnConsumedCapacity', 'ScanIndexForward', 'Select']

		query = {
			'KeyConditionExpression': '',
			'ExpressionAttributeNames': {},
			'ExpressionAttributeValues': {}
		}

		required_items = cls.get_required_items()
		for i in required_items:
			try:
				attr_name = '#'+i
				attr_value = ':'+i
				query['ExpressionAttributeValues'][attr_value] = params[i]
				query['ExpressionAttributeNames'][attr_name] = i
				query['KeyConditionExpression'] += attr_name+'='+attr_value
			except KeyError:
				continue

		table = cls.get_table()
		response = table.query(**query)

		return json.dumps(response, indent=4, cls=DecimalEncoder)

	def scan(self, **params):
		pass

	def update(self):
		pass

	def delete(self):
		pass
	

class Key:
	def __init__(self, name=None, key_type=None, attr_type=None):
		self._name = name
		self._key_type = key_type
		self._attr_type = attr_type

	@property
	def name(self):
		return self._name

	@property
	def key_type(self):
		if self._key_type in ['hash', 'range']:
			return self._key_type.upper()
		else:
			raise WrongKeyTypeError(_key_type, '{} is not a valid key type. Valid types are hash and range.'.format(_key_type))

	@property
	def attr_type(self):
		if self._attr_type in ['N', 'S']: # Make sure there are no more types; I thnink there are! 
			return self._attr_type
		else:
			raise WrongAttributeTypeError(_attr_type, '{} is not a valid attribute type. Valid types are N and S'.format(_attr_type))

	def get_values(self):
		Attributes = namedtuple('Attributes', ['KeySchema', 'AttributeDefinitions'])
		schema = {
					'AttributeName': self.name,
					'KeyType': self.key_type
				}
		definitions = {
						'AttributeName': self.name,
						'AttributeType': self.attr_type
						}

		attributes = Attributes(schema, definitions)

		return attributes

class Throughput:
	### INCLUDE CHECKS!!
	def __init__(self, read=None, write=None):
		self.read = read
		self.write = write

	def get_values(self):
		return {
					'ReadCapacityUnits': self.read,
					'WriteCapacityUnits': self.write
				}