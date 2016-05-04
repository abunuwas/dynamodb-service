# Copyright statement
# License statement

"""
:module: pynamodb.models
:author: Jose Antonio Haro Peralta <JHaroPeralta@intamac.com>
:author: ...
"""

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

# This dynamodb connection object defined here ad hoc will be removed in the final version. 
# DynamoDB connection objects should be built from config files or user defined parameters. 
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

# Not sure yet if a TableModel will be really needed or not. 
class TableModel:
	def __init__(self, table_name=None, keys=None, provisioned_throughput=None):
		self._table_name = table_name
		self._keys = keys
		self._provisioned_throughput = provisioned_throughput

	@property
	def table_name(self):
	    return self._table_name


class Model(object):
	"""
	This class provides all the necessary functionality to define a
	data model for DynamoDB and perform database operations. 
	"""
	def __init__(self, **params):
		"""
		Class constructor. Parameters can optionally be passed into it. 
		"""
		self.params = params
		self.parameters = {}
		self.table_name = self.get_table_name()
		self.table = self.get_table()

	def __repr__(self):
		return '<Model object: {}>'.format(self.get_attributes())

	@classmethod 
	def get_attributes(cls):
		"""
		Class method intended for introspection of attributes defined at class
		level, usually attributes of the data model. Returns a dictionary mapping
		class attributes with their values.
		"""
		return  cls.__dict__	

	@classmethod
	def get_required_items(cls):
		"""
		Class method that returns a list of the names of the variables that define the index
		of a DynamoDB table. 
		"""
		attributes = cls.get_attributes()
		required_items = [key for key, value in attributes.items() if isinstance(value, Key)]
		return required_items

	@classmethod
	def get_table_name(cls):
		"""
		Class method to obtain the name of the DynamoDB table. The table name can be
		explicitly defined by the user in the data model definition of the table, by 
		setting a variable `table_name` as a class attribute. If such attribute is 
		missing, this method assumes that the table name corresponds to the class name.
		"""
		return cls.get_attributes().get('table_name', cls.__name__)

	@classmethod
	def create_table(cls):
		"""
		Class method that creates a DynamoDB table based on the attribute definitions
		specified by the user in the data model. 
		"""
		# CONSIDER CASE IN WHICH TABLE ALREADY EXISTS AND ONLY HAS TO BE UPDATED!!!! 

		# For some of the attributes defined in the table we might need additional checks, which might 
		# justify moving the logic into a TableModel class to build it. 

		# Allowed parameters for the creation of a table in the AWS API. 
		table_attributes = ['TableName', 'KeySchema', 'AttributeDefinitions', 'ProvisionedThroughput', 'GlobalSecondaryIndexes', 
		'LocalSecondaryIndexes', 'StreamSpecification']

		# Dictionary setting initial values for the minimum required parameters in the creation of a table. 
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

		# Keep a migration JSON file log of the table operation performed. 
		# Check first that there're no name conflicts with the file names!! 
		with open(os.path.join('migration_logs', 'migration.json'), 'w') as json_file:
			json.dump(table_model, json_file, indent=4)
		print('Migration file completed')

		# TODO: build a useful response object, not just a map as returned by the SDK. 
		response = dynamodb.create_table(**table_model)

		return response

	@classmethod
	def get_table(cls):
		"""
		Class method that returns an object representation of the DynamoDB table corresponding
		with the class upon which it is invoked. The method provided by the Python SDK actually
		returns an abstract resource identifier, which does not necessarily correspond with an
		existing table. For this reason, the present method performs a check to verify that the table
		exists by trying to access one of its attributes. If this operation throws an error, it is
		assumed that the table does not exist and a new table is created. 
		"""
		table_name = cls.get_table_name()
		resource_identifier = dynamodb.Table(table_name)
		try:
			resource_identifier.table_status
		except ClientError:
			cls.create_table()
			resource_identifier = dynamodb.Table(table_name)
		return resource_identifier

	def build_params(self, item, required_items, params):
		"""
		Helper method to build the parameters of an item insertion request in DynamoDB. 

		:param item:
			A container object where the parameters will be stored.
		:type item:
			`dict`. 
		:param required_items:
			A list of items that must be present in the item container.
		:type item:
			`list`. 
		"""
		for i in required_items:
			item['Item'][i] = params[i]
		return item

	def create(self, **params):
		"""
		Object method to perform an item insertation operation in a DynamoDB table. 

		:param params:
			A map of values to be used in the item insertion operation. 
		:type params:
			`dict`. 
		"""

		# I think that there should also be the possibility of calling this method as a class method,
		# like Class.create(hash_key=HASH_KEY, range_key=RANGE_KEY)
		# Should we have perhaps an objects manager to deal with these things? 
		# The object manager would build an instance of the class to call all of these especial methods...

		# List of allowed parameters that can be used in an item insertion operation in a DynamoDB table. 
		fields = ['ConditionExpression', 'ExpressionAttributeNames', 'ExpressionAttributeValues',
					'Item', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics', 'ReturnValues', 'TableName']
		required_items = self.get_required_items()

		# Dictionary setting initial values for the minimum required parameters in an item insertion operation. 
		item = {
			'Item': {}
		}

		# Build the request from parameters passed directly to this method... 
		if params:
			item = self.build_params(item, required_items, params)
		# ...or from parameters passed into the class constructor method. 
		else:
			item = self.build_params(item, required_items, self.params)
		# Wrap next line in ClientError try-except for possible error in the use of data types.
		response = self.table.put_item(**item)
		# Return a JSON representation of the response returned by this transaction. 
		# TODO: build a useful object representation from this map. 
		return json.dumps(response, indent=4, cls=DecimalEncoder)
		#self.table.put_item(self.item)

	def save(self):
		# Perhaps at some point this might be a useful method...
		pass 

	@classmethod
	def get(cls, **params):
		"""
		Class method for querying individual items in a DynamoDB table. 

		:param params:
			A map of values to be used in the query operation. 
		:type params:
			`dict`. 
		"""

		# List of allowed parameters that can be used in an item query operation in a DynamoDB table. 
		fields = ['Key', 'TableName', 'ConsistentRead', 'ExpressionAttributeNames', 'ProjectionExpression',
					'ReturnConsumedCapacity']
		
		required_items = cls.get_required_items()
		for item in required_items:
			if item not in params.keys():
				# Raise a more helpful exception
				raise Exception 
		table = cls.get_table()

		# Dictionary setting initial values for the minimum required parameters in an item query operation. 
		item = {
			'Key': {}
		}
		#Before doing this loop, it must be first checked that the required items are in params
		for i in required_items:
			item['Key'][i] = params[i]
		response = table.get_item(**item)

		# Return a JSON representation of the response returned by this transaction. 
		# TODO: build a useful object representation from this map. 
		return json.dumps(response, indent=4, cls=DecimalEncoder)
		
	@classmethod
	def query(cls, **params):
		"""
		Class method that implements a layer of abstraction on the bare wrapper for the query operator 
		of DynamoDB provided by the Python SDK. 

		:param params:
			A map of values to be used in the query operation. 
		:type params:
			`dict`. 
		"""

		# List of allowed parameters that can be used in a query transaction in a DynamoDB table. 
		fields = ['TableName', 'ConsistentRead', 'ExclusiveStartKey', 'ExpressionAttributeNames', 'ExpressionAttributeValues',
					'FilterExpression', 'IndexName', 'KeyConditionExpression', 'Limit', 'ProjectionExpression',
					'ReturnConsumedCapacity', 'ScanIndexForward', 'Select']

		# Dictionary setting initial values for the minimum required parameters in a query transaction. 
		query = {
			'KeyConditionExpression': '',
			'ExpressionAttributeNames': {},
			'ExpressionAttributeValues': {}
		}

		required_items = cls.get_required_items()

		# TODO:	implement support for all the possible logical comparisons that can be performed on the 
		# condition expressions for the table keys. To do this the interface that will be used to 
		# reference the logical operators must be first defined. In the code below there's support only
		# for the equality operator. 
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
		"""
		Class method that implements a layer of abstraction on the bare wrapper for the scan operator 
		of DynamoDB provided by the Python SDK. 

		:param params:
			A map of values to be used in the query operation. 
		:type params:
			`dict`. 
		"""

		# List of allowed parameters that can be used in a scan transaction in a DynamoDB table. 
		fields = []

	def update(self):
		pass

	def delete(self):
		pass
	

class Key:
	"""
	This class builds an object representation of a key parameter in the creation of an index for a
	DynamoDB table. 
	"""

	def __init__(self, , key_type, attr_type, name=None):
		"""
		Class constructor. The values fetched within this scope are aimed for internal use, therefore 
		their trailing slash. The intended public interface for these attributes is defined below
		with property decorators. 

		:param key_type:
			Type of the DynamoDB index key. Only two values are allowed: `hash` or `range`.
		:type key_type:
			`str`.
		:param attr_type:
			Data type of the key. Only one of the following values is allowed: `S | N | B`. 
		:type attr_type:
			`str`.
		:para name:
			Name of the DynamoDB index key. 
		:type name:
			`str`. 
		"""
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
		if self._attr_type in ['N', 'S', 'B']: # Make sure there are no more types. 
			return self._attr_type
		else:
			raise WrongAttributeTypeError(_attr_type, '{} is not a valid attribute type. Valid types are N and S'.format(_attr_type))

	def get_values(self):
		"""
		This method builds a named tuple representation of the definition of the DynamoDb talbe index. 
		A named tuple is preferred over mere map of values since it allows for a more convenient way 
		of accessing this data with namespacing instead of using heavly nested index notation. 
		"""
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
		"""
		This class builds an object representation of the throughput capacity of a DynamoDB table for its creation. 
		###TODO: define default values!

		:param read:
			Read throughput capacity. 
		:type read:
			`int`.
		:param write:
			Write throughput capacity. 
		:type write:
			`int`.
		"""
	### INCLUDE CHECKS!!
	def __init__(self, read=None, write=None):
		self.read = read
		self.write = write

	def get_values(self):
		"""
		This method returns a dictionary with the required format for the definition of throughput capacity
		in a table creation operation in DynamoDB. 
		"""
		return {
					'ReadCapacityUnits': self.read,
					'WriteCapacityUnits': self.write
				}