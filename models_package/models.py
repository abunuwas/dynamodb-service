from collections import namedtuple
import boto3

from .exceptions import WrongKeyTypeError, WrongAttributeTypeError, FieldError, HashRangeKeyError

dynamodb = boto3.resource('dynamodb', region_name='eu-west-1', 
	endpoint_url="http://localhost:8000")

class TableModel:
	def __init__(self, table_name=None, keys=None, provisioned_throughput=None):
		self._table_name = table_name
		self._keys = keys
		self._provisioned_throughput = provisioned_throughput

	@property
	def table_name(self):
	    return self._table_name

class Model(object):
	def __init__(self, params=None):
		self.parameters = {}
		self.fields = ['ConditionExpression', 'ExpressionAttributeNames', 'ExpressionAttributeValues',
				'Item', 'ReturnConsumedCapacity', 'ReturnItemCollectionMetrics', 'ReturnValues', 'TableName']
		self.table_name = self.get_table_name()
		self.table = dynamodb.Table(self.table_name)


	@classmethod 
	def get_attributes(cls):
		return  cls.__dict__

	def get_table_name(self):
		return self.get_attributes().get('table_name', self.__class__)

	def get_required_items(self):
		attributes = self.get_attributes()
		required_items = [key for key, value in attributes.items() if isinstance(value, Key)]
		return required_items

	def create(self, **params):
		required_items = self.get_required_items()
		fields = [key for key, value in params]
		# Modify the following line to account for cases in which we only have a hash key!
		hash_key, range_key = self.get_required_items()
		if hash_key and range_key not in self.fields:
			raise HashRangeKeyError('Either a hash or a range key is missing from your arguments. Your table keys are: {}'.foramt(
				self.get_required_items()))
		for key, value in params:
			if key not in self.fields:
				raise FieldError(key, '{} is not a valid field for an item.'format(key))
			else:

		self.table.put_item(self.item)

	def get(self, **params):
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