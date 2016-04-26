from .exceptions import WrongKeyTypeError, WrongAttributeTypeError

class TableModel:
	def __init__(self, table_name=None, keys=None, provisioned_throughput=None):
		self._table_name = table_name
		self._keys = keys
		self._provisioned_throughput = provisioned_throughput

	@property
	def table_name(self):
	    return self._table_name

class Model(object):
	def __init__(self):
		self.user_defined_model = True

	def get_attributes(self):
		attributes = self.__class__.__dict__
		return attributes

	def create(self):
		pass

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
			return self._key_type
		else:
			raise WrongKeyTypeError(_key_type, '{} is not a valid key type. Valid types are hash and range.'.format(_key_type))

	@property
	def attr_type(self):
		if self._attr_type in ['N', 'S']: # Make sure there are no more types; I thnink there are! 
			return self._attr_type
		else:
			raise WrongAttributeTypeError(_attr_type, '{} is not a valid attribute type. Valid types are N and S'.format(_attr_type))

	def get_values(self):
		schema = {
					'AttributeName': self.name,
					'KeyType': self.key_type
				}
		definitions = {
						'AttributeName': self.name,
						'AttributeType': self.attr_type
						}

		return schema, definitions

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