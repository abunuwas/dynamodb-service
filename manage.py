import inspect

import models
from models_package.models import Model, Throughput, Key

def model_constructor(obj):
	table_model = {
					'TableName': None,
					'KeySchema': [
						None,
						None
					],
					'AttributeDefinitions': [
						{
							None,
							None
						}
					],
					'ProvisionedThroughput': None
	}

	model = obj.__call__()
	attributes = model.__class__.__dict__
	table_name = attributes.get('table_name', model.__class__)
	provisioned_throughput = attributes.get('privisioned_throughput', Throughput(read=5, write=5))
	keys = [(key, value) for key, value in attributes.items() if isinstance(value, Key)]

	table_model['TableName'] = table_name
	table_model['ProvisionedThroughput'] = provisioned_throughput.get_values()

	for key, value in keys:
		values = value.get_values()

		print(key, values)




def migrate():
	pass




def makemigrations():
	defined_models = []
	for name, obj in inspect.getmembers(models, inspect.isclass):
		# HORROR!!!!!!!!!!! CHANGE THIS ASAP!!!!!!!!!!
		if 'models_package' not in repr(obj):
			defined_models.append(obj)

	for obj in defined_models:
		model_constructor(obj)

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
	except KeyError:
		print('Please provide a valid command')
