from models_package.models import Model, Key, Throughput

class Camera(Model):
	table_name = 'Camera'
	year = Key(name='year', key_type='hash', attr_type='N')
	title = Key(key_type='range', attr_type='N')
	provisioned_throughput = Throughput(read=10, write=10)


if __name__ == '__main__':
	camera = Camera()
	title = camera.title.get_values()
	print(title)
	print(camera.__dict__)
	print(vars(camera))
	year = getattr(camera, 'year')
	print(camera.get_attributes())
	print(camera.get_table_name())
	print(camera.get_required_items())

	class Dog:
		def __init__(*args):
			print(args)

	dog = Dog('height'=3, 'name'='Othelo')
	print(dog)