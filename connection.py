import boto3


def getDatabase(engine, endpoint, port, region):
	try:
		endpoint_url = 'http://{}:{}'.format(endpoint, port)
		dynamodb = boto3.resource(engine, endpoint_url=endpoint_url, region_name=region)	
		return dynamodb
	except Exception as e:
		print('The following Exception was raised!! ', repr(e))
		
def getClient(engine, endpoint, port, region):
	try:
		endpoint_url = 'http://{}:{}'.format(endpoint, port)
		client = boto3.client(engine, endpoint_url=endpoint_url, region_name=region)	
		return client
	except Exception as e:
		print('The following Exception was raised!! ', repr(e))