import botocore
import boto3

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
import json

def getDynamoDBConnection(config=None, region_name=None, endpoint_url=None, port=None, local=False, use_instance_metadata=False):
    if local:
    	endpoint_url = 'http://{}:{}'.format(endpoint_url, port)
    	db = boto3.resource(
        	'dynamodb',
        	region_name=region_name,
        	endpoint_url=endpoint_url,
            aws_secret_access_key='ticTacToeSampleApp',
            aws_access_key_id='ticTacToeSampleApp'
            )
    else:
        params = {
            'is_secure': True
            }

        # Read from config file, if provided
        if config is not None:
            if config.has_option('dynamodb', 'region'):
                params['region'] = config.get('dynamodb', 'region')
            if config.has_option('dynamodb', 'endpoint'):
                params['endpoint_url'] = config.get('dynamodb', 'endpoint_url')

            if config.has_option('dynamodb', 'aws_access_key_id'):
                params['aws_access_key_id'] = config.get('dynamodb', 'aws_access_key_id')
                params['aws_secret_access_key'] = config.get('dynamodb', 'aws_secret_access_key')

        # Use the endpoint specified on the command-line to trump the config file
        if endpoint is not None:
            params['endpoint_url'] = endpoint_url
            if 'region' in params:
                del params['region']

        # Only auto-detect the DynamoDB endpoint if the endpoint was not specified through other config
        if 'endpoint_url' not in params and use_instance_metadata:
            response = urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document').read()
            doc = json.loads(response);
            params['endpoint_url'] = 'dynamodb.%s.amazonaws.com' % (doc['region'])
            if 'region' in params:
                del params['region']

        db = boto3.resource('dynamodb', **params)
    return db

def createTable(table_name, key_schema, attribute_definitions, provisioned_throughput, global_secondary_indexes=None, local_secondary_indexes=None, stream_specification=None):
	try:
	    table = db.create_table(
	    			TableName=table_name,
	    			KeySchema=key_schema,
	                AttributeDefinitions=attribute_definitions,
	                ProvisionedThroughput=provisioned_throughput,
	                GlobalSecondaryIndexes=global_secondary_indexes,
	                )
	except Exception as e:
		print('The following exception was found {}'.format(repr(e)))

def createSecondaryIndexes():
    try:
        hostStatusDate = GlobalAllIndex("HostId-StatusDate-index",
                                        parts=[HashKey("HostId"), RangeKey("StatusDate")],
                                        throughput={
                                            'read': 1,
                                            'write': 1
                                        })
        opponentStatusDate  = GlobalAllIndex("OpponentId-StatusDate-index",
                                        parts=[HashKey("OpponentId"), RangeKey("StatusDate")],
                                        throughput={
                                            'read': 1,
                                            'write': 1
                                        })

        #global secondary indexes
        GSI = [hostStatusDate, opponentStatusDate]
    except:
    	pass
    finally:
    	pass

def destroyTable(db, table_name):
	table = db.table(table_name)
	table.delete()
	return True

#parse command line args for credentials and such
#for now just assume local is when args are empty
