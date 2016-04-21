import unittest
from unittest import TestCase

import boto3
import os
import json
import psutil

from databaseSetup import createTable, deleteTable
from connection import getDatabase, getClient
from crud import insertItem


def check_port(port):
	for conn in psutil.net_connections():
		if port in conn.laddr:
			return True
	else:
		return False

def delete_test_tables(dynamodb, client):
	test_tables = []
	for metadata, tables in client.list_tables().items():
		test_tables = (table for table in tables if 'Test' in table)
	for table in test_tables:
		t = dynamodb.Table(table)
		t.delete()

table_model = {
			'TableName': 'TestTable',
			'KeySchema': [
				{
					'AttributeName': 'year',
					'KeyType': 'HASH' # Partition key
				},
				{
					'AttributeName': 'title',
					'KeyType': 'RANGE' # Sort key
				}
			],
			'AttributeDefinitions': [
				{
					'AttributeName': 'year',
					'AttributeType': 'N'
				},
				{
					'AttributeName': 'title',
					'AttributeType': 'S'
				}
			],
			'ProvisionedThroughput': {
				'ReadCapacityUnits': 10,
				'WriteCapacityUnits': 10
			}
		}


class TestDynamoDBLowLevel(TestCase):

	def setUp(self):
		#if not check_port(8000):
		#	os.system('java -Djava.library.path=../Dynamodb/DynamoDBLocal_lib -jar ../Dynamodb/DynamoDBLocal.jar')
		self.dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000', region_name='eu-west-1')	
		self.client = boto3.client('dynamodb', endpoint_url='http://localhost:8000', region_name='eu-west-1')	

	def tearDown(self):
		connection = [conn for conn in psutil.net_connections() if 8000 in conn.laddr][0]
		process = psutil.Process(connection.pid)
		#process.terminate()

		delete_test_tables(self.dynamodb, self.client)

	def test_create_delete_table(self):
		table = createTable(self.dynamodb, table_model)

		if table:
			print(table.table_status)
			response = deleteTable(self.dynamodb, table_model['TableName'])
			self.assertEqual(table.table_status, 'ACTIVE')
		else:
			raise Exception('Table could not be created!')

	def test_update_table(self):
		pass

class TestCRUD(TestCase):

	def setUp(self):
		self.dynamodb = getDatabase(engine='dynamodb', endpoint='localhost', port='8000', region='eu-west-1')
		self.client = getClient(engine='dynamodb', endpoint='localhost', port='8000', region='eu-west-1')
		createTable(self.dynamodb, table_model)
		self.table = self.dynamodb.Table(table_model['TableName'])
		if self.table:
			self.assertEqual(self.table.table_status, 'ACTIVE')
		else:
			raise Exception('Table could not be created!')

	def tearDown(self):
		self.table.delete()
		delete_test_tables(self.dynamodb, self.client)

	def test_insert_item(self):
		item = {
				'Item': {
					'year': 12016,
					'title': 'From the future',
					'info': {
						'plot': 'Something happens in the future.'
						}
					}
			}
		response = insertItem(self.table, item)
		response = json.loads(response)
		response_status = response['ResponseMetadata']['HTTPStatusCode']
		self.assertEqual(response_status, 200)

	def test_insert_items_many(self):
		with open('moviedata.json') as json_file:
			data = json.load(json_file)
			response = insertItem(self.table, item, many=True)
			print(response)

	def test_get_item(self):
		pass

	def test_get_items_many(self):
		pass

	def test_update_item(self):
		pass

	def test_update_items_many(self):
		pass

	def test_delete_item(self):
		pass

	def test_delete_item_many(self):
		pass 


if __name__ == '__main__':
	unittest.main(warnings='ignore')


