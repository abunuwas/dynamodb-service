import unittest
from unittest import TestCase

from dynamodb.connectionManager import ConnectionManager


class TestDynamoDBLowLevel(TestCase):

	def setUp(self):
		self.cm = ConnectionManager(mode='local', endpoint_url='localhost', port=8000)

	def test_create_delete_table(self):
		self.cm.setupDevicesTable()
		self.cm.getDevicesTable()
		self.cm.createDevicesTable()
		print(self.cm.db.tables.all())



if __name__ == '__main__':
	unittest.main()