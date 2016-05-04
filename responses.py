import json
import decimal


class QueryResponse:
	def __init__(self, response):
		self._raw_data = response
		metadata = response.get('ResponseMetadata', {})
		self.request_id = metadata.get('RequestId', None)
		self.count = response.get('Count', None)
		self.items = response.get('Items', None)
		self.status = metadata.get('HTTPStatusCode', None)
		self.scannedCount = response.get('ScannedCount', None)
		self.last_evaluated_key = response.get('LastEvaluatedKey', None)

	def __str__(self):
		return '<Class QueryResponse>'

	def __len__(self):
		return self.scannedCount

	def __iter__(self):
		try:
			for item in self.items:
				yield item

			while self.last_evaluated_key:
				yield item

		except Exception as e:
			print('The following exception was thrown: {}'.format(e))