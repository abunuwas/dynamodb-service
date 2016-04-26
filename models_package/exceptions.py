
class Error(Exception):
	"""Base class for exceptions in this module"""
	pass

class WrongKeyTypeError(Error):
	"""
	Exception raised when the wrong definition type is provided for a key.

	Attributes:
		expression -- input expression in which the error occurred.
		message -- explanation of the error. 
	"""
	def __init__(self, expression, message):
		self.expression = expression
		self.message = message

class WrongAttributeTypeError(Error):
	"""
	Exception raised when the wrong definition type is provided for a key.

	Attributes:
		expression -- input expression in which the error occurred.
		message -- explanation of the error. 
	"""
	def __init__(self, expression, message):
		self.expression = expression
		self.message = message
