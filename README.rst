PynamoDB
========
PynamoDB is a layer of abstraction over the Python SDK wrapper for AWS DynamoDB. 
Following an ORM design, it allows for the definition of data models for DynamoDB
and to perform all database transactions using a class-based interface. By doing this
it also leverages the power and convenicen of OOP when incorporating DynamoDB as a
database backend into a project. The design philosophy followed in this wrapper is
to hide as much complexity and implementation details as possible from the end user
of this library. 

Example of model definition
---------------------------
The following lines of code define a data model for a DynamoDB table:

.. code=block:: python

	from pynamodb.models import Model, Key, Throughput

	class Camera(Model):
		table_name = 'Camera'
		year = Key(name='year', key_type='hash', attr_type='N')
		title = Key(key_type='range', attr_type='N')
		provisioned_throughput = Throughput(read=10, write=10)

To create a table, it is enough to instantiate the class and start performing
database transactions with it. PynamoDB will the information available from within
the class definition to obtain an object representation of the table corresponding
with this class. If a table is not found, PynamoDB creates it automatically. 

Alternatively, migrations can be performed manually from the command line with the 
following commands:


.. code=block:: python
    
    python3 manage.py makemigrations
    python3 manage.py migrate

The first command will create a JSON migrations file and save it in a migrations_log
directory. The second command will apply the migrations defiend in that file on the
corresponding tables. 