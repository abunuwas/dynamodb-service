from .setupDynamoDB import getDynamoDBConnection, createDevicesTable

class ConnectionManager:

    def __init__(self, mode=None, config=None, endpoint_url=None, port=None, use_instance_metadata=False):
        self.db = None
        self.camerasTable = None

        if mode == "local":
            if config is not None:
                raise Exception('Cannot specify config when in local mode')
            if endpoint_url is None:
                endpoint_url = 'localhost'
            if port is None:
                port = 8000
            self.db = getDynamoDBConnection(endpoint_url=endpoint_url, port=port, local=True)
        elif mode == "service":
            self.db = getDynamoDBConnection(config=config, endpoint_url=endpoint_url, use_instance_metadata=use_instance_metadata)
        else:
            raise Exception("Invalid arguments, please refer to usage.");

        self.setupDevicesTable()

    def setupDevicesTable(self):
        try:
            self.camerasTable = self.db.Table("Cameras")
        except Exception as e:
            raise e("There was an issue trying to retrieve the Cameras table.")

    def getDevicesTable(self):
        if self.camerasTable == None:
            self.setupCamerasTable()
        return self.camerasTable

    def createDevicesTable(self):
        self.camerasTable = createDevicesTable(self.db)