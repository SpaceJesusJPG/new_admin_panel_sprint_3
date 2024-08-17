class Loader:
    def __init__(self, connection):
        self.connection = connection

    def filmwork2elastic(self, records):

        for fw_id in records.keys():
            self.connection.index(
                index="movies", id=fw_id, body=records[fw_id]
            )
