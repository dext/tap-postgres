import singer
from singer import utils

import psycopg2
import psycopg2.extras


class Postgres:
    __instance = None

    @staticmethod
    def get_instance():
        if Postgres.__instance is None:
            Postgres()

        return Postgres.__instance

    @staticmethod
    def get_configuration(logical_replication):
        args = utils.parse_args({})

        configuration = {
            "host": args.config["host"],
            "dbname": args.config["dbname"],
            "user": args.config["user"],
            "password": args.config["password"],
            "port": args.config["port"],
            "connect_timeout": 30,
            "connection_factory": psycopg2.extras.LogicalReplicationConnection,
        }

        if args.config.get("sslmode"):
            configuration["sslmode"] = args.config["sslmode"]

        if logical_replication:
            configuration["connection_factory"] = psycopg2.extras.LogicalReplicationConnection

        return configuration


    def __init__(self):
        if Postgres.__instance is not None:
            raise Exception("This class is a singleton!")

        Postgres.__instance = self
        self.connections =  {"logical": None, "transactional": None}

    def connect(self, logical_replication=False):
        connection_type = "logical" if logical_replication else "transactional"

        if not self.connections[connection_type] or self.connections[connection_type].closed:
            config = Postgres.get_configuration(logical_replication)
            self.connections[connection_type] = psycopg2.connect(**config)

        return self.connections[connection_type]
