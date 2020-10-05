"""
This module manages persisting data from a message into the RDS Resource.
"""

# the postgresql connection module
from psycopg2 import connect
from psycopg2 import OperationalError, DataError, IntegrityError

# project specific configuration parameters.
from .config import CONFIG

# allows for logging information
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RDS:

    def __init__(self, connect_timeout=65):
        """
        connect to the database resource.
        wait for 50 seconds before giving up on getting a connection
        """
        self.connection_parameters = {
            'host': CONFIG['rds']['host'],
            'database': CONFIG['rds']['database'],
            'user': CONFIG['rds']['user'],
            'password': CONFIG['rds']['password'],
            'connect_timeout': connect_timeout
            # keyword argument from https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        }

        logger.debug("created RDS instance %s" % self.connection_parameters)
        self.conn, self.cursor = self._connect()

    def _connect(self):
        conn = connect(**self.connection_parameters)  # should raise a OperationalError if it can't get a connection
        # Interestingly, autocommit seemed necessary for create table too.
        conn.autocommit = True
        cursor = conn.cursor()
        return conn, cursor

    def disconnect(self):
        try:
            self.conn.close()
            logger.debug(f'Disconnected from database: {self.conn}.')
        except AttributeError as e:
            # Would be surprised if this ever gets thrown.
            # An exception should be thrown well before this.
            logger.debug(f'Error closing connection objection: {repr(e)}', exc_info=True)
            raise RuntimeError

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except (OperationalError, DataError, IntegrityError) as e:
            logger.debug(f'Error during SQL execution: {repr(e)}', exc_info=True)
            self.conn.rollback()
