"""
Cubrid database backend for Django.

Requires CUBRIDdb: http://www.cubrid.org/downloads#py
"""

try:
    import CUBRIDdb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading CUBRIDdb module: %s" % e)

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.db.backends.cubrid.client import DatabaseClient
from django.db.backends.cubrid.creation import DatabaseCreation
from django.db.backends.cubrid.introspection import DatabaseIntrospection
from django.db.backends.cubrid.validation import DatabaseValidation
from django.utils.safestring import SafeString, SafeUnicode

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

class CursorWrapper(object):
    """
    A thin wrapper around CUBRID's normal curosr class.

    Based on MySQL backend's CursorWrapper class.
    """
    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, args=None):
        try:
            query = query.replace ("%s","?")
            return self.cursor.execute(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def executemany(self, query, args):
        try:
            query = query.replace ("%s","?")
            return self.cursor.executemany(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

class DatabaseFeatures(BaseDatabaseFeatures):
    interprets_empty_strings_as_nulls = True
    # TODO: Go through BaseDatabaseFeatures attributes and methods.
    # Implement differences here.

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django.db.backends.cubrid.compiler"

    def quote_name(self, name):
        if name.startswith("`") and name.endswith("`"):
            return name # Quoting once is enough.
        return "`%s`" % name

    def no_limit_value(self):
        # 2**63 - 1
        return 9223372036854775807

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute("SELECT LAST_INSERT_ID()")
        result = cursor.fetchone()
        return result[0]

    # TODO: Implement methods covering CUBRID-specific characteristics.
    # Start with methods returning NotImplementedError().


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'cubrid'
    # Operators taken from MySQL implementation.
    # TODO: Check for differences between this syntax and CUBRID's.
    operators = {
        'exact': '= %s',
        'iexact': 'LIKE %s',
        'contains': 'IN %s',
        'icontains': 'LIKE %s',
        'regex': 'LIKE %s',
        'iregex': 'LIKE %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE %s',
        'iendswith': 'LIKE %s',
        }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.server_version = None
        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    def _valid_connection(self):
        if self.connection is not None:
            try:
                self.connection.commit()
                return True
            except DatabaseError:
                self.connection.close()
                self.connection = None
        return False

    def _cursor(self):
        if not self._valid_connection():
            settings_dict = self.settings_dict

            # Connection to CUBRID database is made through connect() method.
            # Syntax:
            # connect (url[,user[password]])
            #    url - CUBRID:host:port:db_name:db_user:db_password
            #    user - Authorized username.
            #    password - Password associated with the username.
            url = "CUBRID"
            if settings_dict['HOST'].startswith('/'):
                url += ':' + settings_dict['HOST']
            elif settings_dict['HOST']:
                url += ':' + settings_dict['HOST']
            else:
                url += ':localhost'
            if settings_dict['PORT']:
                url += ':' + settings_dict['PORT']
            if settings_dict['NAME']:
                url += ':' + settings_dict['NAME']
            if settings_dict['USER']:
                url += ':' + settings_dict['USER']
            if settings_dict['PASSWORD']:
                url += ':' + settings_dict['PASSWORD']

            self.connection = Database.connect(url)
            connection_created.send(sender=self.__class__, connection=self)
        cursor = CursorWrapper(self.connection.cursor())
        return cursor

    def get_server_version(self):
        if not self.server_version:
            if not self._valid_connection():
                self.cursor()
            m = self.connection.server_version()
            if not m:
                raise Exception('Unable to determine CUBRID version')
            self.server_version = m
        return self.server_version
