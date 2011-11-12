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
from django.db.backends.mysql.client import DatabaseClient
from django.db.backends.mysql.creation import DatabaseCreation
from django.db.backends.mysql.introspection import DatabaseIntrospection
from django.db.backends.mysql.validation import DatabaseValidation
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
        # Simple return for test. Should enclose in try/except. See MySQL.
        return self.cursor.execute(query, args)

    def executemany(self, query, args):
        # Simple return for test. Should enclose in try/except. See MySQL.
        return self.cursor.executemany(query, args)

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
    # TODO: Implement methods covering CUBRID-specific characteristics.
    # Start with methods returning NotImplementedError().


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'cubrid'
    # Operators taken from MySQL implementation.
    # TODO: Check for differences between this syntax and CUBRID's.
    operators = {
        'exact': '= %s',
        'iexact': 'LIKE %s',
        'contains': 'LIKE BINARY %s',
        'icontains': 'LIKE %s',
        'regex': 'REGEXP BINARY %s',
        'iregex': 'REGEXP %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE BINARY %s',
        'endswith': 'LIKE BINARY %s',
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
        # Should use server_version() method. CUBRIDdb has it implemented,
        # but for some reason it's not working.
        pass