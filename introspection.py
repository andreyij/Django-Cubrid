from django.db.backends import BaseDatabaseIntrospection

class DatabaseIntrospection(BaseDatabaseIntrospection):

	def get_table_list(self, cursor):
		"Returns a list of table names in the current database."
		cursor.execute("SHOW TABLES")
		return [row[0] for row in cursor.fetchall()]
		