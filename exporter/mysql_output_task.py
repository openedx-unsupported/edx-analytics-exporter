from contextlib import closing
import json
import csv
import mysql.connector

MAX_FETCH_SIZE = 10000


class MysqlOutputTask(object):

    def __init__(self, host, username, password, database, destination_filename):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.destination_filename  = destination_filename

    def execute(self, query):
        with self._connect() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)

                with open(self.destination_filename, 'w') as output_file:
                    self._write_results_to_tsv(cursor, output_file)
            finally:
                cursor.close()

    def _connect(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.username,
            password=self.password,
            database=self.database,
        )

        return closing(connection)

    def _write_results_to_tsv(self, cursor, output_file):

        writer = csv.writer(output_file, delimiter="\t", quoting=csv.QUOTE_NONE, quotechar='')

        writer.writerow(cursor.column_names)

        while True:
            rows = cursor.fetchmany(size=MAX_FETCH_SIZE)
            if not rows:
                break

            for row in rows:
                converted_row = [unicode(v).encode('utf-8').replace('\r', '\\r').replace('\t','\\t').replace('\n', '\\n') for v in row]
                writer.writerow(converted_row)
