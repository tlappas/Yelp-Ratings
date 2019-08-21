import psycopg2
import psycopg2.sql
import json
import os
import datetime

class YelpDataImporter:
    """Creates a database (if necessary) and fills table from json files.

    Contains functions to create (if necessary) a database and tables to store
    data from the Yelp JSON files. Provides on-screen feedback to the user.

    Attributes:
        conn: The database connection.
        datafiles: A list of json data files.
        dataset_path: The path where the data files live.
        per_commit: The number of records to add to the db per commit.
            Defaults to 1000.
    """

    def __init__(self, conn, datafiles, dataset_path, per_commit=1000):
        """Creates database connection and parameters for loading data.
        """
        self.conn = conn
        self.datafiles = datafiles
        self.dataset_path = dataset_path
        self.per_commit = per_commit

    def populate(self):
        """Imports data for available data files.
        """
        column_maps = {
            'business':{'business_id':'business_id', 'name':'name', 'address':'address', 'city':'city', 'state':'state', 'postal_code':'postal_code', 'lat':'latitude', 'long':'longitude', 'stars':'stars', 'review_count':'review_count', 'is_open':'is_open', 'categories':'categories'},
            'checkin':{'business_id':'business_id', 'dates':'date'},
            'review':{'review_id':'review_id', 'user_id':'user_id', 'business_id':'business_id', 'stars':'stars', 'review_date':'date', 'review_text':'text', 'useful':'useful', 'funny':'funny', 'cool':'cool'},
            'tip':{'tip_text':'text', 'tip_date':'date', 'compliment_count':'complement_count', 'business_id':'business_id', 'user_id':'user_id'},
            'user':{'user_id':'user_id', 'name':'name', 'review_count':'review_count', 'yelping_since':'yelping_since', 'friends':'friends', 'useful':'useful', 'funny':'funny', 'cool':'cool', 'fans':'fans', 'elite':'elite', 'average_stars':'average_stars', 'compliment_hot':'compliment_hot', 'compliment_more':'compliment_more', 'compliment_profile':'compliment_profile', 'compliment_cute':'compliment_cute', 'compliment_list':'compliment_list', 'compliment_note':'compliment_note', 'compliment_plain':'compliment_plain', 'compliment_cool':'compliment_cool', 'compliment_funny':'compliment_funny', 'compliment_writer':'compliment_writer', 'compliment_photos':'compliment_photos'}
        }

        if 'business.json' in self.datafiles:
            print('Importing data into business table...')
            self._populate_business_table()
        if 'checkin.json' in self.datafiles:
            print('Importing data into checkin table...')
            self._populate_checkin_table()
        if 'review.json' in self.datafiles:
            print('Importing data into review table...')
            self._populate_review_table()
        if 'tip.json' in self.datafiles:
            print('Importing data into tip table...')
            self._populate_tip_table()
        if 'user.json' in self.datafiles:
            print('Importing data into user_info table...')
            self._populate_user_table()

    # Not capturing business attributes or hours. The JSON broke the query.
    def _populate_table(self, file_name, table_name):
        """Populates a database table from a Yelp dataset json file.

        Reads information in from any of the Yelp json files and inserts that
        information into the corresponding database table.

        This function builds the SQL query from the corresponding dictionary
        inside the column_names dictionary. It uses psycopg2.sql to safely build
        the query string. This prevents SQL injection attacks and very ugly code
        duplication.

        Args:
            file_name: The name of the json file.
            table_name: The name of the table. May differ from the file name
                if the filename is a reserved word in Postgres.
        """
        cur = self.conn.cursor()
        n_processed = 0
        total_rows = 0

        # Create list of keys.
        # This is to preserve the order of the key/value pairs. Python guarentees
        #   that keys retrieved from a dictionary multiple times will be in the 
        #   same order, provided the dict hasn't been altered. But this for loop
        #   creates many dicts that all have the same keys and it's unclear if
        #   the key order property can be guarenteed.
        column_map = column_maps[file_name.split('.json')[0]]
        columns = [*column_map]

        # Generate SQL statement
        query = psycopg2.sql.SQL('INSERT INTO {} ({}) VALUES ({});').format(
            psycopg2.sql.Identifier(table_name),
            psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(col_name) for col_name in columns]),
            psycopg2.sql.SQL(', ').join(['%s'] * len(columns))
        )

        with open(os.path.join(self.dataset_path, file_name),'r',encoding='utf8') as f:
            for line in f:
                total_rows += 1
            f.seek(0)
            for line in f:
                # Load json object into dictionary
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in json file.')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')
                    continue

                # Insert dict values into database table
                try:
                    cur.execute(query, tuple([data[column_map[col_name]] for col_name in columns]))
                    n_processed += 1
                    if n_processed % self.per_commit == 0:
                        self.conn.commit()
                        print_status_bar('{}: '.format(table_name), n_processed, total_rows)
                except psycopg2.Error as e:
                    print(e.pgerror)
                    # Add logging and error handling
                    continue
        self.conn.commit()
        print_status_bar('{}: '.format(table_name), n_processed, total_rows)
        cur.close()

def print_status_bar(prefix, current, total, symbol = '=', width = 80):
    """Prints a status bar to the screen.

    Args:
        prefix: The text (string) that appears before the status-update section
            of the bar.
        current: The current progress represented as an int. Expected value is
            less than total.
        total: The completed task, represented as an integer.
        symbol: The character to use as the progress bar. A single character is
            expected. Expected to fail if len(symbol) > 1.
        width: The width of the status bar.
    """
    completed = symbol * int(current*width/total)
    remaining = ' ' * (width - int(current*width/total))
    percent = int(current*100/total)
    print(' {} {}{} | {:d}%'.format(prefix, completed, remaining, percent), end = '\r')
    if current >= total:
        print('')

if __name__ == '__main__':
    import time
    size = 75
    for i in range(size):
        print_status_bar('progress: ', i, size-1)
        time.sleep(0.8)
