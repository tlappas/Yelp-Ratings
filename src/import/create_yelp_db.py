import psycopg2

"""
TODO:

Foriegn key constrains: user_id, business_id, and review_id should all be
    foriegn keys.

There's a few things in the business table that need to be fixed:

    1. Categories should be stored in a seperate table with a mapping table
        that connects them back to the business. Each business may have
        muliple categories.
    2. Attributes - same as above. This is going to be trickier though
        because there's a weird nested structure. Needs more investigation.
    3. City, State, and Zip do not meet NF3. City and State can be derived
        from the zip code.

"""
class YelpDBMaker:

    def __init__(self, conn, datafiles):
        self.conn = conn
        self.datafiles = datafiles

    def create(self):
        """Drops existing tables and re-creates them.
        """
        print('Drop existing tables...')
        self._drop_existing_tables()
        if 'business.json' in self.datafiles:
            self._create_business_table()
        if 'checkin.json' in self.datafiles:
            self._create_checkin_table()
        if 'review.json' in self.datafiles:
            self._create_review_table()
        if 'tip.json' in self.datafiles:
            self._create_tip_table()
        if 'user.json' in self.datafiles:
            self._create_user_table()

    def _drop_existing_tables(self):
        """Drops any tables that correspond to existing Yelp json files.
        """
        cur = self.conn.cursor()
        for datafile in self.datafiles:
            if datafile == 'user.json':
                datafile = 'user_info.json'
            try:
                print('Dropping table ' + datafile.split('.')[0].lower() + '...')
                cur.execute('DROP TABLE IF EXISTS ' + datafile.split('.')[0].lower() + ';')
                self.conn.commit()
            except psycopg2.Warning as warn:
                print(warn.pgerror)
            except psycopg2.Error as err:
                print(err.pgerror)
                self.conn.rollback()

    def _create_business_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE business(
                business_id char(22) PRIMARY KEY,
                name text,
                address text,
                city text,
                state text,
                postal_code text,
                lat real,
                long real,
                stars real,
                review_count integer,
                is_open boolean,
                attributes json,
                categories text,
                hours json
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_review_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE review (
                review_id char(22) PRIMARY KEY,
                user_id char(22),
                business_id char(22),
                stars integer,
                review_date date,
                review_text text,
                useful integer,
                funny integer,
                cool integer
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_user_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE user_info (
                user_id char(22) PRIMARY KEY,
                name text,
                review_count integer,
                yelping_since date,
                friends text,
                useful integer,
                funny integer,
                cool integer,
                fans integer,
                elite text,
                average_stars real,
                compliment_hot integer,
                compliment_more integer,
                compliment_profile integer,
                compliment_cute integer,
                compliment_list integer,
                compliment_note integer,
                compliment_plain integer,
                compliment_cool integer,
                compliment_funny integer,
                compliment_writer integer,
                compliment_photos integer
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_tip_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE tip (
                tip_id serial PRIMARY KEY,
                tip_text text,
                tip_date date,
                compliment_count integer,
                business_id char(22),
                user_id char(22)
            );
        """)
        self.conn.commit()
        cur.close()

    def _create_checkin_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE checkin (
                checkin_id serial PRIMARY KEY,
                business_id char(22),
                dates text
            );
        """)
        self.conn.commit()
        cur.close()

if __name__ == '__main__':
    pass
