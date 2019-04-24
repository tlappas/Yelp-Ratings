import psycopg2

"""
TODO:

1. Add error handling
2. Add documentation

"""
class YelpDBMaker:

    def __init__(self, conn, datafiles):
        self.conn = conn
        self.datafiles = datafiles

    def create(self):
        print('Drop existing tables...')
        self._drop_existing_tables()
        if 'business.json' in self.datafiles:
            self._create_business_table()
            self._create_attributes_table()
        if 'checkin.json' in self.datafiles:
            self._create_checkin_table()
        if 'review.json' in self.datafiles:
            self._create_review_table()
        if 'tip.json' in self.datafiles:
            self._create_tip_table()
        if 'user.json' in self.datafiles:
            self._create_user_table()

    def _drop_existing_tables(self):
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
        try:
            print('Dropping table attributes...')
            cur.execute('DROP TABLE IF EXISTS attributes;')
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
                categories text,
                hours_mon text,
                hours_tues text,
                hours_wed text,
                hours_thurs text,
                hours_fri text,
                hours_sat text,
                hours_sun text            
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

    def _create_attributes_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE attributes (
                business_id CHAR(22),
                restaurants_price_range2 integer,
                noise_level text,
                restaurants_attire text,
                alcohol text,
                dj boolean,
                background_music boolean,
                jukebox boolean,
                live boolean,
                video boolean,
                karaoke boolean,
                no_music boolean,
                dessert boolean,
                latenight boolean,
                lunch boolean,
                dinner boolean,
                brunch boolean,
                breakfast boolean,
                touristy boolean,
                hipster boolean,
                romantic boolean,
                divey boolean,
                intimate boolean,
                trendy boolean,
                upscale boolean,
                classy boolean,
                casual boolean,
                garage boolean,
                street boolean,
                validated boolean,
                lot boolean,
                valet boolean,
                by_appointment_only boolean,
                happy_hour boolean,
                business_accepts_creditcards boolean,
                good_for_kids boolean,
                caters boolean,
                restaurants_table_service boolean,
                business_accepts_bitcoin boolean,
                accepts_insurance boolean,
                bike_parking boolean,
                restaurants_reservations boolean,
                outdoor_seating boolean,
                wifi text,
                hastv boolean,
                restaurants_take_out boolean,
                restaurants_delivery boolean,
                restaurants_good_for_groups boolean,
                dogs_allowed boolean,
                wheelchair_accessible boolean
            );
        """)
        self.conn.commit()
        cur.close()

if __name__ == '__main__':
    pass
