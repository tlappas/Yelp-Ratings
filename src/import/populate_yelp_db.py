import psycopg2
import json
import os
import datetime

class YelpDataImporter:

    def __init__(self, conn, datafiles, dataset_path):
        self.conn = conn
        self.datafiles = datafiles
        self.dataset_path = dataset_path

    def populate(self):
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

    # Not capturing attributes or hours. The JSON broke the query.
    def _populate_business_table(self):
        cur = self.conn.cursor()
        with open(self.dataset_path + os.sep + 'business.json','r',encoding='utf8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in business.json')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')

                try:
                    cur.execute("""
                        INSERT INTO business (business_id, name, address, city, state, postal_code, lat, long, stars, review_count, is_open, categories) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (data['business_id'], data['name'], data['address'], data['city'], data['state'], data['postal_code'], data['latitude'], data['longitude'], str(data['stars']), data['review_count'], str(data['is_open']), data['categories']))
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

    def _populate_review_table(self):
        cur = self.conn.cursor()
        with open(self.dataset_path + os.sep + 'review.json','r',encoding='utf8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in review.json')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')

                try:
                    cur.execute("""
                        INSERT INTO review (review_id, user_id, business_id, stars, review_date, review_text, useful, funny, cool) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (data['review_id'], data['user_id'], data['business_id'], data['stars'], data['date'], data['text'], data['useful'], data['funny'], data['cool']))
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

    def _populate_user_table(self):
        cur = self.conn.cursor()
        with open(self.dataset_path + os.sep + 'user.json','r',encoding='utf8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in user.json')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')

                try:
                    cur.execute("""
                        INSERT INTO user_info (user_id, name, review_count, yelping_since, friends, useful, funny, cool, fans, elite, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (data['user_id'], data['name'], data['review_count'], data['yelping_since'], data['friends'], data['useful'], data['funny'], data['cool'], data['fans'], data['elite'], data['average_stars'], data['compliment_hot'], data['compliment_more'], data['compliment_profile'], data['compliment_cute'], data['compliment_list'], data['compliment_note'], data['compliment_plain'], data['compliment_cool'], data['compliment_funny'], data['compliment_writer'], data['compliment_photos']))
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

    def _populate_tip_table(self):
        cur = self.conn.cursor()
        with open(self.dataset_path + os.sep + 'tip.json','r',encoding='utf8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in tip.json')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')

                try:
                    cur.execute("""
                        INSERT INTO tip (tip_text, tip_date, compliment_count, business_id, user_id) VALUES (%s, %s, %s, %s, %s);
                    """, (data['text'], data['date'], data['compliment_count'], data['business_id'], data['user_id']))
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

    def _populate_checkin_table(self):
        cur = self.conn.cursor()
        with open(self.dataset_path + os.sep + 'tip.json','r',encoding='utf8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as err:
                    print('Encountered error decoding line in tip.json')
                    print('\n' + err)
                    print('\n' + line[:60] + '\n')

                try:
                    cur.execute("""
                        INSERT INTO checkin (business_id, dates) VALUES (%s, %s);
                    """, (data['business_id'], data['date']))
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

    #@classmethod
def print_status_bar(prefix, current, total, symbol = '=', width = 80):
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
