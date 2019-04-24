import psycopg2
import json
import os
import datetime
import yaml

class YelpDataImporter:

    def __init__(self, conn, datafiles, dataset_path):
        self.conn = conn
        self.datafiles = datafiles
        self.dataset_path = dataset_path

    def populate(self):
        if 'business.json' in self.datafiles:
            print('Importing data into business table...')
            self._populate_business_table()
            print('Importing data into attributes table...')
            self._populate_attributes_table()
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
                    d = {}
                    if data['hours'] is not None:
                        d['hours_mon'] = data['hours'].get('Monday', None)
                        d['hours_tues'] = data['hours'].get('Tuesday', None)
                        d['hours_wed'] = data['hours'].get('Wednesday', None)
                        d['hours_thurs'] = data['hours'].get('Thursday', None)
                        d['hours_fri'] = data['hours'].get('Friday', None)
                        d['hours_sat'] = data['hours'].get('Saturday', None)
                        d['hours_sun'] = data['hours'].get('Sunday', None)
                    cur.execute("""
                        INSERT INTO business (business_id, name, address, city, state, postal_code, lat, long, stars, review_count, is_open, categories, hours_mon, hours_tues, hours_wed, hours_thurs, hours_fri, hours_sat, hours_sun) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", (data['business_id'], data['name'], data['address'], data['city'], data['state'], data['postal_code'], data['latitude'], data['longitude'], str(data['stars']), data['review_count'], str(data['is_open']), data['categories'], d.get('hours_mon', None), d.get('hours_tues', None), d.get('hours_wed', None), d.get('hours_thurs', None), d.get('hours_fri', None), d.get('hours_sat', None), d.get('hours_sun', None)))
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

    def _populate_attributes_table(self):
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
                    data_att = data['attributes']
                    d = {}
                    d['business_id'] = data['business_id']

                    if data_att is not None:
                        for k,v in data_att.items():
                            if v == 'None':
                                data_att[k] = None

                        d['restaurants_price_range2'] = data_att.get('RestaurantsPriceRange2', None)
                        d['noise_level'] = data_att.get('NoiseLevel', None)
                        d['restaurants_attire'] = data_att.get('RestaurantsAttire', None)
                        d['alcohol'] = data_att.get('Alcohol', None)
                        d['by_appointment_only'] = data_att.get('ByAppointmentOnly', None)
                        d['happy_hour'] = data_att.get('HappyHour', None)
                        d['business_accepts_creditcards'] = data_att.get('BusinessAcceptsCreditCards', None)
                        d['good_for_kids'] = data_att.get('GoodForKids', None)
                        d['caters'] = data_att.get('Caters', None)
                        d['restaurants_table_service'] = data_att.get('RestaurantsTableService', None)
                        d['business_accepts_bitcoin'] = data_att.get('BusinessAcceptsBitcoin', None)
                        d['accepts_insurance'] = data_att.get('AcceptsInsurance', None)
                        d['bike_parking'] = data_att.get('BikeParking', None)
                        d['restaurants_reservations'] = data_att.get('RestaurantsReservations', None)
                        d['outdoor_seating'] = data_att.get('OutdoorSeating', None)
                        d['wifi'] = data_att.get('WiFi', None)
                        d['hastv'] = data_att.get('HasTV', None)
                        d['restaurants_take_out'] = data_att.get('RestaurantsTakeOut', None)
                        d['restaurants_delivery'] = data_att.get('RestaurantsDelivery', None)
                        d['restaurants_good_for_groups'] = data_att.get('RestaurantsGoodForGroups', None)
                        d['dogs_allowed'] = data_att.get('DogsAllowed', None)
                        d['wheelchair_accessible'] = data_att.get('WheelchairAccessible', None)

                        if data_att.get('Music', None) is not None and type(yaml.load(data_att['Music'])) is dict: 
                            data_att_m = yaml.load(data_att['Music'])
                            d['dj'] = data_att_m.get('dj', None)
                            d['background_music'] = data_att_m.get('background_music', None)
                            d['jukebox'] = data_att_m.get('jukebox', None)
                            d['live'] = data_att_m.get('live', None)
                            d['video'] = data_att_m.get('video', None)
                            d['karaoke'] = data_att_m.get('karaoke', None)
                            d['no_music'] = data_att_m.get('no_music', None)

                        if data_att.get('GoodForMeal', None) is not None and type(yaml.load(data_att['GoodForMeal'])) is dict: 
                            data_att_gfm = yaml.load(data_att['GoodForMeal'])
                            d['dessert'] = data_att_gfm.get('dessert', None)
                            d['latenight'] = data_att_gfm.get('latenight', None)
                            d['lunch'] = data_att_gfm.get('lunch', None)
                            d['dinner'] = data_att_gfm.get('dinner', None)
                            d['brunch'] = data_att_gfm.get('brunch', None)
                            d['breakfast'] = data_att_gfm.get('breakfast', None)  

                        if data_att.get('Ambience', None) is not None and type(yaml.load(data_att['Ambience'])) is dict: 
                            data_att_a = yaml.load(data_att['Ambience'])
                            d['touristy'] = data_att_a.get('touristy', None)
                            d['hipster'] = data_att_a.get('hipster', None)
                            d['romantic'] = data_att_a.get('romantic', None)
                            d['divey'] = data_att_a.get('divey', None)
                            d['intimate'] = data_att_a.get('intimate', None)
                            d['trendy'] = data_att_a.get('trendy', None)
                            d['upscale'] = data_att_a.get('upscale', None)
                            d['classy'] = data_att_a.get('classy', None)
                            d['casual'] = data_att_a.get('casual', None)

                        if data_att.get('BusinessParking', None) is not None and type(yaml.load(data_att['BusinessParking'])) is dict: 
                            data_att_bp = yaml.load(data_att['BusinessParking'])
                            d['garage'] = data_att_bp.get('garage', None)
                            d['street'] = data_att_bp.get('street', None)
                            d['validated'] = data_att_bp.get('validated', None)
                            d['lot'] = data_att_bp.get('lot', None)
                            d['valet'] = data_att_bp.get('valet', None)

                    cur.execute("""
                        INSERT INTO attributes (
                            business_id,
                            restaurants_price_range2,
                            noise_level,
                            restaurants_attire,
                            alcohol,
                            dj,
                            background_music,
                            jukebox,
                            live,
                            video,
                            karaoke,
                            no_music,
                            dessert,
                            latenight,
                            lunch,
                            dinner,
                            brunch,
                            breakfast,
                            touristy,
                            hipster,
                            romantic,
                            divey,
                            intimate,
                            trendy,
                            upscale,
                            classy,
                            casual,
                            garage,
                            street,
                            validated,
                            lot,
                            valet,
                            by_appointment_only,
                            happy_hour,
                            business_accepts_creditcards,
                            good_for_kids,
                            caters,
                            restaurants_table_service,
                            business_accepts_bitcoin,
                            accepts_insurance,
                            bike_parking,
                            restaurants_reservations,
                            outdoor_seating,
                            wifi,
                            hastv,
                            restaurants_take_out,
                            restaurants_delivery,
                            restaurants_good_for_groups,
                            dogs_allowed,
                            wheelchair_accessible
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """, (d['business_id'], d.get('restaurants_price_range2', None), d.get('noise_level', None), d.get('restaurants_attire', None), d.get('alcohol', None),
                             d.get('dj', None), d.get('background_music', None), d.get('jukebox', None), d.get('live', None), d.get('video', None), d.get('karaoke', None),
                             d.get('no_music', None), d.get('dessert', None), d.get('latenight', None), d.get('lunch', None), d.get('dinner', None), d.get('brunch', None),
                             d.get('breakfast', None), d.get('touristy', None), d.get('hipster', None), d.get('romantic', None), d.get('divey', None), d.get('intimate', None), 
                             d.get('trendy', None), d.get('upscale', None), d.get('classy', None), d.get('casual', None), d.get('garage', None), d.get('street', None),
                             d.get('validated', None), d.get('lot', None), d.get('valet', None), d.get('by_appointment_only', None), d.get('happy_hour', None), 
                             d.get('business_accepts_creditcards', None), d.get('good_for_kids', None), d.get('caters', None), d.get('restaurants_table_service', None),
                             d.get('business_accepts_bitcoin', None), d.get('accepts_insurance', None), d.get('bike_parking', None), d.get('restaurants_reservations', None),
                             d.get('outdoor_seating', None), d.get('wifi', None), d.get('hastv', None), d.get('restaurants_take_out', None), d.get('restaurants_delivery', None),
                             d.get('restaurants_good_for_groups', None), d.get('dogs_allowed', None), d.get('wheelchair_accessible', None)))      
                except psycopg2.Error as e:
                    print(e.pgerror)
        self.conn.commit()
        cur.close()

if __name__ == '__main__':
    pass
