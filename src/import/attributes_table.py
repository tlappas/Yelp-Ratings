import psycopg2
import json
import yaml
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', type=str, default='postgres', help='User to access Postgres database. Default is \"postgres\"')
parser.add_argument('-w', '--password', type=str, default='', help='Password to access database. Default is empty. Password is not needed if current user can access db.')
parser.add_argument('-d', '--dbname', type=str, default='yelp', help='Name of the Postgres database. Must exist. Default is \"yelp\"')
parser.add_argument('-p', '--path', type=str, default="""./Yelp-Ratings/data/test""", help="""Location of the yelp json files. Default is \"./Yelp-Ratings/data/test\"""")
parser.add_argument('-o', '--host', type=str, default="""/var/run/postgresql""", help="""Postgres host. Default is \"/var/run/postgresql\"""")

args = parser.parse_args()
dbname = args.dbname
username = args.username
password = args.password
host = args.host
dataset_path = args.path

conn = psycopg2.connect('dbname={} user={} password={} host={}'.format(dbname, username, password, host))
cur = conn.cursor()
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

conn.commit()
print('Attributes table created')
print('Populating attributes table...')
with open(dataset_path + os.sep + 'business.json', 'r', encoding='utf8') as f:
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
conn.commit()
print('Population of attributes table complete')
cur.close()                    
