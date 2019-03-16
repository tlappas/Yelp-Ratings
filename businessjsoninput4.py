import psycopg2
import json


data = []

with open('business.json') as business_json:
	for instance in business_json:
		data.append(json.loads(instance))

i = 0
while i < len(data):
	data[i]['is_open'] = bool(data[i]['is_open'])
	i += 1

conn = psycopg2.connect("dbname = 'test1' user = 'postgres' password = 'INSERT YOUR PASSWORD HERE' host= 'localhost'")
cur = conn.cursor()

i = 0
while i < len(data):
	cur.execute("INSERT INTO public.business (name, business_id, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (data[i]['name'], data[i]['business_id'], data[i]['address'], data[i]['city'], data[i]['state'], data[i]['postal_code'], data[i]['latitude'], data[i]['longitude'], data[i]['stars'], data[i]['review_count'], data[i]['is_open']))
	conn.commit()
	i += 1

cur.close()
conn.close()
business_json.close()