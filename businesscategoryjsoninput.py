import psycopg2
import json


data = []

conn = psycopg2.connect("dbname = 'test1' user = 'postgres' password = 'INSERT YOUR PASSWORD HERE' host= 'localhost'")
cur = conn.cursor()

with open('business.json') as business_json:
	for instance in business_json:
		data = json.loads(instance)

		if data['categories'] is not None:
			cur.execute("INSERT INTO public.business_categories (business_id, categories) VALUES (%s, %s)", (data['business_id'], data['categories']))
			conn.commit()
		
		else:
			cur.execute("INSERT INTO public.business_categories (business_id, categories) VALUES (%s, %s)", (data['business_id'], None))
			conn.commit()

cur.close()
conn.close()
business_json.close()