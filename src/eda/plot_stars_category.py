import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import pprint
import pickle

if __name__ == '__main__':
	conn = psycopg2.connect('dbname={} user={} host={}'.format('yelp', 'tlappas', '/var/run/postgresql'))
	cur = conn.cursor()

	pp = pprint.PrettyPrinter(indent=4).pprint

	cur.execute("""
		SELECT category.name, category.alias
		FROM category
		WHERE parent IS NULL;
	""")

	primary = [[row[0], row[1]] for row in cur.fetchall()]
	pp(primary)
	print('')

	cur.execute("""
		SELECT category.name, review.stars, count(review.stars)
		FROM category, review, business, bus_cat_map
		WHERE category.parent IS NULL
			AND category.id = bus_cat_map.category_id
			AND bus_cat_map.business_id = business.business_id
			AND business.business_id = review.business_id
		GROUP BY category.name, review.stars
		ORDER BY category.name, review.stars ASC;
	""")

	#star_counts.append([*cur.fetchall()])
	star_counts = [*cur.fetchall()]
	pp(star_counts)
	print('')

	cur.execute("""
		SELECT review.stars, COUNT(review.stars)
		FROM review
		GROUP BY review.stars
		ORDER BY review.stars;
	""")
	all_stars = [*cur.fetchall()]
	pp(all_stars)

	with open('star_counts.pkl', 'wb') as pkl_file:
		pickle.dump([primary, star_counts, all_stars], pkl_file)
