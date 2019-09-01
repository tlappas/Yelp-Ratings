import psycopg2
import json
import sys

import populate_yelp_db

def create_category_table(conn):
    """Create a table to store Yelp categories.

    Stores primary and secondary categories along with their parent category.
    Primary categories have a null parent.

    Args:
        conn: The database connection.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE category(
            id serial PRIMARY KEY,
            name text NOT NULL,
            alias text NOT NULL,
            parent text
        );
    """)

    conn.commit()
    cur.close()


def create_bus_cat_mapping_table(conn):
    """Creates mapping table for businesses and categories.

    Args:
        conn: The database connection.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE bus_cat_map(
            business_id char(22) REFERENCES business(business_id),
            category_id int REFERENCES category(id),
            PRIMARY KEY(business_id, category_id)
        );
    """)

    conn.commit()
    cur.close()

def populate_category_table(conn, cat_file):
    """Fills category table with primary and secondary categories.

    Yelp provides a json file (categories.json) for developers that lists
    hirarchical categories as an adjacency list
    ("category: Italian, parent: Restaurant"). They also provide the full
    category structure (i.e. Resturants > Italian > Sicilian). Within the
    Yelp business.json file, businesses are described with secondary and
    tertiary categories.

    Yelp API(v3) list: https://www.yelp.com/developers/documentation/v3/category_list

    Args:
        conn: The database connection.
        cat_file: A file handle to the categories.json file.
    """
    cur = conn.cursor()

    all_cats = json.load(cat_file)

    primary_cats = [p for p in all_cats if p['parents'] == []]
    for p in primary_cats:
        cur.execute("""
            INSERT INTO category (name, alias, parent)
            VALUES (%s, %s, %s);
        """, 
        (p['title'], p['alias'], None))

        sec_cats = [s for s in all_cats if p['alias'] in s['parents']]
        for s in sec_cats:
            cur.execute("""
                INSERT INTO category (name, alias, parent)
                VALUES (%s, %s, %s);
            """,
            (s['title'], s['alias'], p['alias']))

    conn.commit()
    cur.close()

def map_business_to_categories(conn, per_commit=1000):
    """Populate mapping table.
    """
    current = 0
    bus_cur = conn.cursor()
    cat_cur = conn.cursor()
    map_cur = conn.cursor()

    bus_cur.execute("""
        SELECT COUNT(*) FROM business;
    """)
    bus_count = bus_cur.fetchone()[0]

    bus_cur.execute("""
        SELECT business_id, categories
        FROM business
        WHERE categories IS NOT NULL;
    """)

    for index, row in enumerate(bus_cur):
        categories = row[1].split(', ')
        for cat in categories:
            cat_cur.execute("""
                SELECT category.id 
                FROM category 
                WHERE name = %s;
            """, (cat,))

            for match in cat_cur:
                map_cur.execute("""
                    INSERT INTO bus_cat_map (business_id, category_id)
                    VALUES (%s, %s) ON CONFLICT ON CONSTRAINT bus_cat_map_pkey DO NOTHING;
                """, (row[0], match[0]))

        if index % per_commit == 0:
            populate_yelp_db.print_status_bar('Map Businesses to Categories: ', index, bus_count)
            conn.commit()

    populate_yelp_db.print_status_bar('Map Businesses to Categories: ', bus_count, bus_count)

    bus_cur.close()
    cat_cur.close()
    map_cur.close()

if __name__ == '__main__':
    
    cat_file_path = '/home/tlappas/data_science/Yelp-Ratings/data/raw/categories.json'
    
    # Connect to the database
    conn = psycopg2.connect('dbname={} user={} host={}'.format('yelp', 'tlappas', '/var/run/postgresql/'))
    cur = conn.cursor()
    
    # Drop existing DB tables (for testing)
    try:
        conn.set_session(autocommit=True)

        cur.execute("""
            DROP TABLE bus_cat_map;
        """)
        cur.execute("""
            DROP TABLE category;
        """)

        conn.set_session(autocommit=False)
    except:
        cursor.close()
        sys.exit('Unable to drop the existing category and/or bus_cat_map tables!\n')

    # Create DB tables
    create_category_table(conn)
    create_bus_cat_mapping_table(conn)
    
    # Fill category table from json file
    cat_file = open(cat_file_path, 'rb')
    populate_category_table(conn, cat_file)
    
    # Map businesses to categories
    map_business_to_categories(conn)
    
    cur.close()