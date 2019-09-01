import psycopg2
import json

def create_category_table(conn):
    """Create a table to store all Yelp (primary) categories.
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
    """Creates table that maps between business_id and categories.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE bus_cat_map(
            business_id char(22),
            category_id int,
            PRIMARY KEY(business_id, category_id)
        );
    """)

    conn.commit()
    cur.close()

def populate_category_table(conn, cat_file):
    """Fills category table with each primary and secondary categories.

        Yelp provides a json file (categories.json) for developers that lists
        hirarchical categories as an adjacency list
        ("category: Italian, parent: Restaurant"). They also provide the full
        category structure (i.e. Resturants > Italian > Sicilian). Within the
        Yelp business.json file, businesses are described with secondary and
        tertiary categories.

        Yelp API(v3) list: https://www.yelp.com/developers/documentation/v3/category_list
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

def map_business_to_categories(conn):
    """Fill 
    """
    current = 0
    bus_cur = conn.cursor()
    cat_cur = conn.cursor()
    map_cur = conn.cursor()

    bus_cur.execute("""
        SELECT business_id, categories
        FROM business;
        """)

    for rows in bus_cur:
        for row in rows:
            categories = row[1].split(',')
            for cat in categories:
                # Find category in cat table
                cat_cur.execute("""
                    SELECT category.id 
                    FROM category 
                    WHERE name = %s;
                """, cat)

                # Add mapping
                # This should only return one row. May need some error handling.
                for match in cat_cur:
                    map_cur.execute("""
                        INSERT INTO bus_cat_map (business_id, category_id)
                        VALUES (%s, %s);
                    """, (row[0], match[0]))
    
    conn.commit()

    bus_cur.close()
    cat_cur.close()
    map_cur.close()

if __name__ == '__main__':
    cat_file_path = '/home/tlappas/data_science/Yelp-Ratings/data/raw/categories.json'
    # Connect to the database
    conn = psycopg2.connect('dbname={} user={} host={}'.format('yelp', 'tlappas', '/var/run/postgresql/'))
    # Drop existing DB tables (for testing)
    try:
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute("""
            DROP TABLE category;
        """)
        cur.execute("""
            DROP TABLE bus_cat_map;
        """)

        conn.set_session(autocommit=False)
        cursor.close()
    except:
        pass

    # Create DB tables
    create_category_table(conn)
    create_bus_cat_mapping_table(conn)
    # Fill category table from json file
    cat_file = open(cat_file_path, 'rb')
    populate_category_table(conn, cat_file)
    # Map businesses to categories
    map_business_to_categories(conn)
    # Verify the table/column exist or add exception handling
    # Drop the cataegories column from the business table
    #cur = conn.get_cursor()
    # I think this is wrong. At least for create table you need to use an autocommiting cursor
    #cur.execute("""
    #    ALTER TABLE %s 
    #    DROP COLUMN %s;
    #""", ('business', 'categories'))
    #cur.close()