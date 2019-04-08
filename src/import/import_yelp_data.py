import psycopg2
import os
import argparse
import sys
import pdb
import create_yelp_db
import populate_yelp_db

if __name__ == '__main__':
    # Argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', type=str, default='tlappas', help='User to access Postgres database. Default is \"tlappas\"')
    parser.add_argument('-w', '--password', type=str, default='', help='Password to access database. Default is empty. Password is not needed if current user can access db.')
    parser.add_argument('-d', '--dbname', type=str, default='yelp', help='Name of the Postgres database. Must exist. Default is \"yelp\"')
    parser.add_argument('-p', '--path', type=str, default="""./Yelp-Ratings/data/test""", help="""Location of the yelp json files. Default is \"./Yelp-Ratings/data/test\"""")
    parser.add_argument('-o', '--host', type=str, default="""/var/run/postgresql""", help="""Postgres host. Default is \"/var/run/postgresql\"""")
    parser.add_argument('-f', '--force', action='store_true', help='Drop an existing database with the same name. Default is \"False\".')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress status updates in terminal. Default is \"False\".')

    args = parser.parse_args()
    dbname = args.dbname
    username = args.username
    password = args.password
    host = args.host
    dataset_path = args.path
    force = args.force
    quiet = args.quiet

    conn = None
    try:
        conn = psycopg2.connect('dbname={} user={} password={} host={}'.format(dbname, username, password, host))
        if force == False:
            print('Database {} already exists. If you\'d like to drop and recreate existing tables use --force.'.format(dbname))
            sys.exit()
    except psycopg2.Error:
        conn = psycopg2.connect('dbname={} user={} password={} host={}'.format(username, username, password, host))
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        cur.execute('CREATE DATABASE {}'.format(dbname))
        cur.close()
        conn.close()
        conn = psycopg2.connect('dbname={} user={} password={} host={}'.format(dbname, username, password, host))

    out = sys.stdout
    if quiet == True:
        out = open(os.devnull, 'w')

    #conn = psycopg2.connect('dbname=yelp user=tlappas host=/var/run/postgresql')
    cur = conn.cursor()

    # Get a list of all existing data files
    datafiles = []
    for file in os.listdir(dataset_path):
        if len(file.split('.')) >= 2 and file.split('.')[-1].lower() == 'json':
            datafiles.append(file)

    print('Available Data Files:', file=out)
    for file in datafiles:
        print('\t' + file, file=out)
    print()

    # Drop/Create db tables
    db_maker = create_yelp_db.YelpDBMaker(conn, datafiles).create()

    # Populate tables with yelp data
    populate_yelp_db.YelpDataImporter(conn, datafiles, dataset_path).populate()
