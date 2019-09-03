import psycopg2
import os
import argparse
import sys
import configparser
import path
import pdb
import create_yelp_db
import populate_yelp_db

if __name__ == '__main__':
    """Creates a Postgres database and populates it with information from the
        Yelp json files.

        Imports all available yelp data files into a database (created if necessary).
        Allows users to log in with a username/password or by passing their
        user credientials.
    """
    # Argument parser
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type=str, help='Config file name. Assumed to be in the project\'s config folder.')
    parser.add_argument('-f', '--force', action='store_true', help='Drop an existing database with the same name. Default is \"False\".')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress status updates in terminal. Default is \"False\".')

    args = parser.parse_args()
    conf_file = args.config
    force = args.force
    quiet = args.quiet

    # Locate/open the config file. Unrecoverable if not found.
    # Assuming a standard project folder structure with
    #   top level src and config folders.

    config = configparser.ConfigParser()

    proj_path = os.path.normpath(os.path.normcase(os.path.dirname(os.path.abspath(__file__))))
    try:
        proj_path = os.path.join(proj_path[:proj_path.index('src')-1])
    except:
        stderr('Cannot locate project\'s src folder.')
    try:
        config.read(os.path.join(proj_path, 'config', conf_file))
    except:
        stderr('Cannot read config file - {}.'.format(conf_file))

    # Load config params
    # If any of the first three params are blank it should kill the program
    dbname = config['db-conn']['dbname']
    host = config['db-conn']['host']
    username = config['db-conn']['username']
    password = config['db-conn']['password']
    dataset_path = config['project-info']['dataset_path']

    conn_str = 'dbname={} host={} user={}'.format(dbname, host, username)
    if password:
        conn_str += ' password={}'.format(password)

    conn = None
    try:
        # There's a bug here with a poor workaround. If one of the variables is blank, it kills the rest of the format substitution.
        conn = psycopg2.connect(conn_str)
        if force == False:
            print('Database {} already exists. If you\'d like to drop and recreate existing tables use --force.'.format(dbname))
            sys.exit()
    except psycopg2.Error:
        conn = psycopg2.connect('dbname={} user={} host={} password={}'.format(username, username, host, password))
        cur = conn.cursor()
        conn.set_session(autocommit=True)
        cur.execute('CREATE DATABASE {}'.format(dbname))
        cur.close()
        conn.close()
        conn = psycopg2.connect('dbname={} user={} host={} password={}'.format(dbname, username, host, password))

    out = sys.stdout
    if quiet == True:
        out = open(os.devnull, 'w')

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
