**Import Code**
- [ ] Pass user params in through a config file.
- [ ] Incorporate category code into db setup code
- [ ] Generic populate function
- [ ] Remove class from db populate code
- [ ] Put column names in config file
- [ ] Create logic for multiple connection strings based on which params have values
- [ ] Do I need to make the connection string safe (no string format replacement)
- [ ] All CREATE and DROP functions should use the psycopg2.SQL substitution functions.
- [ ] Set up foreign keys
- [ ] Update db creation code in import_yelp_data.py. Assume all users have a default psql db, connect to it, look for the yelpdb, create/delete/abort based on result. 