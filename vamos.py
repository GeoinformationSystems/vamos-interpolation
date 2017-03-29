import csv
import json
import urllib
import sqlite3
import os
import fiona
from fiona.crs import from_epsg
from shapely.geometry import Point, mapping
import sys, getopt

class VamosClient:
    def __init__(self):
        self.sqlite_file = 'vamos_temp.sqlite'
        self.host = '141.76.16.144'
        self.port = '20050'
        self.locations_path = 'locations'
        self.emissions_path = 'emissions'
        self.dates_path = 'dates'
        try:
            os.remove(self.sqlite_file)
        except OSError:
            pass
        self.db = sqlite3.connect(self.sqlite_file)

        # create tables
        cur = self.db.cursor()
        cur.execute('''CREATE TABLE locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                epid TEXT UNIQUE NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL)''')

        cur.execute('''CREATE UNIQUE INDEX locations_epid_idx ON locations(epid);''')

        # id,gesamt_mg_m2,reifen_mg_m2,bremsen_mg_m2,strasse_mg_m2,zw,datum
        cur.execute('''CREATE TABLE emissions (
                        epid TEXT UNIQUE NOT NULL,
                        gesamt REAL NOT NULL,
                        reifen REAL NOT NULL,
                        bremsen REAL NOT NULL,
                        strasse REAL NOT NULL,
                        zw REAL NOT NULL)''')

        cur.execute('''CREATE UNIQUE INDEX emissions_epid_idx ON emissions(epid);''')

        cur.execute('''CREATE TABLE mapped_locations (
                        epid TEXT MOT NULL,
                        lat REAL NOT NULL,
                        lng REAL NOT NULL)''')
        cur.execute('''CREATE INDEX mapped_locations_epid_idx ON mapped_locations(epid);''')

        cur.execute('''CREATE VIEW mapped_locations_view AS
                        SELECT id, mapped_locations.lat, mapped_locations.lng
                        FROM mapped_locations INNER JOIN locations
                        ON mapped_locations.epid = locations.epid;''')

        cur.execute('''CREATE VIEW emission_id_view AS
                       SELECT id, gesamt, reifen, bremsen, strasse, zw
                        FROM emissions INNER JOIN locations
                        ON emissions.epid = locations.epid;''')

        cur.execute('''CREATE VIEW emission_point_view AS
                        SELECT lat, lng, gesamt, reifen, bremsen, strasse, zw
                        FROM emissions INNER JOIN mapped_locations
                        ON emissions.epid = mapped_locations.epid;''')

        cur.close()

    @staticmethod
    def emission_point_view_dict(variable):
        return {
            'gesamt': 2,
            'reifen': 3,
            'bremsen': 4,
            'strasse': 5,
            'zw': 6
        }.get(variable, None)  # default if variable not found

    def __str__(self):
        url = "http://" + self.host
        if self.port is not None:
            url += ":" + self.port
        url += "/"
        if self.path is not None:
            url += self.path
        if self.params is not None:
            url += "?"
            url += urllib.urlencode(self.params)
        return url

    def root_url(self):
        url = "http://" + self.host
        url += ":" + self.port
        url += "/"
        return url

    def dates_url(self):
        return self.root_url() + self.dates_path

    def locations_url(self):
        return self.root_url() + self.locations_path

    def emissions_url(self, date):
        date_param = {"DATE": "%s" % date}
        url = self.root_url() + self.emissions_path
        url += "?"
        url += urllib.urlencode(date_param)
        return url

    # returns a list of date strings
    def load_dates(self):
        print 'loading dates from [%s] ...' % self.dates_url()
        sys.stdout.flush()
        response = urllib.urlopen(self.dates_url())
        dates = json.loads(response.read())
        return dates

    def load_locations(self):
        print 'loading locations from [%s] ...' % self.locations_url()
        sys.stdout.flush()
        response = urllib.urlopen(self.locations_url())
        dr = csv.DictReader(response)
        print dr.fieldnames
        to_db = [(i['id'], i['lat'], i['lon']) for i in dr]

        self.db.execute("DELETE FROM locations;")
        self.db.executemany("INSERT INTO locations (epid, lat, lng) VALUES (?, ?, ?);", to_db)
        self.db.commit()

    def load_mapped_locations(self):
        print 'loading mapped locations from [locations-mapped-densified.csv] ...'
        sys.stdout.flush()
        with open('locations-mapped-densified.csv') as csvfile:
            dr = csv.DictReader(csvfile)
            print dr.fieldnames
            to_db = [(i['id'], i['lat'], i['lon']) for i in dr]

            self.db.execute("DELETE FROM mapped_locations;")
            self.db.executemany("INSERT INTO mapped_locations (epid, lat, lng) VALUES (?, ?, ?);", to_db)
            self.db.commit()

    def load_emissions(self, date):
        print 'loading emissions for %s from [%s] ...' % (date, self.emissions_url(date))
        sys.stdout.flush()
        response = urllib.urlopen(self.emissions_url(date))
        dr = csv.DictReader(response)
        print dr.fieldnames
        # id,gesamt_mg_m2,reifen_mg_m2,bremsen_mg_m2,strasse_mg_m2,zw
        to_db = [(i['id'], i['gesamt_mg_m2'], i['reifen_mg_m2'], i['bremsen_mg_m2'], i['strasse_mg_m2'], i['zw'])
                 for i in dr]

        self.db.execute("DELETE FROM emissions;")
        self.db.executemany("INSERT INTO emissions (epid, gesamt, reifen, bremsen, strasse, zw) VALUES (?, ?, ?, ?, ?, ?);", to_db)
        self.db.commit()

    def load_all_for_date(self, date):
        valid_dates = self.load_dates()
        if date not in valid_dates:
            print 'Invalid date %s. Valid dates:' % date
            print str(valid_dates)
            sys.stdout.flush()
            sys.exit(2)
        self.load_locations()
        self.load_emissions(date)
        self.load_mapped_locations()

    def located_ids(self):
        return self.db.execute('SELECT * FROM mapped_locations_view').fetchall()

    def id_value_lut(self):
        return self.db.execute('SELECT * FROM emission_id_view').fetchall()

    def mapped_values(self):
        return self.db.execute('SELECT * FROM emission_point_view').fetchall()

    def write_location_points_shape(self, shapefilename):
        schema = {'geometry': 'Point', 'properties': {'id': 'int'}}
        from_epsg(3857)
        with fiona.open(shapefilename, 'w', crs=from_epsg(4326), driver='ESRI Shapefile', schema=schema) as output:
            for row in self.located_ids():
                # (91506, 50.998424433004615, 13.844039325476826)
                # geometry
                point = Point(float(row[2]), float(row[1]))
                # attributes
                prop = {'id': int(row[0])}
                # write the row (geometry + attributes in GeoJSON format)
                output.write({'geometry': mapping(point), 'properties': prop})

    def write_location_points_csv(self, csvfilename):
        with open(csvfilename, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['id', 'lat', 'lon'])
            for row in self.located_ids():
                # (91506, 50.998424433004615, 13.844039325476826)
                writer.writerow([row[0], row[1], row[2]])

    def write_value_points_csv(self, variable, csvfilename):
        print 'Writing %s to %s' % (variable, csvfilename)
        sys.stdout.flush()
        with open(csvfilename, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['lat', 'lon','value'])
            pos = self.emission_point_view_dict(variable)
            for row in self.mapped_values():
                # (50.998424433004615, 13.844039325476826, v1, v2, ...)
                writer.writerow([row[0], row[1], row[pos]])


def main2(argv):
    vamos = VamosClient()
    vamos.load_all_for_date('2017-01-12')
    print 'Converting point data to csv'
    for variable in ('gesamt', 'reifen', 'bremsen', 'strasse', 'zw'):
        vamos.write_value_points_csv(variable=variable, csvfilename=variable + '.csv')

def main(argv):
    date = ''
    try:
        opts, args = getopt.getopt(argv, "hd:")
    except getopt.GetoptError:
        print 'vamos.py -d <date>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'vamos.py -d <date>'
            sys.exit()
        elif opt in ("-d", "--date"):
            date = arg
    print 'Processing date %s' % date

    vamos = VamosClient()
    vamos.load_all_for_date(date)
    print 'Loaded point data'

    workspace = os.getcwd()

    print 'Converting point data to csv'
    for variable in ('gesamt', 'reifen', 'bremsen', 'strasse', 'zw'):
        vamos.write_value_points_csv(variable=variable, csvfilename=variable+'.csv')


if __name__ == "__main__":
    main(sys.argv[1:])
