from zipfile import ZipFile
from urllib import request
from datetime import datetime
import re
import sqlite3
import csv
from csv import Dialect

class gtfs(csv.Dialect):
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL

# utility function
def tts(timestring):
    parts = timestring.split(":")
    if len(parts) != 3:
        return 0
    accumulator = int(parts[0])*3600
    accumulator += int(parts[1])*60
    accumulator += int(parts[2])
    return accumulator

GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
LATEST_DOWNLOADED = "/home/ec2-user/google_transit.zip"
DB_PATH = "/tmp/google_transit.db"
#DB_PATH = ":memory:"

class GoogleTransitData():
    def __init__(self):
        csv.register_dialect("gtfs",gtfs)
        self.db = sqlite3.connect('/tmp/transit.db')
        c = self.db.cursor()
        try:
            c.execute("SELECT * FROM meta_data WHERE key='last_update';")
            last_update = c.fetchone()[1]
        except sqlite3.OperationalError:
            c.execute("CREATE TABLE meta_data (key TEXT, value TEXT);")
            c.execute("INSERT INTO meta_data (key,value) VALUES ('last_update','0');")
            self.db.commit()
    def getLastUpdate(self):
        c = self.db.cursor()
        c.execute("SELECT value FROM meta_data WHERE key='last_update';")
        row = c.fetchone()
        if row:
            return datetime.fromtimestamp(int(row[0]))
        return None
    def extractGoogleTransitData(self,google_transit_zip):
        start_extract = datetime.now()
        z = ZipFile(google_transit_zip)
        z.extract("stops.txt","/tmp/")
        print("Extracting stops...")
        self.extractStops("/tmp/stops.txt")
        z.extract("routes.txt","/tmp/")
        print("Extracting routes...")
        self.extractRoutes("/tmp/routes.txt")
        z.extract("routes.txt","/tmp/")
        print("Extracting trips...")
        z.extract("trips.txt","/tmp/")
        self.extractTrips("/tmp/trips.txt")
        print("Extracting calendar...")
        z.extract("calendar.txt","/tmp/")
        self.extractCalendar("/tmp/calendar.txt")
        print("Extracting stop times...")
        z.extract("stop_times.txt","/tmp/")
        self.extractStopTimes("/tmp/stop_times.txt")
        c = self.db.cursor()
        c.execute("UPDATE meta_data SET value='%d' WHERE key='last_update'" % datetime.now().timestamp())
        self.db.commit()
        end_extract = datetime.now()
        print("Done in %0.2f seconds." % (end_extract.timestamp()-start_extract.timestamp(),))
    def extractCalendar(self,calendar_file_name):
        c = self.db.cursor()
        c.executescript("DROP TABLE IF EXISTS calendar;CREATE TABLE calendar (service_id TEXT PRIMARY KEY, mon INTEGER, tue INTEGER, wed INTEGER, thu INTEGER, fri INTEGER, sat INTEGER, sun INTEGER, start_date INTEGER, end_date INTEGER);")
        with open(calendar_file_name) as f:
            reader = csv.reader(f,"gtfs")
            for row in reader:
                c.execute("INSERT INTO calendar (service_id, mon, tue, wed, thu, fri, sat, sun, start_date, end_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
            self.db.commit()
    def extractTrips(self,trips_file_name):
        c = self.db.cursor()
        c.executescript("DROP TABLE IF EXISTS trips;CREATE TABLE trips (route_id TEXT, service_id TEXT, trip_id TEXT PRIMARY KEY, trip_headsign TEXT, direction_id INTEGER, shape_id TEXT);")
        with open(trips_file_name) as f:
            reader = csv.reader(f,"gtfs")
            for row in reader:
                c.execute("INSERT INTO trips (route_id,service_id,trip_id,trip_headsign,direction_id,shape_id) VALUES (?,?,?,?,?,?)",
                (row[0],row[1],row[2],row[3],row[4],row[6]))
            self.db.commit()
    def extractStops(self,stops_file_name):
        c = self.db.cursor()
        c.executescript("DROP TABLE IF EXISTS stops;CREATE TABLE stops (stop_id TEXT PRIMARY KEY,stop_name TEXT,stop_lat DOUBLE, stop_long DOUBLE, parent TEXT);")
        with open(stops_file_name) as f:
            reader = csv.reader(f,"gtfs")
            for row in reader:
                c.execute("INSERT INTO stops (stop_id,stop_name,stop_lat,stop_long,parent) VALUES (?,?,?,?,?)",
                (row[0],row[2],row[4],row[5],row[9]))
            self.db.commit()
    def extractRoutes(self,route_file_name):
        c = self.db.cursor()
        c.executescript("DROP TABLE IF EXISTS routes;CREATE TABLE routes (route_id TEXT PRIMARY KEY,route_short_name TEXT,route_long_name TEXT, route_color TEXT,route_text_color TEXT);")
        with open(route_file_name) as f:
            reader = csv.reader(f,"gtfs")
            for row in reader:
                c.execute("INSERT INTO routes (route_id,route_short_name,route_long_name,route_color,route_text_color) VALUES (?,?,?,?,?)",
                (row[0],row[2],row[3],row[7],row[8]))
            self.db.commit()
    def extractStopTimes(self,stoptimes_file_name):
        c = self.db.cursor()
        c.executescript("DROP TABLE IF EXISTS stop_times;CREATE TABLE stop_times (trip_id TEXT,arrival_time TEXT,departure_time TEXT, stop_id TEXT, stop_sequence INTEGER);")
        with open(stoptimes_file_name) as f:
            reader = csv.reader(f,"gtfs")
            for row in reader:
                c.execute("INSERT INTO stop_times (trip_id,arrival_time,departure_time,stop_id,stop_sequence) VALUES (?,?,?,?,?)",
                          (row[0],tts(row[1]),tts(row[2]),row[3],row[4]))
            self.db.commit()
    def getSchedulesInEffect(self,for_datetime):
        c = self.db.cursor()
        c.execute("SELECT * FROM calendar;")
        data = c.fetchall()
        schedules = []
        for row in data:
            if row[0] == "service_id":
                continue
            start_year = int(str(row[8])[:4])
            start_month = int(str(row[8])[4:6])
            start_day = int(str(row[8])[6:8])
            end_year = int(str(row[9])[:4])
            end_month = int(str(row[9])[4:6])
            end_day = int(str(row[9])[6:8])
            schedule_started = False
            if for_datetime.year >= start_year and for_datetime.month >= start_month:
                if for_datetime.month == start_month:
                    if for_datetime.day >= start_day:
                        schedule_started = True
                else:
                    schedule_started = True
            if schedule_started != True:
                continue
            schedule_ended = False
            if for_datetime.year > end_year:
                continue
            if for_datetime.month > end_month:
                continue
            if for_datetime.month == end_month:
                if for_datetime.day > end_day:
                    continue
            
            if row[for_datetime.weekday()+1] == 1:
                schedules.append(row[0])
        return schedules
    def getTripsForTimeframe(self,start,end):
        trips = []
        start_seconds = (start.hour * 3600) + (start.minute * 60) + start.second
        end_seconds = (end.hour * 3600) + (end.minute * 60) + end.second
        c = self.db.cursor()
        c.execute("SELECT trip_id,departure_time,stop_id,stop_sequence FROM stop_times WHERE departure_time>? AND departure_time<?",
        (start_seconds,end_seconds))
        if row[0] not in trips:
            trips.append(row[0])
        return trips
    def getStopsNear(self,lat,longitude):
        lat_upper = float(lat) + .015
        lat_lower = float(lat) - .015
        long_upper = float(longitude) + .015
        long_lower = float(longitude) - .015
        
        c = self.db.cursor()
        stops = []
        c.execute("SELECT stop_id,stop_name,stop_lat,stop_long,parent FROM stops WHERE stop_lat > ? AND stop_lat < ? AND stop_long > ? AND stop_long < ?",
        (lat_lower,lat_upper,long_lower,long_upper))
        for row in c:
            new_stop = tuple((row[0],row[1],row[2],row[3],row[4]))
            stops.append(new_stop)
        return stops
    def getTripsForServiceId(self,service_id):
        c = self.db.cursor()
        trips = []
        c.execute("SELECT route_id,trip_id,trip_headsign,direction_id FROM trips WHERE service_id=?",(service_id,))
        for row in c:
            new_trip = tuple((row[0],row[1],row[2],row[3]))
            trips.append(new_trip)
        return trips
    def getStopsBetweenTimes(self,stop_id,start,end):
        stops = []
        c = self.db.cursor()
        start_seconds = (start.hour * 3600) + (start.minute * 60) + start.second
        end_seconds = (end.hour * 3600) + (end.minute * 60) + end.second
        c.execute("SELECT trip_id,departure_time,stop_id,stop_sequence FROM stop_times WHERE stop_id=? AND arrival_time > ? AND departure_time < ?" , (stop_id,start_seconds,end_seconds))
        for row in c:
            new_trip = tuple((row[0],row[1],row[2],row[3]))
            stops.append(new_trip)
        return stops
    def getStopsForTripBetweenTimes(self,trip_id,start,end):
        stops = []
        c = self.db.cursor()
        start_seconds = (start.hour * 3600) + (start.minute * 60) + start.second
        end_seconds = (end.hour * 3600) + (end.minute * 60) + end.second
        c.execute("SELECT trip_id,departure_time,stop_id,stop_sequence FROM stop_times WHERE trip_id=? AND arrival_time > ? AND departure_time < ?" , (trip_id[1],start_seconds,end_seconds))
        for row in c:
            new_trip = tuple((row[0],row[1],row[2],row[3]))
            stops.append(new_trip)
        return stops
    def getStopsForTrip(self,trip_id):
        c = self.db.cursor()
        stops = []
        c.execute("SELECT trip_id,arrival_time,departure_time,stop_id,stop_sequence FROM stop_times WHERE trip_id=?",(trip_id,))
        for row in c:
            new_stop = tuple((row[0],row[1],row[2],row[3],row[4]))
            stops.append(new_stop)
        return stops

class Stations():
    def __init__(self,stops_list,transit_data):
        self.transit_data = transit_data
        self.stops = {}
        for stop in stops_list:
            if stop[4] == '':
                self.stops[stop[1]] = {"stop_times":[],"lat":stop[2],"long":stop[3],"children":[stop[0]]}
            else:
                self.stops[stop[1]]["children"].append(stop[0])
    def getStations(self):
        stations = {}
        for each in self.stops.keys():
            stations[each] = tuple((self.stops[each]['lat'],self.stops[each]['long'],self.stops[each]['children']))
        return stations
    def getStopsForStation(self,station_name,start,end):
        stops = []
        if station_name in self.stops:
            current_station = self.stops[station_name]
            for substation in current_station["children"]:
                stops.extend(transit_data.getStopsBetweenTimes(substation,start,end))
        return stops

if __name__ == '__main__':
    transit_data = GoogleTransitData()
    last_update = transit_data.getLastUpdate()
    if last_update:
        print("Checking database... last update: " + last_update.ctime())
        update_epoch = datetime.fromtimestamp(datetime.now().timestamp() + (3600 * 7 * 24))
        update_epoch = datetime.fromtimestamp(datetime.now().timestamp() + (3600 * 7 * 24))
        if last_update < update_epoch:
            print("Updating database...")
            transit_data.extractGoogleTransitData(LATEST_DOWNLOADED)
    else:
        print("No SQLite database found at %s, extracting Google Transit zip" % DB_PATH)
        transit_data.extractGoogleTransitData(LATEST_DOWNLOADED)
    services = transit_data.getSchedulesInEffect(datetime.now())
    stops_list = []
    # TEST: 20 minutes before and after now
    start = datetime.fromtimestamp(datetime.now().timestamp()-1200)
    end = datetime.fromtimestamp(datetime.now().timestamp()+1200)
    start_seconds = (start.hour * 3600) + (start.minute * 60) + start.second
    end_seconds = (end.hour * 3600) + (end.minute * 60) + end.second
    # TEST: Fulton Street GPS
    stops_list.extend(transit_data.getStopsNear(40.710368,-74.009509))
    stations = Stations(stops_list,transit_data)
    station_stops = stations.getStopsForStation("Fulton St",start,end)
    import pdb;pdb.set_trace()
    

