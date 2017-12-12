from flask import Flask, request, render_template
from datetime import datetime
from GoogleTransitData import GoogleTransitData,Stations
import json
import re
app = Flask(__name__)

INVALID_REQUEST = "{\"success\":false,\"msg\":\"Invalid UUID.\"}"
UUID_RE = re.compile("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")

NEARBY_STOPS = ['Canal St', 'Franklin St', 'Chambers St', 'Cortlandt St', 'Rector St', 'South Ferry Loop', 'South Ferry', 'Park Pl', 'Fulton St', 'Wall St', 'Bowling Green', 'Spring St', 'Brooklyn Bridge - City Hall', 'Broadway-Lafayette St', 'World Trade Center', 'Broad St', 'Prince St', 'City Hall', 'Whitehall St']

@app.route('/')
def home():
    return render_template("main.html",nearby_stations=NEARBY_STOPS)

@app.route('/station/<station>/<uuid>')
def stops(station,uuid):
    if UUID_RE.match(uuid) == None:
        return INVALID_REQUEST
    return render_template("station_detail.html")

@app.route('/stations-near/<uuid>')
def stations_near(uuid):
    if UUID_RE.match(uuid) == None:
        return INVALID_REQUEST
    # check parameters
    lat = request.args.get('lat')
    longitude = request.args.get('long')
    transit_data = GoogleTransitData()
    stops_nearby = transit_data.getStopsNear(lat,longitude)
    stations = Stations(stops_nearby,transit_data)
    return json.dumps(stations.getStations())
