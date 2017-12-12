from flask import Flask, request, render_template
from datetime import datetime
from GoogleTransitData import GoogleTransitData,Stations
import json
import re
app = Flask(__name__)

INVALID_REQUEST = "{\"success\":false,\"msg\":\"Invalid UUID.\"}"
UUID_RE = re.compile("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}")

@app.route('/')
def home():
    return render_template("main.html")

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
