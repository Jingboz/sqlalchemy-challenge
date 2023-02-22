import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
from flask import Flask, jsonify

# Database Setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Set the most recent date
datelast = dt.date(2017, 8, 23)
datefirst = dt.date(2010, 1, 1)
# Calculate the date one year from the last date in data set.
date1year = datelast - dt.timedelta(days=365)  

# Flask Setup
app = Flask(__name__)

# Flask Routes
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all precipitation data
    result_prcp = session.query(Measurement.date,Measurement.prcp).filter(Measurement.date >= date1year).all()

    session.close()

    # Convert list of tuples into dictionary
    dateprcp_dict = [{row[0]:row[1]} for row in result_prcp]

    return jsonify(dateprcp_dict)


@app.route("/api/v1.0/stations")
def stations():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all station data
    result_sta = session.query(Station.station).all()

    session.close()

    all_names = list(np.ravel(result_sta))

    return jsonify(all_names)


@app.route("/api/v1.0/tobs")
def tobs():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query all station data
    temp_obs = session.query(Measurement.date,Measurement.tobs).\
            filter(Measurement.station == 'USC00519281').\
            filter(Measurement.date >= date1year).all()

    session.close()

    temp_obs_dict = [{row[0]:row[1]} for row in temp_obs]

    return jsonify(temp_obs_dict)


@app.route("/api/v1.0/<start>")
def startdate(start):
    try:
        inputstartdate = pd.to_datetime(start,format='%Y-%m-%d').date()
        if ((inputstartdate>datelast) |
            (inputstartdate<datefirst)):
            return jsonify({"error": f"Date {start} are beyond the range of dataset."}), 404
        else:
            session = Session(engine)

            sel = [func.min(Measurement.tobs),
                    func.avg(Measurement.tobs),
                    func.max(Measurement.tobs)]
            
            temp_obs = session.query(*sel).filter(Measurement.date >= inputstartdate).all()

            session.close()

            temp_obs_list = list(np.ravel(temp_obs))
            
            temp_obs_dict = {"start date":inputstartdate,
                             "min":temp_obs_list[0],
                             "avg":temp_obs_list[1],
                             "max":temp_obs_list[2]}

            return jsonify(temp_obs_dict)
    
    except:
        return jsonify({"error": f"The start date {start} not found."}), 404


@app.route("/api/v1.0/<start>/<end>")
def startenddate(start,end):
    try:
        inputstartdate = pd.to_datetime(start,format='%Y-%m-%d').date()
        inputenddate = pd.to_datetime(end,format='%Y-%m-%d').date()
        if ((inputstartdate > datelast) |
            (inputstartdate < datefirst)):
            return jsonify({"error": f"Date {start} is beyond the range of dataset."}), 404
        elif ((inputenddate > datelast) |
            (inputenddate < datefirst)):
            return jsonify({"error": f"Date {end} is beyond the range of dataset."}), 404
        elif (inputenddate < inputstartdate):
            return jsonify({"error": f"The start date {start} is later than the end date {end}."}), 404
        else:
            session = Session(engine)

            sel = [func.min(Measurement.tobs),
                    func.avg(Measurement.tobs),
                    func.max(Measurement.tobs)]
            
            temp_obs = session.query(*sel).\
                    filter(Measurement.date >= inputstartdate).\
                    filter(Measurement.date <= inputenddate).all()

            session.close()

            temp_obs_list = list(np.ravel(temp_obs))
            
            temp_obs_dict = {"start date":inputstartdate,
                             "end date":inputenddate,
                             "min":temp_obs_list[0],
                             "avg":temp_obs_list[1],
                             "max":temp_obs_list[2]}

            return jsonify(temp_obs_dict)
    
    except:
        return jsonify({"error": f"The start date {start} not found."}), 404

if __name__ == '__main__':
    app.run(debug=True)