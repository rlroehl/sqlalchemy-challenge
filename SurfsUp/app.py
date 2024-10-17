# Import the dependencies.
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

# Reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    
    session = Session(engine)

    try:
        # Find the most recent date in the data set.
        ceiling = session.query(func.max(Measurement.date)).scalar()
        floor = session.query(func.min(Measurement.date)).scalar()

        session.close()  
    
        """List all available api routes."""
        return (
            f"This site shows Honolulu weather stats.<br/><br/>"
            f"Available Routes: <br/>"
            f"/api/v1.0/precipitation <br/>"
            f"/api/v1.0/stations <br/>"
            f"/api/v1.0/tobs <br/><br/>"
            f"This dataset covers data between the dates of {floor} and {ceiling} <br/>"
            f"To access min, max, and avg temperatures for all data following any specific date: <br/>"
            f"/api/v1.0/YYYY-MM-DD <br/>"
            f"To access min, max, and avg temperatures between two dates: <br/>"
            f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD (start date/end date)"
        )

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    try:
        # Find the most recent date in the data set.
        maxDate = session.query(func.max(Measurement.date)).scalar()

        # Calculate the date one year from the last date in data set
        maxDate = pd.to_datetime(maxDate)
        minDate = maxDate - pd.DateOffset(years=1)

        # Change max/min vars back to strings to match the dataset
        maxDate = maxDate.strftime('%Y-%m-%d')
        minDate = minDate.strftime('%Y-%m-%d')

        # Perform a query to retrieve the data and precipitation scores
        pastYr = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= minDate).all()

        # Create a dictionary to hold the results
        prcp_dct = {date: prcp for date, prcp in pastYr}
        return jsonify(prcp_dct)

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine) 
    try:
        # Query all stations
        stations = session.query(Station).all()

        # Append row data to station_list
        station_list = [s.station for s in stations]  

        return jsonify(station_list)

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    try:
        # Find the most recent date in the data set.
        maxDate = session.query(func.max(Measurement.date)).scalar()

        # Calculate the date one year from the last date in data set
        maxDate = pd.to_datetime(maxDate)
        minDate = maxDate - pd.DateOffset(years=1)

        # Change max/min vars back to strings to match the dataset
        maxDate = maxDate.strftime('%Y-%m-%d')
        minDate = minDate.strftime('%Y-%m-%d')

        #Find most active station (station with most temp observations)
        mas = session.query(Measurement.station).\
            group_by(Measurement.station).\
            order_by(func.count(Measurement.station).desc()).\
            first()[0]
    
        #Query dates and temp observations for most active station
        pastYrActive = session.query(Measurement.date, Measurement.tobs).\
            filter(Measurement.station == mas).\
            filter(Measurement.date >= minDate).\
            all()

        #Convert query results to list
        tobs_dct = [{date: tobs} for date, tobs in pastYrActive]

        #Return the JSON representation of the list
        return jsonify(tobs_dct)

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

@app.route("/api/v1.0/<start>")
def userDate(start):
    session = Session(engine)

    try:
        # Find the most recent date in the data set.
        end = session.query(func.max(Measurement.date)).scalar()

        # Query to get min, max, avg temps following start date
        userTemps = session.query(
            func.max(Measurement.tobs).label('TMAX'),
            func.min(Measurement.tobs).label('TMIN'),
            func.avg(Measurement.tobs).label('TAVG')
        ).filter(
            Measurement.date >= start,
            Measurement.date <= end
        ).all()

        # Set variables to temperature values
        if userTemps:
            TMAX = userTemps[0].TMAX
            TMIN = userTemps[0].TMIN
            TAVG = userTemps[0].TAVG

        # Convert query results to dictionary
        user_dct = {"Maximum Temperature:": TMAX, 
                    "Minimum Temperature:": TMIN, 
                    "Average Temperature:": TAVG
                    }

        session.close()  

        # Add a message to the dictionary, including the dates
        final = {f"Starting on {start}, and ending on {end}:": user_dct}

        # Return the JSON representation of the final dictionary
        return jsonify(final)

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

@app.route("/api/v1.0/<start>/<end>")
def userDates(start, end):
    session = Session(engine)

    try:
        # Query to get min, max, avg temps following start date
        userTemps = session.query(
            func.max(Measurement.tobs).label('TMAX'),
            func.min(Measurement.tobs).label('TMIN'),
            func.avg(Measurement.tobs).label('TAVG')
        ).filter(
            Measurement.date >= start,
            Measurement.date <= end
        ).all()

        # Set variables to temperature values
        if userTemps:
            TMAX = userTemps[0].TMAX
            TMIN = userTemps[0].TMIN
            TAVG = userTemps[0].TAVG

        # Convert query results to dictionary
        user_dct = {"Maximum Temperature:": TMAX, 
                    "Minimum Temperature:": TMIN, 
                    "Average Temperature:": TAVG
                    }

        session.close()  

        # Add a message to the dictionary, including the dates
        final = {f"Starting on {start}, and ending on {end}:": user_dct}

        # Return the JSON representation of the final dictionary
        return jsonify(final)

    except Exception as e:
        return jsonify({"error": str(e)})

    finally:
        session.close()  

if __name__ == '__main__':
    app.run(debug=True)

session.close()