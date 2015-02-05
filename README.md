#Map My Cab
This is a data engineering project at Insight Data Science. There are two goals that this project aims to accomplish:
- Provide an API for data scientists and cab dispatchers, for analyzing long term trends in cab behavior w.r.t metrics such as average pickups, dropoffs, occupancy, miles travelled etc.
- Enable a framework for real-time monitoring of cab locations, so that a user can know the unoccupied cabs across a city and zoom in on a specific neighborhood to spot and catch available cabs nearest to them.

#Data Set:
Historical:
The project is based on historical geolocation data for 500 yellow cabs in San Francisco, collected over a month's time frame. The data is available as a time series, with updates on individual cab occupancy and locations at a time interval of 1 minute (approximately). The following table provides a snap shot of the raw data set (500 text files, each representing one cab):

<img src="https://github.com/PreetikaKuls/Insight-MapMyCab/blob/master/images/raw_data.png" alt="alt text" width="300" height="200">

Real-Time:
The historical data set is played back to simulate real-time behavior.

#Data Processing Framework
The following tools have been utilized for various part of the pipeline:



#Live Demo:
A Live Demo of the project is available here:
Charts from sample batch queries available here:






