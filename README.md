#Map My Cab
This is a data engineering project at Insight Data Science. There are two goals that this project aims to accomplish:
- Provide an API for data scientists and cab dispatchers, for analyzing long term trends in cab behavior w.r.t metrics such as average pickups, dropoffs, occupancy, miles travelled etc.
- Enable a framework for real-time monitoring of cab locations, so that a user can know the unoccupied cabs across a city and zoom in on a specific neighborhood to spot and catch available cabs nearest to them.

#Data Set:
Historical:
The project is based on historical geolocation data for 500 yellow cabs in San Francisco, collected over a month's time frame. The data is available as a time series, with updates on individual cab occupancy and locations at a time interval of 1 minute (approximately). The following table provides a snap shot of the raw data set:
Text file for cabID: "abboip"
Lat      | Long       |Occ| Timestamp
-------- | ---------- | - | ----------
37.75134 | -122.39488 | 0 | 1213084687


First Header | Second Header |
------------ | ------------- |
37.75134 | -122.39488 | 0 | 1213084687
Content in the first column | Content in the second column

37.75136 -122.39527 0 1213084659
37.75199 -122.3946 0 1213084540
37.7508 -122.39346 0 1213084489
37.75015 -122.39256 0 1213084237
37.75454 -122.39227 0 1213084177
37.75901 -122.3925 0 1213084172
37.77053 -122.39788 0 1213084092
37.77669 -122.39382 0 1213084032
37.78194 -122.38844 0 1213083971




Real-Time:
Since at this point, there is no API for obtaining real-time data for the cabs, the historical data set is played back to simulate real-time behavior.



