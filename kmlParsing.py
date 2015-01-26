# Code for parsing kml files, from googles direction API - deprecated, since
# the project now uses cab data 

from BeautifulSoup import BeautifulSoup as Soup
import dateutil.parser
import math
import itertools
import matplotlib.pyplot as plt

def cleanData(rawFile):
    handler = open(rawFile).read()
    soup = Soup(handler)
    coords = soup.findAll("gx:coord")
    timeStamp = soup.findAll("when")
    
    path = createPath(coords, timeStamp)

    speed = velocity(dist_time(path))
    vel = []
    time = []
    coord = []
    for val in speed:
        #print("%.2f" % val[0]), val[1].hour, val[1].minute
        vel.append(val[0])
        time.append(val[1])
        #print val[2], val[1],
        coord.append(val[2])
    #plt.plot(time, coord)
    #plt.show()
    #plt.plot(time, vel)
    #plt.show()
    detectPark(speed)
    
def createPath(coords, timeStamp):
	path = []
	for pos, time in itertools.izip(coords, timeStamp):
		longitude, latitude, altitude = pos.text.split(" ")
		longitude, latitude = float(longitude), float(latitude)
		dateTime = dateutil.parser.parse(time.text)
		path.append([longitude, latitude, dateTime])
	return path

def calc_dist (src, dest):
	
	longA, latA = src
	longB, latB = dest
	deg_to_rad = math.pi/180.0

	phiA = (90.0 - latA)*deg_to_rad
	phiB = (90.0 - latB)*deg_to_rad

	thetaA = longA*deg_to_rad
	thetaB = longB*deg_to_rad

	cos = (math.sin(phiA)*math.sin(phiB)*math.cos(thetaA - thetaB)) + (math.cos(phiA)*math.cos(phiB))
	arc = math.acos(cos)

	return arc*3963.168

def calc_interval(timeA, timeB):
	return ((timeA-timeB).seconds)

def dist_time(path):
    dist_path = []
    prev_point = (path[0][0], path[0][1])
    prev_time  = path[0][2]
    for val in path:
        cur_point = (val[0], val[1])
        cur_time = val[2]
        dist = calc_dist(prev_point, cur_point)
        time = calc_interval(cur_time, prev_time)
        dist_path.append([dist, time, cur_time, cur_point])
        prev_point = cur_point
        prev_time = cur_time
        #print dist, time
    return dist_path

def velocity(path):
    velocity_path = []
    for val in path:
        dist, time, absTime, absPos = val[0], val[1], val[2], val[3]
      
        if (float(time) != 0):
            velocity = 3600.0*(float(dist)/(float(time)))
        else: velocity = 0
        if velocity > 100:
            velocity = 0
        velocity_path.append([velocity, absTime, (absPos)])
    return velocity_path

def detectPark(path_vel):
    potentialPark = 0
    count = 0
    i = 0
    print len(path_vel)
    while i < len(path_vel):
        #print path_vel[i][0]
        if path_vel[i][0] <= 5.0 and not potentialPark:
            potentialPark = i
            print i
            while path_vel[i][0] <= 5.0 and i < 660:
                count = count + 1
                i = i + 1
        if count > 10:
            park_spot = path_vel[potentialPark][2]
            print park_spot
            potentialPark = 0
        i = i + 1
    
rawFile = "/Users/preetikataly/Documents/Insight/history-01-16-2015.kml"
cleanData(rawFile)

"""with open(rawFile) as f:
		root = parser.fromstring(open(rawFile, 'r').read())
		#print root.Document.Placemark.gxTrack
		info = root.findall(".//{http://www.google.com/kml/ext/2.2}Track")
		#ans = root.Document.Placemark.findall("name")
		#print ans
		#info = root.findall(".//{http://www.google.com/kml/ext/2.2}when")
		print root.Document.Placemark["{http://www.google.com/kml/ext/2.2}Track"].getchildren()
		#print root.Document.Placemark."""
	