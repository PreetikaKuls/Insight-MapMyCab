# Create views for the html page
from app import app
import json
import time
from flask import jsonify, render_template, request
import happybase
import ast

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/maps')
def maps():
    return render_template('index.html')

@app.route('/batch')
def batch():
    return render_template('batch.html')

@app.route('/downloaddata')
def downloaddata():
    return render_template('downloaddata.html')

@app.route('/aboutme')
def aboutme():
    return render_template('aboutme.html')

@app.route('/realtime')
def realtime():
    conn = happybase.Connection('54.67.126.144')
    table = conn.table('avlbl_Cabs')
    row = table.row('StormData')
    cabs = []
    for key, val in row.iteritems():
	dval = json.loads(val)
	cabs.append({'name':key.split(':')[1], 'lat': dval['c:lat'], 'lng': dval['c:lng']})
    return jsonify(cabs=cabs)

@app.route('/doworder')
def doworder():
        hbase = happybase.Connection('54.215.177.124')
        table = hbase.table('dow_stats')
	keys = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
	totalCabs = 500.0
	pickups, dropoffs, occ, dist = ([] for i in range(4))
        for i, day in enumerate(keys):
		row = table.row('2008_5_'+ day)
                cellval = json.loads(row['c:Totals'])
                pickups.append(round(cellval['TPickups']/totalCabs, 2))
                dropoffs.append(cellval['TDropoffs']/totalCabs)
                occ.append(round(cellval['Avocc']*100.0,2))
                dist.append(round(cellval['Avdist']/totalCabs,2))
        items={'pickups':pickups, 'dropoffs':dropoffs, 'occ':occ, 'distances':dist, 'topic':'Cab Metrics By Day of Week', 'keys':keys}	
        return jsonify(items=items) 
       
@app.route('/hodorder/<day>')
def hodorder(day):
        hbase = happybase.Connection('54.215.177.124')
        table = hbase.table('dow_stats')

	results = [{} for x in range(24)]
	totalCabs = 500.0
	cols = table.row('2008_5_'+ day)
	for col in cols:
                if(col == 'c:Totals'):
                	continue
		hour = int(col.split(':')[1])
		cellval = json.loads(cols[col])
		results[hour]['pickups'] = round(float(cellval['pickups'])/totalCabs, 2)
                results[hour]['dropoffs'] = round(float(cellval['dropoffs'])/totalCabs, 2)
                results[hour]['occ'] = round(float(cellval['occ'])*100, 2)
                results[hour]['dist'] = round(float(cellval['dist'])/totalCabs, 2)
 	keys, pickups, dropoffs, occ, dist = ([] for i in range(5))
	for i, v in enumerate(results):
		keys.append(i)
                pickups.append(v['pickups'])
                dropoffs.append(v['dropoffs'])
                occ.append(v['occ'])
		dist.append(v['dist'])
        items = {'topic':'Cab Metrics by Hour of Day for ' + day, 'keys':keys, 'distances':dist, 'pickups':pickups, 'dropoffs':dropoffs, 'occ':occ}
	return jsonify(items=items)
