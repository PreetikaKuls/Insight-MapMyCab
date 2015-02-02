from app import app
import json
import time
from flask import jsonify, render_template
import happybase
import ast

@app.route('/')
@app.route('/index')
def index():
    return "Hello World!"

@app.route('/test_chart')
def test_chart():
	return render_template('test_chart.html')

@app.route('/dow/<month>')
def dow(month):
	hbase = happybase.Connection(ip)
	table = hbase.table('dow_stats')
	rows = table.scan()
	keys, pickups, dropoffs, occ, dist = ([] for i in range(5))
	for key, val in rows:
                year, mon, dow = key.split('_')
		if mon == month:
                	keys.append(dow)
                	cellval = json.loads(val['c:Totals'])
                	pickups.append(cellval['TPickups'])
                        dropoffs.append(cellval['TDropoffs'])
                        occ.append(cellval['Avocc'])
                        dist.append(cellval['Avdist'])
        print pickups, dropoffs, dist, occ
        return render_template('test_chart.html', keys=json.dumps(keys), distances=json.dumps(dist), pickups=json.dumps(pickups), dropoffs =json.dumps(dropoffs), occ=json.dumps(occ))

@app.route('/doworder')
def doworder():
        hbase = happybase.Connection(ip)
        table = hbase.table('dow_stats')
	keys = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
	totalCabs = 500.0
	pickups, dropoffs, occ, dist = ([] for i in range(4))
	for i, day in enumerate(keys):
		row = table.row('2008_5_'+day)
		print day, row
                cellval = json.loads(row['c:Totals'])
                pickups.append(cellval['TPickups']/totalCabs)
                dropoffs.append(cellval['TDropoffs']/totalCabs)
                occ.append(cellval['Avocc']*100.0)
                dist.append(cellval['Avdist']/totalCabs)		
        print keys, pickups, dropoffs, dist, occ
        return render_template('test_chart.html', topic=json.dumps('Cab Metrics by Day of Week'), keys=json.dumps(keys), distances=json.dumps(dist), pickups=json.dumps(pickups), dropoffs =json.dumps(dropoffs), occ=json.dumps(occ))


@app.route('/hod/<day>')
def hod(day):
	hbase = happybase.Connection(ip)
	table = hbase.table('dow_stats')
       	rows = table.scan()
        keys, pickups, dropoffs, occ, dist = ([] for i in range(5))
        for key, val in rows:
             year, mon, dow = key.split('_')
             if day == dow and mon == '5':
  		for col in val:
                        if(col == 'c:Totals'):
				continue
              		keys.append(col)
           		cellval = json.loads(val[col])
                        pickups.append(float(cellval['pickups']))
                        dropoffs.append(float(cellval['dropoffs']))
                        occ.append(float(cellval['occ']))
                        dist.append(float(cellval['dist']))
        print pickups, dropoffs, dist, occ
        return render_template('test_chart.html', keys=json.dumps(keys), distances=json.dumps(dist), pickups=json.dumps(pickups), dropoffs =json.dumps(dropoffs), occ=json.dumps(occ))

@app.route('/hodorder/<day>')
def hodorder(day):
        hbase = happybase.Connection(ip)
        table = hbase.table('dow_stats')

	results = [{} for x in range(24)]
	totalCabs = 500
	cols = table.row('2008_5_'+day)
	for col in cols:
                if(col == 'c:Totals'):
                	continue
		hour = int(col.split(':')[1])
		print hour
		cellval = json.loads(cols[col])
		results[hour]['pickups'] = float(cellval['pickups'])/totalCabs
                results[hour]['dropoffs'] = float(cellval['dropoffs'])/totalCabs
                results[hour]['occ'] = float(cellval['occ'])*100
                results[hour]['dist'] = float(cellval['dist'])/totalCabs
 	keys, pickups, dropoffs, occ, dist = ([] for i in range(5))
	for i, v in enumerate(results):
		keys.append(i)
                pickups.append(v['pickups'])
                dropoffs.append(v['dropoffs'])
                occ.append(v['occ'])
		dist.append(v['dist'])
        print keys, pickups, dropoffs, dist, occ
        return render_template('test_chart.html', topic=json.dumps('Cab Metrics by Hour of Day'), keys=json.dumps(keys), distances=json.dumps(dist), pickups=json.dumps(pickups), dropoffs =json.dumps(dropoffs), occ=json.dumps(occ))	
