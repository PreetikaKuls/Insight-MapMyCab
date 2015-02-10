// Display the historical charts populated by queries

function disp_charts(data) { 
   items = data.items
   var topic = items.topic
   var keys = items.keys
   var distances = items.distances
   var pickups = items.pickups
   var dropoffs = items.dropoffs
   var occ = items.occ
    $('#p_container').highcharts({
        title: {
            text: topic
        },
        xAxis: {
            categories: keys
        },
        yAxis: {
            title: {
                text: topic
            }
        },
        plotOptions: {
            series: {
                cursor: 'pointer',
                point: {
                    events:{
                        click: function(e) {
                            hs.htmlExpand(null, {
                                pageOrigin: {
                                    x: e.pageX || e.clientX,
                                    y: e.pageY || e.clientY
                                },
                                headingText: this.series.name,
                                maincontentText: this.x + ':<br/>' + this.y,
                                width: 200
                            });
                        }
                    }
                },
                marker: {
                    lineWidth: 1
                }
            }
        },
        series: [
	    {
		name: 'Total Pickups Per Cab',
		data: pickups
            } , 
	   // {
	    //  name: 'Total Dropoffs',
            //  data: dropoffs
           // }
        ]
    });      
    $('#d_container').highcharts({
        chart: {
            type: 'column'
        },
        title: {
            text: topic
        },
        xAxis: {
            categories: keys
        },
        yAxis: {
            title: {
                text: topic
            }
        },
        series: [
            {
            name: 'Avg Distance Per Cab',
		data: distances
            },
            {
		name: 'Avg Occupancy Per Cab',        
		data: occ
        }]
    });      
}

$.getJSON('/doworder', disp_charts);
