// Render the markers for cab locations on Google Maps
var SF = new google.maps.LatLng(37.7577,-122.4376);
var markers = [];
var map;
function initialize() {
    var mapOptions = {
	zoom: 12,
	center: SF
    };
    map = new google.maps.Map(document.getElementById('map-canvas'),
			      mapOptions);
}
function update_values() {
    $.getJSON('/realtime',
              function(data) {
                  cabs = data.cabs
		  console.log(cabs)
		  clearMarkers();
		  for (var i = 0; i < cabs.length; i = i + 1) {
	              addMarker(new google.maps.LatLng(cabs[i].lat, cabs[i].lng));
		  }
            });
    window.setTimeout(update_values, 5000);
}
update_values();
function drop(lat, lng) {
    point  = new google.maps.LatLng(lat,lng);
    clearMarkers();
    addMarker(point);
}
function addMarker(position) {
    markers.push(new google.maps.Marker({
	position: position,
	map: map,
    }));
}
function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
	markers[i].setMap(null);
    }
    markers = [];
}
google.maps.event.addDomListener(window, 'load', initialize);
