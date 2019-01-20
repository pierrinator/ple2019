var L = require('leaflet');

var map = L.map('map');

map.setView([47, 13], 7);

var osm_mapnik = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	maxZoom: 19,
}).addTo(map);