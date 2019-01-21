var L = require('leaflet');
var express = require('express');

var app = express();
var map = L.map('map');

app.use('/:z/:x/:y', imgController);

map.setView([47, 13], 7);

var osm_mapnik = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	maxZoom: 19,
}).addTo(map);

