'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}



hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


hclient.table('weather_delays_by_route').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const route=req.query['origin'] + req.query['dest'];
    console.log(route);
	hclient.table('weather_delays_by_route').row(route).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1);
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			dest : req.query['dest'],
			clear_dly : weather_delay("clear"),
			fog_dly : weather_delay("fog"),
			rain_dly : weather_delay("rain"),
			snow_dly : weather_delay("snow"),
			hail_dly : weather_delay("hail"),
			thunder_dly : weather_delay("thunder"),
			tornado_dly : weather_delay("tornado")
		});
		res.send(html);
	});
});


/* Send simulated weather to kafka*/

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);


app.get('/weather.html',function (req, res) {
	var station_val = req.query['station'];
	var fog_val = (req.query['fog']) ? true : false;
	var rain_val = (req.query['rain']) ? true : false;
	var snow_val = (req.query['snow']) ? true : false;
	var hail_val = (req.query['hail']) ? true : false;
	var thunder_val = (req.query['thunder']) ? true : false;
	var tornado_val = (req.query['tornado']) ? true : false;
	var report = {
		station : station_val,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	};

	kafkaProducer.send([{ topic: 'weather-reports', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(report);
			res.redirect('submit-weather.html');
		});
});

/* airport delay*/
hclient.table('annual_airport_weather_delays').scan(
	{
		filter: {
			type: "PrefixFilter",
			value: "ORD"
		},
		maxVersions: 1
	},
	function (err, cells) {
		console.info(cells);
		console.info(groupByYear("ORD", cells));
	})

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function weather_delay(totals, weather) {
	console.info(totals);
	let flights = totals[weather + "_flights"];
	let delays = totals[weather + "_delays"];
	if(flights == 0)
		return " - ";
	return (delays/flights).toFixed(1); /* One decimal place */
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}

function groupByYear(airport, cells) {
	// Mustache expects averages, not total flights and delays
	function yearTotalsToYearAverages(year, yearTotals) {
		let yearRow = { year : year };
		for (const weather of ['clear', 'fog', 'rain', 'snow', 'hail', 'thunder', 'tornado']) {
			yearRow[weather + '_dly'] = weather_delay(yearTotals, weather);
		}
		return yearRow;
	}
	let result = []; // let is a replacement for var that fixes some technical issues
	let yearTotals; // Flights and delays for each year
	let lastYear = 0; // No year yet
	cells.forEach(function (cell) {
		let year = Number(removePrefix(cell['key'], airport));
		if(lastYear !== year) {
			if(yearTotals) {
				result.push(yearTotalsToYearAverages(year, yearTotals))
			}
			yearTotals = {}
		}
		yearTotals[removePrefix(cell['column'], 'delay:')] = Number(cell['$'])
		lastYear = year;
	})
	return result;
}


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
	const airport=req.query['airport'];
	console.log(airport);
	hclient.table('annual_airport_weather_delays').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: airport
			},

			maxVersions: 1
		},
		function (err, cells) {
			let template = filesystem.readFileSync("a-result.mustache").toString();
			let html = mustache.render(template,  { yearly_averages: groupByYear(airport, cells)});
			res.send(html);
		})
});


app.listen(port);
