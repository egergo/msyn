var request = require('request-promise');
var Promise = require('bluebird');
var util = require('util');
var fs = require('fs');
var path = require('path');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var MAX_ID = 150000;

	// for (var x = 0; x < MAX_ID; x++) {
	// 	var txt = fs.readFileSync(path.join(__dirname, x + '.json')).toString();
	// 	if (txt) {
	// 		fs.unlinkSync(path.join(__dirname, x + '.json'));
	// 	}
	// }

console.log('checking existing items...');
var toFetch = [];
for (var x = 0; x < MAX_ID; x++) {
	if (!fs.existsSync(path.join(__dirname, x + '.json'))) {
		toFetch.push(x);
	}
}

if (toFetch.length) {
	console.log('still need to fetch %s items', toFetch.length);


	var done = 0;
	var inter = setInterval(function() {
		console.log('%s / %s [%s%%]', done, toFetch.length, Math.floor(done * 100 / toFetch.length));
	}, 1000);

	Promise.map(toFetch, function(id) {
		return fetchItem(id).then(function(item) {
			var txt = item ? JSON.stringify(item) : '';
			fs.writeFileSync(path.join(__dirname, id + '.json'), txt);
			done++;
		}).catch(function(err) {
			console.error(err.stack);
			// swallow
		});
	}, {concurrency: 10}).finally(function() {
		clearInterval(inter);
	});

} else {
	console.log('got all, joining');

	var result = {};
	for (var x = 0; x < MAX_ID; x++) {
		var txt = fs.readFileSync(path.join(__dirname, x + '.json')).toString();
		if (txt) {
			result[x] = JSON.parse(txt);
		}
	}

	console.log('writing result');
	fs.writeFileSync(path.join(__dirname, 'index.json'), JSON.stringify(result));
}


function fetchItem(itemId) {
	return Promise.resolve().then(function() {
		return request({
			uri: 'http://www.wowhead.com/item=' + itemId + '&xml'
		}).then(function(xml) {
			return xml2js.parseStringAsync(xml);
		}).then(function(itemRaw) {
			if (itemRaw.wowhead.error) { return; }

			var item = {
				n: itemRaw.wowhead.item[0].name[0],
				i: itemRaw.wowhead.item[0].icon[0]._,
				q: itemRaw.wowhead.item[0].quality[0].$.id,
			};
			return item;
		});
	});
}
