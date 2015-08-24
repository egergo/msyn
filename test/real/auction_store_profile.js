var request = require('request-promise');

var AuctionStore = require('../../auction_store.js');
var Auctions = require('../../auction_house.js').Auctions;
var Azure = require('../../platform_services/azure');

var Promise = require('bluebird');

var azure = Azure.createFromEnv();
var auctionStore = new AuctionStore({azure: azure});

console.log('fetching');
return request({
	uri: 'http://eu.battle.net/auction-data/fcbdc528d47f05674aa1be0ea589cd34/auctions.json',
	gzip: true,
	resolveWithFullResponse: true
}).then(function(res) {
	console.log('fetched');
	global.gc();

	var auctionsRaw = JSON.parse(res.body);

	return Promise.delay(5000).then(function() {
		console.log('processing');

		var lastModified = new Date(res.headers['last-modified']);
		if (!lastModified.getDate()) { throw new Error('invalid Last-Modified value: ' + res.headers['last-modified']); }

		var auctions = new Auctions({
			lastModified: lastModified,
			data: auctionsRaw
		});

		return auctionStore.storeAuctions(auctions, 'eu', 'lightbringer');
	});

}).catch(function(err) {
	console.error(err.stack);
}).finally(function() {
	//process.exit(0);
});


process.stdin.resume();