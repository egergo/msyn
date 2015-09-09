var request = require('request-promise');

var AuctionStore = require('../../auction_store.js');
var Auctions = require('../../auction_house.js').Auctions;
var Azure = require('../../platform_services/azure');

var Promise = require('bluebird');
var fs = require('fs');

var azure = Azure.createFromEnv();
var auctionStore = new AuctionStore({azure: azure});


var auctions = new Auctions({
	data: fs.readFileSync('/Users/egergo/Downloads/auctions.json'),
	lastModified: new Date()
});

var NUM_BUCKETS = 10;
var buckets = [];
for (var x = 0; x < NUM_BUCKETS; x++) {
	buckets.push([]);
}

Object.keys(auctions.index2.items).forEach(function(itemId) {
	var bucketNumber = itemId % NUM_BUCKETS;
	buckets[bucketNumber].push(itemId);
});

for (var x = 0; x < NUM_BUCKETS; x++) {
	console.log('bucket', x, '->', buckets[x].length);
}


process.stdin.resume();