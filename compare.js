var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');
var fs = require('fs');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var Redis = require('ioredis');
var redis = new Redis(process.env.REDIS_URI);

var log = require('./log');

// var key = process.env.BLIZZARD_KEY;

// var now = new Date().getTime();

// var a = load('medivh-1437395516000.json', 1437395516000);
// var b = load('medivh-1437404516000.json', 1437404516000);
// var result = compare(a, b);
// var remind = makeIndex(a, result.removed);
// var addind = makeIndex(b, result.added);
// //console.log('removed', remind.owners['Jovtkc-Medivh'].map(function(x) { return a[remind.items[x][0]];}));
// //console.log('created', addind.owners['Jovtkc-Medivh'].map(function(x) { return b[addind.items[x][0]];}));

// var changes = makeChanges(a, b, remind, addind);
// console.log(changes);





// var a = {
// 	items: b,
// 	changes: changes
// };
// var buf = new Buffer(JSON.stringify(a), 'binary');
// var compressed = require('zlib').deflateRawSync(buf, {level:7});
// console.log('data length:', buf.length, 'compressed:', compressed.length);

// console.log('time: ', new Date().getTime() - now);
// process.exit();

function makeChangesAll(a, b) {
	var result = compare(a, b);
	var remind = makeIndex(a, result.removed);
	var addind = makeIndex(b, result.added);
	var changes = makeChanges(a, b, remind, addind);
	return changes;
}

function makeChanges(a, b, remind, addind) {
	var result = {};

	Object.keys(remind.owners).forEach(function(owner) {
		var items = remind.owners[owner];

		Object.keys(items).forEach(function(itemId) {
			var auctionIds = items[itemId];

			var removedAuctions = auctionIds.map(function(x) { return a[x]; });
			removedAuctions.sort(function(a, b) {
				return b.buyoutPerItem - a.buyoutPerItem;
			});

			var relisted = [];
			if (addind.owners[owner] && addind.owners[owner][itemId]) {
				addind.owners[owner][itemId].forEach(function(auctionId) {
					for (var x = 0; x < removedAuctions.length; x++) {
						if (removedAuctions[x].quantity === b[auctionId].quantity) {
							relisted.push(removedAuctions.splice(x, 1)[0]);
							break;
						}
					}
				});
			}

			var resultItem = result[itemId];
			if (!resultItem) {
				resultItem = result[itemId] = {};
			}
			var resultOwner = resultItem[owner];
			if (!resultOwner) {
				resultOwner = resultItem[owner] = {};
			}

			if (relisted.length) {
				resultOwner.relisted = relisted;
			}
			if (removedAuctions.length) {
				resultOwner.sold = removedAuctions;
			}
		});
	});

	return result;
}

function compare(a, b) {
	var removed = [];
	var added = [];
	var expired = [];

	for (var x in b) {
		if (!a[x]) {
			added.push(x);
		} else {
			if (a[x].timeLeft === b[x].timeLeft) {
				b[x].timeLeftSince = a[x].timeLeftSince;
			}
		}
	}

	for (var x in a) {
		if (!b[x]) {
			if (a[x].timeLeft === 1) { // TODO: calculate properly
				expired.push(x);
			} else {
				removed.push(x);
			}
		}
	}

	return {
		removed: removed,
		added: added,
		expired: expired
	};
}

function load(file, now) {
	var json = JSON.parse(fs.readFileSync(file));
	return loadJson(json, now);
}

function loadJson(json, now) {
	var auctions = {};

	json.auctions.auctions.forEach(function(auction) {
		if (auction.buyout === 0) { return; } // we only deal with buyouts

		var my = auctions[auction.auc] = {
			item: auction.item,
			owner: auction.owner + '-' + auction.ownerRealm,
			quantity: auction.quantity,
			buyoutPerItem: auction.buyout / auction.quantity,
			timeLeft: timeLeftToInt(auction.timeLeft),
			timeLeftSince: now
		};
	});

	return auctions;
}

function makeIndex(auctions, indices) {
	var items = {};
	var owners = {};
	var itemOwners = {};

	if (!indices) {
		indices = Object.keys(auctions);
	}

	indices.forEach(function(x) {
		var my = auctions[x];

		var ownerSet = owners[my.owner];
		if (!ownerSet) {
			ownerSet = owners[my.owner] = {};
		}
		var ownerSetItem = ownerSet[my.item];
		if (!ownerSetItem) {
			ownerSetItem = ownerSet[my.item] = [];
		}
		ownerSetItem.push(x);


		var itemSet = items[my.item];
		if (!itemSet) {
			itemSet = items[my.item] = [x];
		} else {
			itemSet.push(x);
		}
	});

	for (var x in items) {
		items[x].sort(function(a, b) {
			a = auctions[a];
			b = auctions[b];
			return a.buyoutPerItem - b.buyoutPerItem;
		});
	}

	return {
		items: items,
		owners: owners
	};
}

function timeLeftToInt(timeLeft) {
	return {
		VERY_LONG: 4,
		LONG: 3,
		MEDIUM: 2,
		SHORT: 1
	}[timeLeft];
}

module.exports = {
	loadJson: loadJson,
	makeChanges: makeChangesAll
};
