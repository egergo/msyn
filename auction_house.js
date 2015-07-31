var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');
var fs = require('fs');
var zlib = require('zlib');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var log = require('./log');

function AuctionHouse(opt) {
	opt = opt || {};
	if (!opt.redis) { throw new Error('opt.redis must be specified'); }
	if (!opt.region) { throw new Error('opt.region must be specified'); }
	if (!opt.realm) { throw new Error('opt.realm must be specified'); }

	this._redis = opt.redis;
	this._region = opt.region;
	this._realm = opt.realm;
	this._key = opt.key;

	this._past = undefined;
	this._present = undefined;
}

AuctionHouse.prototype.load = function(opt) {
	return Promise.bind(this).then(function() {
		opt = opt || {};

		if (opt.data) {
			if (!opt.lastModified) { throw new Error('opt.lastModified must be specified when opt.data is specified'); }
			return {
				data: opt.data,
				lastModified: opt.lastModified
			};
		} else {
			if (!this._key) { throw new Error('opt.key must be specified in the constructor when using network operations'); }

			var redisLastModified = opt.force ? Promise.resolve() : this._redis.get(util.format('realms:%s:%s:auc:lastModified', this._region, this._realm));

			// TODO: format locale
			var url = util.format('https://%s.api.battle.net/wow/auction/data/%s?locale=%s&apikey=%s', this._region, this._realm, 'en_GB', encodeURIComponent(this._key));
			var download = request({
				uri: url,
				gzip: true
			});

			return Promise.all([download, redisLastModified]).bind(this).spread(function(auctionDesc, lastModified) {
				auctionDesc = JSON.parse(auctionDesc);
				var file = auctionDesc.files[0];
				var fileLastModified = Number(file.lastModified);
				var storedLastModified = Number(lastModified);

				if (fileLastModified <= storedLastModified) { var err = new Error('no auction updates found'); err.noUpdatesFound = true; throw err; }

				return request({
					uri: file.url,
					gzip: true
				}).then(function(res) {
					return {
						data: res,
						lastModified: fileLastModified
					};
				});
			});
		}
	}).then(function(res) {
		return new Auctions(res);
	}).then(function(present) {
		return this._redis.getBuffer(util.format('realms:%s:%s:auc:past', this._region, this._realm)).then(function(pastComp) {
			if (!pastComp) { return present; }
			var data = JSON.parse(zlib.inflateRawSync(pastComp));
			var past = new Auctions({
				past: data,
				lastModified: 1
			});
			present.applyPast(past);
			return present;
		});
	}).then(function(present) {
		var comp = zlib.deflateRawSync(new Buffer(JSON.stringify(present.auctions)), {level: 7});
		var saveLastModified = this._redis.set(util.format('realms:%s:%s:auc:lastModified', this._region, this._realm), present._lastModified);
		var saveLast = this._redis.set(util.format('realms:%s:%s:auc:past', this._region, this._realm), comp);

		return Promise.all([saveLastModified, saveLast]).then(function() {
			return present;
		});
	});
};

function Auctions(opt) {
	opt = opt || {};
	if (!opt.lastModified) { throw new Error('opt.lastModified must be specified'); }

	this._lastModified = opt.lastModified;

	if (opt.data) {
		this._auctions = Auctions.readRawAuctions(opt.data, opt.lastModified);
	} else if (opt.past) {
		this._auctions = opt.past;
	} else {
		throw new Error('at least one of opt.data or opt.past must be specified');
	}

	Object.defineProperty(this, 'auctions', {
		get: function() { return this._auctions }
	});
	Object.defineProperty(this, 'index', {
		get: function() {
			if (!this._index) { this._index = Auctions.makeIndex(this._auctions); }
			return this._index;
		}
	});
}
AuctionHouse.Auctions = Auctions;

Auctions.prototype.getAuction = function(auctionId) {
	var result = this._auctions[auctionId];
	if (!result) { throw new Error('auction does not exist. auctionId: ' + auctionId) };
	return result;
};

/**
 * Returns an array of item IDs of the items for which the owner
 * has an active auction.
 *
 * @param {string} owner
 * @returns {number[]}
 */
Auctions.prototype.getOwnerItemIds = function(owner) {
	var ownerIndex = this.index.owners[owner];
	if (!ownerIndex) { return []; }
	return Object.keys(ownerIndex);
};

/**
 * Returns an array of auctions IDs for the given item ID.
 *
 * @param {number} itemId
 * @returns {number[]}
 */
Auctions.prototype.getItemAuctionIds = function(itemId) {
	return this.index.items[itemId] || [];
};

Auctions.prototype.applyPast = function(past) {
	var a = past.auctions;
	var b = this.auctions;

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

	var priceChanges = {};
	for (var x in this.index.items) {
		var presentCheapest = this.getCheapestAuction(x);
		var pastCheapest = past.getCheapestAuction(x);
		if (pastCheapest) {
			priceChanges[x] = presentCheapest.buyoutPerItem - pastCheapest.buyoutPerItem;
		}
	}

	this._priceChanges = priceChanges;
	this._removedIndex = Auctions.makeIndex(a, removed);
	this._addedIndex = Auctions.makeIndex(b, added);
	this._changes = Auctions.makeChanges(a, b, this._removedIndex, this._addedIndex);
};

Auctions.prototype.getCheapestAuction = function(itemId) {
	if (!this.index.items[itemId]) {
		return undefined;
	}
	return this.getAuction(this.index.items[itemId][0]);
}

/**
 * Converts the data format coming from the API into something more usable.
 */
Auctions.readRawAuctions = function(data, now) {
	var json = JSON.parse(data);
	var auctions = {};

	json.auctions.auctions.forEach(function(auction) {
		if (auction.buyout === 0) { return; } // we only deal with buyouts

		var my = auctions[auction.auc] = {
			item: auction.item,
			owner: auction.owner + '-' + auction.ownerRealm,
			quantity: auction.quantity,
			buyoutPerItem: auction.buyout / auction.quantity,
			timeLeft: Auctions.timeLeftToInt(auction.timeLeft),
			timeLeftSince: now
		};
	});

	return auctions;
};

Auctions.timeLeftToInt = function(timeLeft) {
	return {
		VERY_LONG: 4,
		LONG: 3,
		MEDIUM: 2,
		SHORT: 1
	}[timeLeft];
};

/**
 * Indexes the set of auctions
 */
Auctions.makeIndex = function(auctions, indices) {
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
};

Auctions.makeChanges = function(a, b, remind, addind) {
	var result = {sold: {}, relisted: {}, owners: {}};

	Object.keys(remind.owners).forEach(function(owner) {
		var items = remind.owners[owner];

		var itemKeys = Object.keys(items);
		result.owners[owner] = itemKeys;

		itemKeys.forEach(function(itemId) {
			var auctionIds = items[itemId];

			var removedAuctions = auctionIds.map(function(x) { return a[x]; });

			// check for relists
			// try to find and added counterpart for each removed auction where
			// the owner and quantity is the same. always try to match more expensive
			// removed auctions first
			removedAuctions.sort(function(a, b) {
				return b.buyoutPerItem - a.buyoutPerItem;
			});
			if (addind.owners[owner] && addind.owners[owner][itemId]) {
				addind.owners[owner][itemId].forEach(function(auctionId) {
					for (var x = 0; x < removedAuctions.length; x++) {
						if (removedAuctions[x].quantity === b[auctionId].quantity) {
							var pastAuction = removedAuctions.splice(x, 1)[0];
							if (!result.relisted[itemId]) {
								result.relisted[itemId] = {};
							}
							result.relisted[itemId][auctionId] = {
								buyoutPerItem: pastAuction.buyoutPerItem
							};
							break;
						}
					}
				});
			}

			var soldItems = result.sold[itemId];
			if (!soldItems) {
				soldItems = result.sold[itemId] = [];
			}
			removedAuctions.forEach(function(auction) {
				soldItems.push({
					owner: auction.owner,
					quantity: auction.quantity,
					buyoutPerItem: auction.buyoutPerItem
				})
			});
		});
	});

	return result;
};

module.exports = AuctionHouse;
