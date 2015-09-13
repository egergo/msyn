var util = require('util');
var Promise = require('bluebird');
var zlib = require('zlib');

var Auctions = require('./auction_house.js').Auctions;

function AuctionStore(opt) {
	opt = opt || {};
	if (!opt.azure) { throw new Error('opt.azure must be specified'); }
	if (!opt.log) { throw new Error('opt.log must be specified'); }

	this._azure = opt.azure;
	this._log = opt.log;
}

/**
 * Load current processed auction data
 *
 * @param {string} region
 * @param {string} realm
 *
 * TODO: currently unused
 */
AuctionStore.prototype.loadCurrentProcessedAuctions = function(region, realm) {
	return Promise.bind(this).then(function() {
		var rowKey = 'current-' + region + '-' + realm;
		return this._azure.tables.retrieveEntityAsync('cache', rowKey, '');
	}).spread(function(result) {
		return result.lastProcessed._;
	}).then(function(lastProcessed) {
		var storageName = this._getStorageName(region, realm, AuctionStore.Type.Processed, lastProcessed);
		return this._azure.blobs.getBlobToBufferGzipAsync(storageName.container, storageName.path).spread(function(buf) {
			return new Auctions({
				lastModified: lastProcessed,
				past: JSON.parse(buf)
			});
		});
	}).catch(function(err) {
		// TODO: check error type
		throw new Error('realm not found: ' + region + '-' + realm);
	});
};

/**
 * Loads processed auctions for a date
 *
 * @param {string} region
 * @param {string} realm
 * @param {date} date
 */
AuctionStore.prototype.loadProcessedAuctions = function(region, realm, date) {
	var name = this._getStorageName(region, realm, AuctionStore.Type.Processed, date);
	return this._azure.blobs.getBlobToBufferGzipAsync(name.container, name.path).spread(function(res) {
		res = JSON.parse(res);
		if (res.auctions) {
			return new Auctions({lastModified: date, processed: res});
		} else {
			return new Auctions({lastModified: date, past: res});
		}
	});
};

/**
 * Loads raw auctions for a date
 *
 * @param {string} region
 * @param {string} realm
 * @param {date} date
 */
AuctionStore.prototype.loadRawAuctions = function(region, realm, date) {
	var name = this._getStorageName(region, realm, AuctionStore.Type.Raw, date);
	return this._azure.blobs.getBlobToBufferGzipAsync(name.container, name.path).spread(function(res) {
		return new Auctions({lastModified: date, data: JSON.parse(res)});
	});
};

/**
 * Gets the last processed time of the realm. Returns zero date if the realm
 * was never processed before
 *
 * @param {string} region
 * @param {string} realm
 * @returns {date}
 */
AuctionStore.prototype.getLastProcessedTime = function(region, realm) {
	return this._azure.tables.retrieveEntityAsync(AuctionStore.CacheTableName, 'current-' + region + '-' + realm, '').spread(function(result) {
		return result.lastProcessed._;
	}).catch(function(err) {
		if (err.code === 'ResourceNotFound') { return new Date(0); }
		throw err;
	});
};

/**
 * Gets a list of timestamps of the fetched auctions since lastProcessed.
 *
 * @param {string} region
 * @param {string} realm
 * @param {date} lastProceessed Returned fetched auctions must be greater than this
 * @returns {[object.<date>]}
 */
AuctionStore.prototype.getFetchedAuctionsSince = function(region, realm, lastProcessed) {
	var q = new this._azure.TableQuery()
		.where('PartitionKey == ? and RowKey > ?', 'snapshots-' + region + '-' + realm, '' + lastProcessed.getTime());
	return this._azure.tables.queryEntitiesAsync(AuctionStore.CacheTableName, q, null).spread(function(res) {
		return res.entries.map(function(entry) {
			return {
				lastModified: entry.lastModified._
			};
		});
	});
};

/**
 * Store processed auctions and update lastProcessed date
 *
 * @param {Auctions} auctions
 * @param {string} region
 * @param {string} realm
 */
// TODO: region and realms into auctions
AuctionStore.prototype.storeAuctions = function(auctions, region, realm) {
	var log = this._log.child({
		method: 'storeAuctions',
		region: region,
		realm: realm
	});


	var self = this;
	var tableName = this._getAuctionsTableName(region, realm, auctions._lastModified);

	return Promise.all([
		storeToBlobs(),
		storeToTable()
	]).then(updateLastProcessed);

	function updateLastProcessed() {
		var ent = self._azure.ent;

		return self._azure.tables.insertOrReplaceEntityAsync(AuctionStore.CacheTableName, {
			PartitionKey: ent.String('current-' + region + '-' + realm),
			RowKey: ent.String(''),
			lastProcessed: ent.DateTime(auctions._lastModified)
		});
	}

	function storeToBlobs() {
		var raw = JSON.stringify({
			auctions: auctions._auctions,
			priceChanges: auctions._priceChanges,
			changes: auctions._changes
		});
		var name = self._getStorageName(region, realm, AuctionStore.Type.Processed, auctions._lastModified);
		self._log.info('saving to %s: %s', name.container, name.path);
		return self._azure.blobs.lazyContainer(name.container, function() {
			return self._azure.blobs.createBlockBlobFromTextGzipAsync(name.container, name.path, raw);
		});
	}

	function storeToTable() {
		return Promise.bind(this).then(function() {
			log.info({tableName: tableName}, 'creating table %s', tableName.table);
			return self._azure.tables.createTableIfNotExistsAsync(tableName.table);
		}).then(function() {
			var index = auctions.index2;
			var batches = [];
			var currentBatch;

			// for better profiling
			function JSONStringify(a) {
				return JSON.stringify(a);
			}

			var s = process.hrtime();
			var ownerindex = auctions.index2.owners;
			var str = JSONStringify(ownerindex);
			var z = zlib.deflateRawSync(new Buffer(str));
			var diff = process.hrtime(s);
			var ms = (diff[0] * 1e9 + diff[1]) / 1e6;

			// item batches
			Object.keys(index.items).forEach(function itemsToBatches(itemId) {
				var auctionsToStore = index.items[itemId];
				if (!currentBatch) {
					currentBatch = new self._azure.TableBatch();
					batches.push(currentBatch);
				}
				var data = zlib.deflateRawSync(new Buffer(JSONStringify(auctionsToStore)));
				if (data.length > 64 * 1024) {
					log.error({itemId: itemId, data: data.length}, 'item too long');
				} else {
					currentBatch.insertOrMergeEntity({
						PartitionKey: self._azure.ent.String(tableName.items),
						RowKey: self._azure.ent.String(itemId),
						Auctions: self._azure.ent.Binary(data)
					});
					if (currentBatch.size() >= 100) {
						currentBatch = undefined;
					}
				}
			});
			currentBatch = undefined;

			// owner batches
			Object.keys(index.owners).forEach(function ownersToBatches(owner) {
				var auctionsToStore = index.owners[owner];

				if (!currentBatch) {
					currentBatch = new self._azure.TableBatch();
					batches.push(currentBatch);
				}
				var data = zlib.deflateRawSync(new Buffer(JSONStringify(auctionsToStore)));
				if (data.length > 64 * 1024) {
					log.error({owner: owner, length: data.length}, 'owner too long');
				} else {
					currentBatch.insertOrMergeEntity({
						PartitionKey: self._azure.ent.String(tableName.owners),
						RowKey: self._azure.ent.String(encodeURIComponent(owner)),
						Auctions: self._azure.ent.Binary(data)
					});
					if (currentBatch.size() >= 100) {
						currentBatch = undefined;
					}
				}
			});
			currentBatch = undefined;

			log.info({batches: batches.length, tableName: tableName}, 'saving %s batches to %s...', batches.length, tableName.table);
			return batches;
		}).map(function(batch) {
			return self._azure.tables.executeBatchAsync(tableName.table, batch);
		}, {concurrency: 100}).then(function() {
			log.info('batches saved');
		});
	}
};

AuctionStore.prototype._getAuctionsTableName = function(region, realm, date) {
	var month = '' + (date.getUTCMonth() + 1);
	if (month.length === 1) { month = '0' + month; }
	var day = '' + (date.getUTCDate() + 1);
	if (day.length === 1) { day = '0' + day; }
	return {
		table: util.format('XAuctions%s%s%s', date.getUTCFullYear(), month, day),
		items: encodeURIComponent(util.format('%s-%s-items-%s', region, realm, date.getTime())),
		owners: encodeURIComponent(util.format('%s-%s-owners-%s', region, realm, date.getTime()))
	};
};
/**
 * @param {string} region
 * @param {string} realm
 * @param {string} type
 * @param {date} date
 */
AuctionStore.prototype._getStorageName = function(region, realm, type, date) {
	var month = '' + (date.getUTCMonth() + 1);
	if (month.length === 1) { month = '0' + month; }
	var day = '' + (date.getUTCDate() + 1);
	if (day.length === 1) { day = '0' + day; }
	var name = util.format('%s/%s/%s/%s.gz', type, region, realm, date.getTime());
	return {
		container: util.format('XAuctions%s%s%s', date.getUTCFullYear(), month, day),
		path: name
	};
};

AuctionStore.CacheTableName = 'cache';

AuctionStore.Type = {
	Raw: 'auctions',
	Processed: 'processed',
};

module.exports = AuctionStore;
