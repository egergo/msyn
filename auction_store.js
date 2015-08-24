var util = require('util');
var Promise = require('bluebird');
var log = require('./log');
var logTop = log;
var zlib = require('zlib');

/*
 * Run with:
 * node --debug --debug-brk --expose-gc test/real/auction_store_profile.js
 */

function AuctionStore(opt) {
	opt = opt || {};
	if (!opt.azure) { throw new Error('opt.azure must be specified'); }

	this._azure = opt.azure;
}

/**
 * Load current processed auction data
 *
 * @param {string} region
 * @param {string} realm
 */
AuctionStore.prototype.loadAuctions = function(region, realm) {
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

// TODO: region and realms into auctions
AuctionStore.prototype.storeAuctions = function(auctions, region, realm) {
	var log = logTop.child({
		task: 'storeAuctions',
		region: region,
		realm: realm
	});


	var self = this;
	var tableName = this._getAuctionsTableName(region, realm, auctions._lastModified);

	return Promise.bind(this).then(function() {
		log.info({tableName: tableName}, 'creating table %s', tableName);
		return this._azure.tables.createTableIfNotExistsAsync(tableName);
	}).then(function() {
		var index = auctions.index2;
		var batches = [];
		var currentBatch;

		// for better profiling
		function JSONStringify(a) {
			return JSON.stringify(a);
		}


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
					PartitionKey: self._azure.ent.String('items'),
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
					PartitionKey: self._azure.ent.String('owners'),
					RowKey: self._azure.ent.String(encodeURIComponent(owner)),
					Auctions: self._azure.ent.Binary(data)
				});
				if (currentBatch.size() >= 100) {
					currentBatch = undefined;
				}
			}
		});
		currentBatch = undefined;

		log.info({batches: batches.length, tableName: tableName}, 'saving %s batches to %s...', batches.length, tableName);
		return batches;
	}).map(function(batch) {
		return this._azure.tables.executeBatchAsync(tableName, batch);
	}, {concurrency: 4}).then(function() {
		log.info('batches saved');
	});
};

AuctionStore.prototype._getAuctionsTableName = function(region, realm, date) {
	realm = realm.replace(/[^a-zA-Z0-9]/g, '');
	return util.format('Auctions%s%s%s', region, realm, date.getTime());
};

/**
 * @param {string} region
 * @param {string} realm
 * @param {string} type
 * @param {date} date
 */
AuctionStore.prototype._getStorageName = function(region, realm, type, date) {
	var name = util.format('%s/%s/%s/%s/%s/%s/%s.gz', type, region, realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
	return {
		container: 'cache',
		path: name
	};
};

AuctionStore.Type = {
	Processed: 'processed'
};

module.exports = AuctionStore;
