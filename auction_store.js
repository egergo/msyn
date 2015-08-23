var util = require('util');
var Promise = require('bluebird');
var log = require('./log');
var logTop = log;
var zlib = require('zlib');

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
// TODO: move to Auctions
AuctionStore.prototype.storeAuctions = function(auctions, region, realm) {
	var log = logTop.child({
		task: 'storeAuctions',
		region: region,
		realm: realm
	});


	var self = this;
	var tableName = this._getAuctionsTableName(region, realm, auctions._lastModified);

	return Promise.bind(this).then(function() {
		return this._azure.tables.createTableIfNotExistsAsync(tableName);
	}).then(function() {
		var index = auctions.index;
		var batches = [];
		var currentBatch;

		// item batches
		Object.keys(index.items).forEach(function(itemId) {
			var auctionIdList = index.items[itemId];
			var auctionsToStore = auctionIdList.map(function(auctionId) {
				return auctions.getAuction(auctionId);
			});

			if (!currentBatch) {
				currentBatch = new self._azure.TableBatch();
				batches.push(currentBatch);
			}
			var data = zlib.deflateRawSync(new Buffer(JSON.stringify(auctionsToStore)));
			if (data.length > 64 * 1024) {
				log.error({itemId: itemId, data: data.length}, 'item too long');
			} else {
				currentBatch.insertOrMergeEntity({
					PartitionKey: self._azure.ent.String('items'),
					RowKey: self._azure.ent.String(itemId),
					ItemId: self._azure.ent.Int64(Number(itemId)),
					Auctions: self._azure.ent.Binary(data)
				});
				if (currentBatch.size() >= 100) {
					currentBatch = undefined;
				}
			}
		});
		currentBatch = undefined;

		// owner batches
		Object.keys(index.owners).forEach(function(owner) {
			var itemIndex = index.owners[owner];
			var auctionsToStore = {};
			Object.keys(itemIndex).forEach(function(itemId) {
				auctionsToStore[itemId] = itemIndex[itemId].map(function(auctionId) {
					return auctions.getAuction(auctionId);
				});
			});

			if (!currentBatch) {
				currentBatch = new self._azure.TableBatch();
				batches.push(currentBatch);
			}
			var data = zlib.deflateRawSync(new Buffer(JSON.stringify(auctionsToStore)));
			if (data.length > 64 * 1024) {
				log.error({owner: owner, length: data.length}, 'owner too long');
			} else {
				currentBatch.insertOrMergeEntity({
					PartitionKey: self._azure.ent.String('owners'),
					RowKey: self._azure.ent.String(encodeURIComponent(owner)),
					Owner: self._azure.ent.String(owner),
					Auctions: self._azure.ent.Binary(data)
				});
				if (currentBatch.size() >= 100) {
					currentBatch = undefined;
				}
			}
		});
		currentBatch = undefined;

		log.info('saving %s batches to %s...', batches.length, tableName);
		return batches;
	}).map(function(batch) {
		return this._azure.tables.executeBatchAsync(tableName, batch);
	}, {concurrency: 4}).then(function() {
		log.info('batches saved');
	});
};

AuctionStore.prototype._getAuctionsTableName = function(region, realm, date) {
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
