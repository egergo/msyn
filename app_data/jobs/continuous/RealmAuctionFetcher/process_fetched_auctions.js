"use strict";

var util = require('util');
var Promise = require('bluebird');
var Auctions = require('../../../../auction_house').Auctions;

function ProcessFetchedAuctions(opt) {
	if (!opt.azure) { throw new Error('opt.azure must be defined'); }
	if (!opt.auctionStore) { throw new Error('opt.auctionStore must be defined'); }
	if (!opt.log) { throw new Error('opt.log must be defined'); }
	if (!opt.region) { throw new Error('opt.region must be defined'); }
	if (!opt.realm) { throw new Error('opt.realm must be defined'); }

	this._azure = opt.azure;
	this._auctionStore = opt.auctionStore;
	this._log = opt.log;
	this._region = opt.region;
	this._realm = opt.realm;
}

ProcessFetchedAuctions.prototype.run = function() {
	return Promise.bind(this).then(function() {
		return this._auctionStore.getLastProcessedTime(this._region, this._realm);
	}).then(function(lastProcessed) {
		return this._auctionStore.getFetchedAuctionsSince(this._region, this._realm, lastProcessed).bind(this).reduce(function(lastProcessed, item) {
			return new AuctionProcessor({
				azure: this._azure,
				auctionStore: this._auctionStore,
				log: this._log,
				region: this._region,
				realm: this._realm,
				lastProcessed: lastProcessed,
				lastModified: item.lastModified,
			}).run().then(function() {
				return item.lastModified;
			});
		}, lastProcessed);
	}).then(function() {
		return this._enqueueUserNotifications();
	}).then(function() {
		return true;
	});
}

ProcessFetchedAuctions.prototype._enqueueUserNotifications = function() {
	return this._azure.serviceBus.sendQueueMessageAsync('MyTopic', {
		body: JSON.stringify({
			type: 'enqueueUserNotifications',
			region: this._region,
			realm: this._realm
		})
	});
}


/**
 * Processes a downloaded raw auction data file. Create a delta between
 * lastProcessed and lastModified files.
 *
 * @param {object} opt
 * @param {Azure} opt.azure
 * @param {Bunyan} opt.log
 * @param {string} opt.region
 * @param {string} opt.realm
 * @param {Date} [lastProcessed] date of the last processed data file
 * @param {Date} lastModified date of the raw auctions file to process
 */
function AuctionProcessor(opt) {
	if (!opt.azure) { throw new Error('opt.azure must be defined'); }
	if (!opt.log) { throw new Error('opt.log must be defined'); }
	if (!opt.region) { throw new Error('opt.region must be defined'); }
	if (!opt.realm) { throw new Error('opt.realm must be defined'); }
	if (!opt.auctionStore) { throw new Error('opt.auctionStore must be defined'); }
	if (!opt.lastModified) { throw new Error('opt.lastModified must be defined'); }

	this._azure = opt.azure;
	this._log = opt.log;
	this._region = opt.region;
	this._realm = opt.realm;
	this._auctionStore = opt.auctionStore;
	this._lastProcessed = opt.lastProcessed;
	this._lastModified = opt.lastModified;
}
ProcessFetchedAuctions.AuctionProcessor = AuctionProcessor;

AuctionProcessor.prototype.run = function() {
	return Promise.bind(this).then(function() {
		var pastPromise = undefined;
		if (this._lastProcessed.getTime()) {
			pastPromise = this._auctionStore.loadProcessedAuctions(this._region, this._realm, this._lastProcessed).bind(this).catch(function(err) {
				if (err.message === 'NotFound') { return this._log.warn({lastModified: this._lastModified, path: this._path, lastProcessed: this._lastProcessed}, 'past processed not found (maybe first run)'); }
				throw err;
			});
		}
		return Promise.all([
			pastPromise,
			this._auctionStore.loadRawAuctions(this._region, this._realm, this._lastModified)
		]);
	}).spread(function(past, current) {
		if (past) {
			current.applyPast(past);
		}
		return this._auctionStore.storeAuctions(current, this._region, this._realm);
	}).catch(function(err) {
		// maybe an old file got deleted
		// we need this clause to self-heal
		if (err.message === 'NotFound') {
			log.error({lastModified: this._lastModified, path: this._path, lastProcessed: this._lastProcessed}, 'fetched auction file not found');
			return;
		}
		throw err;
	});
}

module.exports = ProcessFetchedAuctions;
