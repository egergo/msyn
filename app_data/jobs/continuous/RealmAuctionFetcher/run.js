
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var azureSb = require('azure-sb');
var Promise = require('bluebird');
var request = require('request-promise');
var zlib = require('zlib');
var util = require('util');

var log = require('../../../../log');

Promise.promisifyAll(zlib);

var retryOperations = new azureCommon.ExponentialRetryPolicyFilter();

var entGen = azureStorage.TableUtilities.entityGenerator;
var tables = azureStorage.createTableService(process.env.AZURE_STORAGE_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(tables);

var blobs = azureStorage.createBlobService(process.env.AZURE_STORAGE_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(blobs);

var serviceBus = azureSb.createServiceBusService(process.env.AZURE_SB_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(serviceBus);

var blizzardKey = process.env.BNET_ID;

function Executor() {
	this._available = 4;
	this._id = 1;
	this._busy = {};

	this._waiter;
}

Executor.prototype.wait = function() {
	if (this._available > 0) {
		return Promise.resolve(this._available);
	} else {
		if (!this._waiter) {
			this._waiter = Promise.pending();
		}
		return this._waiter.promise.bind(this).then(function() {
			return this.wait();
		});
	}
}

Executor.prototype.schedule = function(promise) {
	if (this._available <= 0) { throw new Error('cannot schedule'); }
	this._available--;
	var id = this._id++;
	this._busy[id] = promise;
	return Promise.bind(this).then(function() {
		return promise;
	}).finally(function() {
		delete this._busy[id];
		this._available++;
		if (this._waiter) {
			var waiter = this._waiter;
			delete this._waiter;
			waiter.resolve();
		}
	});
}

var executor = new Executor;
var backoff = 0;

function endless() {
	Promise.resolve().then(function() {

		function run() {
			return Promise.resolve().then(function() {
				log.debug('waiting for slot');
				return executor.wait();
			}).then(function() {
				log.debug('getting message');
				return serviceBus.receiveQueueMessageAsync('MyTopic', {isPeekLock: true, timeoutIntervalInS: 60 * 60 * 24})
			}).spread(function(message) {
				executor.schedule(Promise.resolve().then(function() {
					log.debug({message: message}, 'incoming message', message.brokerProperties.MessageId);
					return processMessage(message);
				})).then(function() {
					log.debug('deleting message', message.brokerProperties.MessageId);
					return serviceBus.deleteMessageAsync(message).catch(function(err) {
						log.warn({err: err, message: message}, 'could not delete message:', err.stack);
					});
				}).catch(function(err) {
					log.error({err: err, message: message}, 'error executing message:', err.stack);
					if (message.brokerProperties.DeliveryCount >= 5) {
						log.error({message: message}, 'removing poison message');
						return serviceBus.deleteMessageAsync(message).catch(function(err) {
							log.warn({err: err, message: message}, 'could not delete message:', err.stack);
						});
					} else {
						return serviceBus.unlockMessageAsync(message).catch(function(err) {
							log.warn({err: err, message: message}, 'could not unlock message:', err.stack);
						});
					}
				});
			}).catch(function(err) {
				if (err.message !== 'No messages to receive') { throw err; }
				log.debug('timeout');
			}).then(run);
		}

		return run();

	}).then(function() {
		backoff = 0;
	}).catch(function(err) {
		backoff = Math.min(Math.max(backoff * 2, 1000), 60000);
		log.error({err: err, backoff: backoff}, err.stack);
		return Promise.delay(backoff);
	}).finally(endless);

}


Promise.resolve().then(function() {
	return endless();
});


function processMessage(message) {
	return Promise.resolve().then(function() {
		var body = JSON.parse(message.body);
		if (body.type !== 'fetchAuction') { return; }
		var region = body.region;
		var realm = body.realm;
		return fetchRealm(region, realm);
	})

	return Promise.delay(10000);
}


function fetchRealm(region, realm) {
	var url = util.format('https://%s.api.battle.net/wow/auction/data/%s?locale=%s&apikey=%s', region, realm, 'en_GB', encodeURIComponent(blizzardKey));
	return request({
		uri: url,
		gzip: true
	}).then(function(auctionDesc) {
		auctionDesc = JSON.parse(auctionDesc);

		var file = auctionDesc.files[0];
		var fileLastModified = new Date(file.lastModified);
		var fileUrl = file.url;

		log.debug('getting last modified', fileUrl);
		return tables.retrieveEntityAsync('cache', 'fetches', encodeURIComponent(fileUrl)).spread(function(entity) {
			return entity.lastModified._;
		}).catch(function(err) {
			if (err.code === 'ResourceNotFound') { return null; }
			console.log('err', err);
			throw err;
		}).then(function(cacheLastModified) {
			log.debug('got last modified');
			if (fileLastModified <= cacheLastModified) {
				console.log('file not modified. region:', region, 'realm:', realm);
				var err = new Error('file not modified');
				err.notModified = true;
				throw err;
			}

			return request({
				uri: file.url,
				gzip: true
			});
		}).then(function(auctionsRaw) {
			var auctions = JSON.parse(auctionsRaw);
			var slug = auctions.realm.slug;
			return zlib.gzipAsync(new Buffer(auctionsRaw, 'binary')).then(function(gzipped) {
				var date = new Date(fileLastModified);
				var name = util.format('auctions/%s/%s/%s/%s/%s/%s.gzip', region, slug, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
				console.log('storing file. name:', name, 'size:', gzipped.length, 'originalSize:', auctionsRaw.length);

				log.debug('saving blob');
				return blobs.createBlockBlobFromTextAsync('realms', name, gzipped).then(function() {
					log.debug('saved blob');
				});
			});
		}).then(function() {
			log.debug('saving last modified');
			return tables.insertOrReplaceEntityAsync('cache', {
				PartitionKey: entGen.String('fetches'),
				RowKey: entGen.String(encodeURIComponent(fileUrl)),
				lastModified: entGen.DateTime(fileLastModified)
			}).then(function() {
				log.debug('saved last modified');
			});
		});
	}).catch(function(err) {
		if (!err.notModified) { throw err; }
	});
}

