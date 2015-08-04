
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var azureSb = require('azure-sb');
var Promise = require('bluebird');
var request = require('request-promise');
var zlib = require('zlib');
var util = require('util');

var log = require('../../../../log');
var realms = require('../../../../realms');
var bnet = require('../../../../bnet');
var Auctions = require('../../../../auction_house').Auctions;

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
		switch (body.type) {
			case 'fetchAuction':
				var region = body.region;
				var realm = body.realm;
				return fetchRealm(region, realm);

			case 'processFetchedAuction':
				return processFetchedAuction(body);

			default:
				throw new Error('unknown message type: ' + body.type);
		}
	});
}


function fetchRealm(region, realm) {
	// TODO: remove function local variable
	var slug;
	var name;

	var endpoint = bnet.mapRegionToEndpoint(region);
	return request({
		uri: endpoint.hostname + '/wow/auction/data/' + encodeURIComponent(realm),
		qs: {
			apikey: blizzardKey,
			locale: endpoint.defaultLocale
		},
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
			slug = auctions.realm.slug;
			return zlib.gzipAsync(new Buffer(auctionsRaw)).then(function(gzipped) {
				var date = new Date(fileLastModified);
				name = util.format('auctions/%s/%s/%s/%s/%s/%s.gzip', region, slug, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
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
		}).then(function() {
			return tables.insertOrReplaceEntityAsync('cache', {
				PartitionKey: entGen.String('snapshots-' + region + '-' + slug),
				RowKey: entGen.String('' + fileLastModified.getTime()),
				path: entGen.String(name),
				lastModified: entGen.DateTime(fileLastModified)
			});
		}).then(function() {
			return serviceBus.sendQueueMessageAsync('MyTopic', {
				body: JSON.stringify({
					type: 'processFetchedAuction',
					region: region,
					realm: slug
				})
			});
		})
	}).catch(function(err) {
		if (!err.notModified) { throw err; }
	});
}

function processFetchedAuction(opt) {

	// TODO: start lock
	return Promise.resolve().then(function() {
		return getLastProcessedTime();
	}).then(function(lastProcessed) {
		return getEntititesSinceLastProcessed(lastProcessed).spread(function(result, response) {
			//console.log(util.inspect(result.entries, {depth:null}));
			return Promise.reduce(result.entries, processItem, lastProcessed);
		});
	});

	function processItem(lastProcessed, item) {
		return Promise.resolve().then(function() {
			return Promise.all([
				loadPastAuctions(lastProcessed),
				loadFile(item.path._)
			]);
		}).spread(function(pastRaw, currentRaw) {
			// TODO: decide if buffer of object
			var current = new Auctions({lastModified: item.lastModified._, data: currentRaw});
			if (pastRaw) {
				var past = new Auctions({lastModified: lastProcessed, past: pastRaw});
				current.applyPast(past);
			}

			return Promise.all([
				saveCurrent(current),
				saveCurrentChanges(current)
			]);
		}).then(function() {
			return updateLastProcessed(item.lastModified._);
		}).then(function() {
			return item.lastModified._.getTime();
		});
	}

	function saveCurrent(auctions) {
		var raw = JSON.stringify(auctions._auctions);
		var date = auctions._lastModified;
		var name = util.format('processed/%s/%s/%s/%s/%s/%s.gzip', opt.region, opt.realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
		return zlib.gzipAsync(new Buffer(raw)).then(function(gzipped) {
			return blobs.createBlockBlobFromTextAsync('realms', name, gzipped);
		});
	}

	function saveCurrentChanges(auctions) {
		if (!auctions._changes) { return Promise.resolve(); }
		var raw = JSON.stringify(auctions._changes);
		var date = auctions._lastModified;
		var name = util.format('changes/%s/%s/%s/%s/%s/%s.gzip', opt.region, opt.realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
		return zlib.gzipAsync(new Buffer(raw)).then(function(gzipped) {
			return blobs.createBlockBlobFromTextAsync('realms', name, gzipped);
		});
	}

	function loadPastAuctions(lastProcessed) {
		// TODO: return lastProcessed object from previous iteration
		if (!lastProcessed) { return Promise.resolve(); }
		// TODO: make lastProcessed a date
		var date = new Date(lastProcessed);
		var name = util.format('processed/%s/%s/%s/%s/%s/%s.gzip', opt.region, opt.realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
		return loadFile(name).catch(function(err) {
			if (err.name === 'Error' && err.message === 'NotFound') {
				log.error({region: opt.region, realm: opt.realm, lastProcessed: lastProcessed, name: name}, 'last processed not found');
				return;
			}
			throw err;
		});
	}

	function futureStream(stream) {
		var bufs = [];
		var resolver = Promise.pending();
		stream.on('data', function(d) {
			bufs.push(d);
		});
		stream.on('end', function() {
			var buf = Buffer.concat(bufs);
			resolver.resolve(buf);
		});
		return resolver.promise;
	}

	function loadFile(path) {
		var gunzip = zlib.createGunzip();
		var promise = futureStream(gunzip);

		var az = blobs.getBlobToStreamAsync('realms', path, gunzip);
		return Promise.all([promise, az]).spread(function(res) {
			return JSON.parse(res);
		});
	}

	function getLastProcessedTime() {
		return tables.retrieveEntityAsync('cache', 'current-' + opt.region + '-' + opt.realm, '').spread(function(result) {
			return 0 || result.lastProcessed._.getTime();
		}).catch(function() {
			return 0;
		});
	}

	function getEntititesSinceLastProcessed(lastProcessed) {
		var q = new azureStorage.TableQuery()
			.where('PartitionKey == ? and RowKey > ?', 'snapshots-' + opt.region + '-' + opt.realm, '' + lastProcessed);
		return tables.queryEntitiesAsync('cache', q, null);
	}

	function updateLastProcessed(lastProcessed) {
		return tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: entGen.String('current-' + opt.region + '-' + opt.realm),
			RowKey: entGen.String(''),
			lastProcessed: entGen.DateTime(lastProcessed)
		});
	}
}

