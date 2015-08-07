
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
var Executor = require('../../../../platform_services/executor');
var TaskQueue = require('../../../../platform_services/task_queue');

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

}

	});
}

var executor = new Executor({concurrency: 4});
var taskQueue = new TaskQueue({
	serviceBus: serviceBus,
	executor: executor,
	queueName: 'MyTopic'
});

taskQueue.run(processMessage).then(function() {
	console.log('done');
}).catch(function(err) {
	console.log(err.stack);
});

function processMessage(message) {
	return Promise.resolve().then(function() {
		var body = JSON.parse(message.body);
		switch (body.type) {
			case 'fetchAuction':
				return fetchRealm(body);

			case 'processFetchedAuction':
				return processFetchedAuction(body);

			default:
				throw new Error('unknown message type: ' + body.type);
		}
	});
}


function fetchRealm(opt) {
	var region = opt.region;
	var realm = opt.realm;
	if (realms[region].bySlug[realm]) {
		realm = realms[region].bySlug[realm].real;
	}

	return Promise.resolve().then(function() {
		return fetchAuctionDescription();
	}).then(function(desc) {
		return checkLastModified(desc.url, desc.lastModified).then(function() {
			return fetchAndSaveRealmToStorage(desc.url).then(function(res) {
				return saveLastModified(desc.url, res.lastModified).then(function() {
					return addToSnapshots(res.path, res.lastModified);
				}).then(function() {
					return enqueueRealmToProcess();
				});
			});
		});
	}).catch(function(err) {
		if (!err.notModified) { throw err; }
	});

	function fetchAuctionDescription() {
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
			return {
				url: file.url,
				lastModified: new Date(file.lastModified)
			};
		});
	}

	function checkLastModified(url, lastModified) {
		if (opt.force) { return Promise.resolve(); }

		return tables.retrieveEntityAsync('cache', 'fetches', encodeURIComponent(url)).spread(function(entity) {
			return entity.lastModified._;
		}).catch(function(err) {
			if (err.code === 'ResourceNotFound') { return null; }
			throw err;
		}).then(function(cacheLastModified) {
			if (lastModified <= cacheLastModified) {
				console.log('file not modified. region:', region, 'realm:', realm);
				var err = new Error('file not modified');
				err.notModified = true;
				throw err;
			}
		});
	}

	function fetchAndSaveRealmToStorage(url) {
		return request({
			uri: url,
			gzip: true,
			resolveWithFullResponse: true
		}).then(function(res) {
			var lastModified = new Date(res.headers['last-modified']);
			if (!lastModified.getDate()) { throw new Error('invalid Last-Modified value: ' + res.headers['last-modified']); }
			var auctionsRaw = res.body;

			// check integrity of the received JSON
			var auctions = JSON.parse(auctionsRaw);
			var slug = auctions.realm.slug;
			if (realm !== slug) {
				throw new Error('realm name mismatch: ', region, realm, ' !=', slug);
			}

			return zlib.gzipAsync(new Buffer(auctionsRaw)).then(function(gzipped) {
				var date = lastModified;
				var name = util.format('auctions/%s/%s/%s/%s/%s/%s.gzip', region, realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
				console.log('storing file. name:', name, 'size:', gzipped.length, 'originalSize:', auctionsRaw.length);

				log.debug('saving blob');
				return blobs.createBlockBlobFromTextAsync('realms', name, gzipped).then(function() {
					return {
						path: name,
						lastModified: lastModified
					};
				});
			});
		});
	}

	/**
	 * @param {string} url
	 * @param {date} lastModified
	 */
	function saveLastModified(url, lastModified) {
		return tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: entGen.String('fetches'),
			RowKey: entGen.String(encodeURIComponent(url)),
			lastModified: entGen.DateTime(lastModified)
		});
	}

	/**
	 * @param {string} path
	 * @param {date} lastModified
	 */
	function addToSnapshots(path, lastModified) {
		return tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: entGen.String('snapshots-' + region + '-' + realm),
			RowKey: entGen.String('' + lastModified.getTime()),
			path: entGen.String(path),
			lastModified: entGen.DateTime(lastModified)
		});
	}

	function enqueueRealmToProcess() {
		return serviceBus.sendQueueMessageAsync('MyTopic', {
			body: JSON.stringify({
				type: 'processFetchedAuction',
				region: region,
				realm: realm
			})
		});
	}


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

