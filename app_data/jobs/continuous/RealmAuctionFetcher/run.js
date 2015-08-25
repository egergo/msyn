var Promise = require('bluebird');
var request = require('request-promise');
var util = require('util');

var log = require('../../../../log');
var realms = require('../../../../realms');
var bnet = require('../../../../bnet');
var Auctions = require('../../../../auction_house').Auctions;
var AuctionStore = require('../../../../auction_store');
var items = require('../../../../items');
var Executor = require('../../../../platform_services/executor');
var TaskQueue = require('../../../../platform_services/task_queue');
var Azure = require('../../../../platform_services/azure');

var azure = Azure.createFromEnv();
var blizzardKey = process.env.BNET_ID;
var auctionStore = new AuctionStore({azure: azure});

if (false) {
	return processFetchedAuction({
		type: 'processFetchedAuction',
		region: 'eu',
		realm: 'lightbringer'
	}).catch(function(err) {
		console.error('bazge', err.stack);
	}).then(function() {
		console.log('done');
	});
}

if (false) {
	enqueueUserNotifications({
		region: 'eu',
		realm: 'mazrigos'
	});
	return;
}

var TASK_CONCURRENCY = Number(process.env.TASK_CONCURRENCY) || 2;

log.info({concurrency: TASK_CONCURRENCY}, 'starting task queue');

var executor = new Executor({concurrency: TASK_CONCURRENCY});
var taskQueue = new TaskQueue({
	azure: azure,
	executor: executor,
	queueName: 'MyTopic'
});

taskQueue.run(processMessage).then(function() {
	log.info('done');
}).catch(function(err) {
	log.error({err: err}, 'TaskQueue error');
});

function processMessage(message) {
	return Promise.resolve().then(function() {
		var body = JSON.parse(message.body);
		switch (body.type) {
			case 'fetchAuction':
				return fetchRealm(body);

			case 'processFetchedAuction':
				return processFetchedAuction(body);

			case 'enqueueUserNotifications':
				return enqueueUserNotifications(body);

			case 'sendNotifications':
				//log.error({message: body}, 'TODO: send notifications');
				return;

			case 'fetchAuctionData':
				return fetchAuctionData(body);

			default:
				throw new Error('unknown message type: ' + body.type);
		}
	});
}

function fetchAuctionData(opt) {

	return Promise.resolve().then(function() {
		return checkStatusTable(opt.region, opt.realm);
	}).then(function(status) {
		return fetchAndSaveRealmToStorage(status.url).then(function(savedParams) {
			var age = status.lastFetched ? savedParams.lastModified - status.lastFetched : undefined;
			log.info({path:savedParams.path, age: age}, 'auction data saved to %s', savedParams.path)

			return addToSnapshots(savedParams.path, savedParams.lastModified).then(function() {
				return updateStatusTable(savedParams.lastModified);
			});
		});
	}).then(function() {
		return enqueueRealmToProcess();
	}).catch(function(err) {
		if (err.notModified) { return; }
		throw err;
	});

	function checkStatusTable(region, realm) {
		return azure.tables.retrieveEntityAsync('RealmFetches', '', region + '-' + realm).spread(function(res) {
			var shouldExit = false;
			if (res.Enabled && !res.Enabled._) {
				shouldExit = true;
			}

			if (!shouldExit && res.LastFetched && res.LastFetched._ >= res.LastModified._) {
				shouldExit = true;
			}

			if (shouldExit) {
				var err = new Error('not modified');
				err.notModified = true;
				throw err;
			}

			return {
				lastFetched: res.LastFetched ? res.LastFetched._ : undefined,
				url: res.URL._
			};
		});
	}

	function updateStatusTable(lastModified) {
		return azure.tables.mergeEntityAsync('RealmFetches', {
			PartitionKey: azure.ent.String(''),
			RowKey: azure.ent.String(opt.region + '-' + opt.realm),
			LastFetched: azure.ent.DateTime(lastModified)
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

			// TODO: this is disabled while new realm format is adopted
			// check if this realm is contained
			// var found = auctions.realms.filter(function(item) {
			// 	return item.slug === opt.realm;
			// });
			// if (!found.length) {
			// 	throw new Error(util.format('realm name mismatch: region=%s realm=%s realms=%s', opt.region, opt.realm, JSON.stringify(auctions.realms)));
			// }

			var date = lastModified;
			var name = util.format('auctions/%s/%s/%s/%s/%s/%s.gz', opt.region, opt.realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
			log.info('storing file. name:', name, 'originalSize:', auctionsRaw.length);
			return azure.blobs.createBlockBlobFromTextGzipAsync('realms', name, auctionsRaw).then(function() {
				return {
					path: name,
					lastModified: lastModified
				};
			});
		});
	}

	/**
	 * @param {string} path
	 * @param {date} lastModified
	 */
	function addToSnapshots(path, lastModified) {
		return azure.tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: azure.ent.String('snapshots-' + opt.region + '-' + opt.realm),
			RowKey: azure.ent.String('' + lastModified.getTime()),
			path: azure.ent.String(path),
			lastModified: azure.ent.DateTime(lastModified)
		});
	}

	function enqueueRealmToProcess() {
		return azure.serviceBus.sendQueueMessageAsync('MyTopic', {
			body: JSON.stringify({
				type: 'processFetchedAuction',
				region: opt.region,
				realm: opt.realm
			})
		});
	}
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
	}).then(function() {
		return true;
	}).catch(function(err) {
		if (!err.notModified) { throw err; }
		return false;
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

		return azure.tables.retrieveEntityAsync('cache', 'fetches', encodeURIComponent(url)).spread(function(entity) {
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

			var date = lastModified;
			var name = util.format('auctions/%s/%s/%s/%s/%s/%s.gz', region, realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());			
			console.log('storing file. name:', name, 'originalSize:', auctionsRaw.length);
			return azure.blobs.createBlockBlobFromTextGzipAsync('realms', name, auctionsRaw).then(function() {
				return {
					path: name,
					lastModified: lastModified
				};
			});
		});
	}

	/**
	 * @param {string} url
	 * @param {date} lastModified
	 */
	function saveLastModified(url, lastModified) {
		return azure.tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: azure.ent.String('fetches'),
			RowKey: azure.ent.String(encodeURIComponent(url)),
			lastModified: azure.ent.DateTime(lastModified)
		});
	}

	/**
	 * @param {string} path
	 * @param {date} lastModified
	 */
	function addToSnapshots(path, lastModified) {
		return azure.tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: azure.ent.String('snapshots-' + region + '-' + realm),
			RowKey: azure.ent.String('' + lastModified.getTime()),
			path: azure.ent.String(path),
			lastModified: azure.ent.DateTime(lastModified)
		});
	}

	function enqueueRealmToProcess() {
		return azure.serviceBus.sendQueueMessageAsync('MyTopic', {
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
	}).then(function() {
		return azure.serviceBus.sendQueueMessageAsync('MyTopic', {
			body: JSON.stringify({
				type: 'enqueueUserNotifications',
				region: opt.region,
				realm: opt.realm
			})
		});
	}).then(function() {
		return true;
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
			return auctionStore.storeAuctions(current, opt.region, opt.realm);
		}).catch(function(err) {
			// maybe an old file got deleted
			if (err.message === 'NotFound') {
				log.warn({item: item, lastProcessed: lastProcessed}, 'fetched auction file not found');
				return;
			}
			throw err;
		}).then(function() {
			return updateLastProcessed(item.lastModified._);
		}).then(function() {
			return item.lastModified._.getTime();
		});
	}

	function loadPastAuctions(lastProcessed) {
		// TODO: return lastProcessed object from previous iteration
		if (!lastProcessed) { return Promise.resolve(); }
		// TODO: re-enable
		return Promise.resolve();
		// TODO: make lastProcessed a date
		var date = new Date(lastProcessed);
		var name = util.format('processed/%s/%s/%s/%s/%s/%s.gz', opt.region, opt.realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
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
		return azure.blobs.getBlobToBufferGzipAsync('realms', path).spread(function(res) {
			return JSON.parse(res);
		});
	}

	function getLastProcessedTime() {
		return azure.tables.retrieveEntityAsync('cache', 'current-' + opt.region + '-' + opt.realm, '').spread(function(result) {
			return 0 || result.lastProcessed._.getTime();
		}).catch(function() {
			return 0;
		});
	}

	function getEntititesSinceLastProcessed(lastProcessed) {
		var q = new azure.TableQuery()
			.where('PartitionKey == ? and RowKey > ?', 'snapshots-' + opt.region + '-' + opt.realm, '' + lastProcessed);
		return azure.tables.queryEntitiesAsync('cache', q, null);
	}

	function updateLastProcessed(lastProcessed) {
		return azure.tables.insertOrReplaceEntityAsync('cache', {
			PartitionKey: azure.ent.String('current-' + opt.region + '-' + opt.realm),
			RowKey: azure.ent.String(''),
			lastProcessed: azure.ent.DateTime(lastProcessed)
		});
	}
}


function enqueueUserNotifications(opt) {
	opt.realm = realms[opt.region].bySlug[opt.realm].real;
	var usersToNotify = [];

	return runQueryUsers(null).then(function() {
		if (usersToNotify.length === 0) { return; }

		var batch = usersToNotify.map(function(userId) {
			return {
				Body: JSON.stringify({
					type: 'sendNotifications',
					region: opt.region,
					realm: opt.realm,
					userId: userId
				})
			}
		});
		return azure.serviceBus.sendQueueMessageBatchAsync('MyTopic', batch);
	});

	function processUser(user) {
		var regionCharacters = user['characters_' + opt.region];
		if (!regionCharacters) { return; }
		regionCharacters = JSON.parse(regionCharacters._);
		regionCharacters.characters.some(function(character) {
			var characterRealRealm = realms[character.region].bySlug[character.realm].real;
			if (characterRealRealm !== opt.realm) { return; }
			usersToNotify.push(user.PartitionKey._);
			return true;
		});
	}

	function processUsers(users) {
		users.forEach(processUser);
	}

	function runQueryUsers(continuationToken) {
		var q = new azure.TableQuery();
		return azure.tables.queryEntitiesAsync('users', q, continuationToken).spread(function(result) {
			processUsers(result.entries);
			if (result.continuationToken) {
				return runQueryUsers(result.continuationToken);
			}
		});
	}
}


