var Promise = require('bluebird');
var request = require('request-promise');
var util = require('util');

var log = require('../../../../log').child({process: 'TaskExecutor'});
var realms = require('../../../../realms');
var bnet = require('../../../../bnet');
var Auctions = require('../../../../auction_house').Auctions;
var AuctionStore = require('../../../../auction_store');
var items = require('../../../../items');
var Executor = require('../../../../platform_services/executor');
var TaskQueue = require('../../../../platform_services/task_queue');
var Azure = require('../../../../platform_services/azure');
var User = require('../../../../user');

Promise.longStackTraces();

var ProcessFetchedAuctions = require('./process_fetched_auctions');
var SendNotifications = require('./send_notifications');

var azure = Azure.createFromEnv();
var blizzardKey = process.env.BNET_ID;
var auctionStore = new AuctionStore({
	azure: azure,
	log: log.child({service: 'AuctionStore'})
});

if (false) {
	return processMessage({body: "{\"type\":\"processFetchedAuction\",\"region\":\"eu\",\"realm\":\"lightbringer\"}"});
}

if (false) {
	return processMessage({body: "{\"type\":\"fetchAuctionData\",\"region\":\"eu\",\"realm\":\"lightbringer\",\"force\":true}"});
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
	queueName: 'MyTopic',
	log: log.child({service: 'TaskQueue'})
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
				return new ProcessFetchedAuctions({
					azure: azure,
					auctionStore: auctionStore,
					log: log,
					region: body.region,
					realm: body.realm
				}).run();

			case 'enqueueUserNotifications':
				return enqueueUserNotifications(body);

			case 'sendNotifications':
				return new SendNotifications({
					auctionStore: auctionStore,
					log: log,
					region: body.region,
					realm: body.realm,
					user: new User({
						id: body.userId,
						tables: azure.tables
					})
				}).run();

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
			log.info({age: age}, 'auction data saved')

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

			if (!opt.force && shouldExit) {
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

			return auctionStore.storeRawAuctions(opt.region, opt.realm, lastModified, auctionsRaw).then(function() {
				return {
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
		}).catch(function(err) {
			throw transientError(err);
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
		}).catch(function(err) {
			throw transientError(err);
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

	function transientError(cause) {
		var err = new Error();
		err.name = 'TransientError';
		err.cause = cause;
		return err;
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


