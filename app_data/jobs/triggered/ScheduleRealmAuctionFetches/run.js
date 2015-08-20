var Promise = require('bluebird');
var util = require('util');

var log = require('../../../../log');
var parentlog = log;
var realms = require('../../../../realms');
var bnet = require('../../../../bnet');
var Azure = require('../../../../platform_services/azure');

var azure = Azure.createFromEnv();

function doOrDoNotThereIsNoTry() {
	var log = parentlog.child({task: 'scheduleRealmAuctionFetches', run: new Date()});
	return Promise.resolve().then(function() {
		log.info('reading RealmFetches table');
		return getCheckableRealms(log);
	}).then(function(realms) {
		log.info({length: realms.length}, '%s updatable realm found', realms.length);
		return Promise.map(realms, checkRealm.bind(null, log), {concurrency: 10});
	}).then(function(res) {
		res = res.filter(function(item) { return !!item; });
		return saveResults(res, log);
	}).then(function(tasks) {
		log.info('enqueuing %s tasks', tasks.length)
		return enqueueFetchTasks(tasks);
	}).then(function(res) {
		log.info('done');
	}).catch(function(err) {
		log.error({err: err}, 'error');
	});
}

function getCheckableRealms() {
	var old = new Date().getTime() - 2.5 * 60 * 60 * 1000; // 2.5 hours ago
	var q = new azure.TableQuery();
	return azure.tables.queryEntitiesAsync('RealmFetches', q, null).spread(function(res) {
		if (res.continuationToken) { log.error('RealmFetches returned a continuationToken'); }
		return res.entries.filter(function(item) {
			if (item.Enabled && !item.Enabled._) {
				return false;
			}

			if (item.LastModified) {
				if (item.LastModified._ < old) {
					return true;
				}
				return item.LastFetched && item.LastModified._ <= item.LastFetched._;
			}

			return true;
		});
	});
}

function checkRealm(parentlog, row) {
	var region = row.Region ? row.Region._ : undefined;
	var realm = row.Realm ? row.Realm._ : undefined;
	if (!region || !realm) {
		parentlog.error({row: row}, 'corrupt row in RealmFetches table');
		return Promise.resolve();
	}

	var log = parentlog.child({region: region, realm: realm});
	return Promise.resolve().then(function() {
		log.info('fetching auction data status...');
		return bnet.getAuctionDataStatus({
			region: region,
			realm: realm,
			accessToken: process.env.BNET_ID
		});
	}).then(function(res) {
		var lastModified = new Date(res.lastModified);
		var prevLastModified = row.LastModified ? row.LastModified._ : undefined;
		var age = prevLastModified ? lastModified - prevLastModified : undefined;
		if (prevLastModified && !age) {
			log.info('no update found');
			return;
		}
		log.info({lastModified: lastModified, prevLastModified: prevLastModified, age: age}, 'update found');
		return {
			region: region,
			realm: realm,
			lastModified: lastModified,
			url: res.url
		};
		return {
			PartitionKey: row.PartitionKey,
			RowKey: row.RowKey,
			LastModified: azure.ent.DateTime(lastModified),
			URL: azure.ent.String(res.url)
		};
	}).catch(function(err) {
		if (region === 'tw' && err.name === 'StatusCodeError' && err.statusCode === 404) {
			// ignoring stupid 404s from Taiwan
			return;
		}
		log.error({err: err}, 'error fetching auction data status');
	});
}

function saveResults(res, log) {
	return Promise.resolve().then(function() {
		var all = res.map(function(item) {
			return {
				PartitionKey: azure.ent.String(''),
				RowKey: azure.ent.String(item.region + '-' + item.realm),
				LastModified: azure.ent.DateTime(item.lastModified),
				URL: azure.ent.String(item.url)
			};
		});
		var chunks = [];
		while (all.length) {
			chunks.push(all.splice(0, 100));
		}

		var batches = chunks.map(function(chunk) {
			var batch = new azure.TableBatch();
			chunk.forEach(function(item) {
				batch.mergeEntity(item);
			});
			return batch;
		});

		log.info('saving %s batches...', batches.length);
		return Promise.map(batches, function(batch) {
			return azure.tables.executeBatchAsync('RealmFetches', batch);
		}).then(function(result) {
			result.forEach(function(batchResult) {
				var first = batchResult[0];
				first.forEach(function(itemResult) {
					if (itemResult.error) {
						log.error({err: itemResult.error, result: itemResult}, 'could not row');
					}
				});
			});
			return res;
		});
	});
}

function enqueueFetchTasks(tasks) {
	return Promise.resolve().then(function() {
		if (!tasks.length) { return; }

		var messages = tasks.map(function(task) {
			return {
				Body: JSON.stringify({
					type: 'fetchAuctionData',
					region: task.region,
					realm: task.realm
				})
			};
		});

		return azure.serviceBus.sendQueueMessageBatchAsync('MyTopic', messages);
	});
};


module.exports = doOrDoNotThereIsNoTry;


function safeExit(code) {
	setTimeout(function() {
		process.exit(code);
	}, 1000);
}


if (require.main === module) {
	module.exports().then(function() {
		safeExit();
	}).catch(function(err) {
		log.error({err: err}, 'createEnvironment error:', err.stack);
		safeExit(1);
	});
}
