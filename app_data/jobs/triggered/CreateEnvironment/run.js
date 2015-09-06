var Promise = require('bluebird');
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var azureSb = require('azure-sb');
var Azure = require('../../../../platform_services/azure');
var realms = require('../../../../realms');
var log = require('../../../../log').child({process: 'CreateEnvironment'});

var azure = Azure.createFromEnv();

module.exports = function() {
	return Promise.resolve().then(function() {
		var retryOperations = new azureCommon.ExponentialRetryPolicyFilter();

		var tables = azureStorage.createTableService(process.env.AZURE_STORAGE_CONNECTION_STRING)
			.withFilter(retryOperations);
		Promise.promisifyAll(tables);

		var blobs = azureStorage.createBlobService(process.env.AZURE_STORAGE_CONNECTION_STRING)
			.withFilter(retryOperations);
		Promise.promisifyAll(blobs);

		var serviceBus = azureSb.createServiceBusService(process.env.AZURE_SB_CONNECTION_STRING)
			.withFilter(retryOperations);
		Promise.promisifyAll(serviceBus);

		return Promise.resolve().then(function() {
			log.info('creating queue MyTopic...');
			return serviceBus.createQueueIfNotExistsAsync('MyTopic', {
				LockDuration: 'PT5M',
			});
		}).then(function() {
			log.info('creating table cache...');
			return tables.createTableIfNotExistsAsync('cache');
		}).then(function() {
			log.info('creating table users...');
			return tables.createTableIfNotExistsAsync('users');
		}).then(function() {
			log.info('creating table tasks...');
			return tables.createTableIfNotExistsAsync('tasks');
		}).then(function() {
			log.info('creating table RealmFetches...');
			return tables.createTableIfNotExistsAsync('RealmFetches');
		}).then(function() {
			log.info('adding realms into RealmFetched');
			return putRealmsIntoDatabase();
		}).then(function() {
			log.info('creating container realms...');
			return blobs.createContainerIfNotExistsAsync('realms');
		});

	}).then(function() {
		log.info('done');
	});



	function putRealmsIntoDatabase() {
		var result = [];
		realms.regions.forEach(function(region) {
			var processed = {};
			Object.keys(realms[region].bySlug).forEach(function(slug) {
				var real = realms[region].bySlug[slug].real;
				if (processed[real]) { return; }
				processed[real] = true;
				result.push({
					PartitionKey: azure.ent.String(''),
					RowKey: azure.ent.String(region + '-' + real),
					Enabled: azure.ent.Boolean(process.env.ENABLED_REALMS ? process.env.ENABLED_REALMS.indexOf(region + '-' + real) > -1 : true),
					Region: azure.ent.String(region),
					Realm: azure.ent.String(real)
				});
			});
		});

		var chunks = [];
		while (result.length) {
			chunks.push(result.splice(0, 100));
		}

		var batches = chunks.map(function(chunk) {
			var batch = new azure.TableBatch();
			chunk.forEach(function(item) {
				batch.insertOrMergeEntity(item);
			});
			return batch;
		});

		return Promise.map(batches, function(batch) {
			return azure.tables.executeBatchAsync('RealmFetches', batch);
		});
	}

};

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
