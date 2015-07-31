var Promise = require('bluebird');
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var azureSb = require('azure-sb');

var log = require('../../../../log');

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
			return serviceBus.createQueueIfNotExistsAsync('MyTopic');
		}).then(function() {
			log.info('creating table cache...');
			return tables.createTableIfNotExistsAsync('cache');
		}).then(function() {
			log.info('creating container realms...');
			return blobs.createContainerIfNotExistsAsync('realms');
		});

	}).then(function() {
		log.info('done');
	})
};

if (require.main === module) {
  module.exports().catch(function(err) {
  	log.error({err: err}, 'createEnvironment error:', err.stack);
  });
}