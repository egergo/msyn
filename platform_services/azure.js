var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var azureSb = require('azure-sb');
var zlib = require('zlib');
var Promise = require('bluebird');
var request = require('request-promise');

function Azure(opt) {
	opt = opt || {};
	if (!opt.storageConnectionString) { throw new Error('opt.storageConnectionString must be defined'); }
	if (!opt.serviceBusConnectionString) { throw new Error('opt.serviceBusConnectionString must be defined'); }

	var retryOperations = new azureCommon.ExponentialRetryPolicyFilter();

	var ent = azureStorage.TableUtilities.entityGenerator;
	var tables = azureStorage.createTableService(opt.storageConnectionString)
		.withFilter(retryOperations);
	var blobs = azureStorage.createBlobService(opt.storageConnectionString)
		.withFilter(retryOperations);
	var serviceBus = azureSb.createServiceBusService(opt.serviceBusConnectionString)
		.withFilter(retryOperations);

	Azure.extendServiceBusWithBatching(serviceBus);
	Azure.extendBlobsWithGzip(blobs);

	Promise.promisifyAll(blobs);
	Promise.promisifyAll(tables);
	Promise.promisifyAll(serviceBus);
	Promise.promisifyAll(zlib);

	this.ent = ent;
	this.tables = tables;
	this.blobs = blobs;
	this.serviceBus = serviceBus;
}

Azure.TableQuery = azureStorage.TableQuery;
Azure.TableBatch = azureStorage.TableBatch;
Azure.prototype.TableQuery = azureStorage.TableQuery;
Azure.prototype.TableBatch = azureStorage.TableBatch;

Azure.createFromEnv = function() {
	return new Azure({
		storageConnectionString: process.env.AZURE_STORAGE_CONNECTION_STRING,
		serviceBusConnectionString: process.env.AZURE_SB_CONNECTION_STRING
	});
};

Azure.extendServiceBusWithBatching = function(serviceBus) {
	/**
	 * Body, BrokerProperties, UserProperties, no limit on batch size
	 * https://msdn.microsoft.com/en-us/library/azure/dn798894.aspx
	 */
	var WebResource = azureCommon.WebResource;
	var Constants = azureCommon.Constants;
	var HeaderConstants = Constants.HeaderConstants;
	serviceBus.sendQueueMessageBatch = function(path, message, callback) {
		var webResource = WebResource.post(path + '/Messages');
		webResource.withHeader(HeaderConstants.CONTENT_TYPE, 'application/vnd.microsoft.servicebus.json');

		var processResponseCallback = function(responseObject, next) {
			var finalCallback = function(returnObject) {
				callback(returnObject.error, returnObject.response);
			};

			next(responseObject, finalCallback);
		};

		this.performRequest(webResource, JSON.stringify(message), null, processResponseCallback);
	};
};

Azure.extendBlobsWithGzip = function(blobs) {
	blobs.getBlobToBufferGzipAsync = function(container, path, options) {
		var gunzip = zlib.createGunzip();
		var promise = futureStream(gunzip);

		var az = this.getBlobToStreamAsync(container, path, gunzip, options);
		return Promise.all([promise, az]).spread(function(res, blobsResult) {
			return [res, blobsResult[1]];
		});
	};

	blobs.createBlockBlobFromTextGzipAsync = function(container, path, text, options) {
		if (!Buffer.isBuffer(text)) { text = new Buffer(text); }
		return zlib.gzipAsync(text).then(function(gzipped) {
			return blobs.createBlockBlobFromTextAsync(container, path, gzipped, options);
		});
	};

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
		stream.on('error', function(err) {
			resolver.reject(err);
		});
		return resolver.promise;
	}
};

module.exports = Azure;
