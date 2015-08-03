var Promise = require('bluebird');
var azureSb = require('azure-sb');
var azureCommon = require('azure-common');

var log = require('../../../../log');
var realms = require('../../../../realms');

function extendServiceBusService() {
	/**
	 * Body, BrokerProperties, UserProperties, no limit on batch size
	 * https://msdn.microsoft.com/en-us/library/azure/dn798894.aspx
	 */
	var azureSb = require('azure-sb');
	var azureCommon = require('azure-common');
	var WebResource = azureCommon.WebResource;
	var Constants = azureCommon.Constants;
	var HeaderConstants = Constants.HeaderConstants;
	azureSb.ServiceBusService.prototype.sendQueueMessageBatch = function (path, message, callback) {
	  var webResource = WebResource.post(path + '/Messages');
	  webResource.withHeader(HeaderConstants.CONTENT_TYPE, 'application/vnd.microsoft.servicebus.json');

	  var processResponseCallback = function (responseObject, next) {
	    var finalCallback = function (returnObject) {
	      callback(returnObject.error, returnObject.response);
	    };

	    next(responseObject, finalCallback);
	  };

	  this.performRequest(webResource, JSON.stringify(message), null, processResponseCallback);
	};
}
extendServiceBusService();




var serviceBus = azureSb.createServiceBusService(process.env.AZURE_SB_CONNECTION_STRING)
	.withFilter(new azureCommon.ExponentialRetryPolicyFilter())
Promise.promisifyAll(serviceBus);


var realmsToProcess = [];

['eu', 'us', 'tw', 'kr'].forEach(function(region) {
	var processed = {};
	Object.keys(realms[region].bySlug).forEach(function(slug) {
		if (!processed[slug]) {
			realmsToProcess.push({region: region, slug: slug});
		}
		processed[slug] = true;
		realms[region].bySlug[slug].connections.forEach(function(connection) {
			processed[connection] = true;
		});
	});
});

var messages = realmsToProcess.map(function(realm) {
	return {
		Body: JSON.stringify({
			type: 'fetchAuction',
			region: realm.region,
			realm: realm.slug
		})
	};
});

serviceBus.sendQueueMessageBatch('MyTopic', messages, function(res, msg) {
	console.log('posted', res, msg);
});
