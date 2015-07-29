var Promise = require('bluebird');
var azureSb = require('azure-sb');
var azureCommon = require('azure-common');

var log = require('../../../../log');

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

var eu = require('../../../../eu.json');
var realms = [];
var processed = {};
for (var x in eu) {
	if (!processed[x]) {
		realms.push(x);
	}
	processed[x] = true;
	eu[x].connections.forEach(function(connection) { processed[connection] = true; });
}


var messages = realms.map(function(realm) {
	return {
		Body: JSON.stringify({
			type: 'fetchAuction',
			region: 'eu',
			realm: realm
		})
	};
});

serviceBus.sendQueueMessageBatch('MyTopic', messages, function(res, msg) {
	console.log('posted', res, msg);
});
