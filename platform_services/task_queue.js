var Promise = require('bluebird');
var azureStorage = require('azure-storage');

var entGen = azureStorage.TableUtilities.entityGenerator;

function TaskQueue(opt) {
	opt = opt || {};
	if (!opt.azure) { throw new Error('opt.azure must be defined'); }
	if (!opt.queueName) { throw new Error('opt.queueName must be defined'); }
	if (!opt.log) { throw new Error('opt.log must be defined'); }

	this._serviceBus = opt.azure.serviceBus;
	this._tables = opt.azure.tables;
	this._queueName = opt.queueName;
	this._executor = opt.executor;
	this._log = opt.log;
}

TaskQueue.prototype.run = function(callback) {
	if (!this._executor) { throw new Error('opt.executor must be defined in the constructor to use this'); }

	var self = this;
	var backoff = 0;

	return repeatMePlease();

	function repeatMePlease() {
		return self._executor.wait().then(function() {
			return receiveMessage().spread(function(message) {
				// breaking promise chain
				self._executor.execute(processMessage(message));
			});
		}).then(function() {
			backoff = 0;
		}).catch(function(err) {
			backoff = Math.min(Math.max(backoff * 2, 1000), 60000);
			self._log.error({err: err, backoff: backoff}, 'task queue error');
			return Promise.delay(backoff);
		}).finally(repeatMePlease);
	}

	function receiveMessage() {
		return self._serviceBus.receiveQueueMessageAsync(self._queueName, {
			isPeekLock: true,
			timeoutIntervalInS: 60 * 60 * 24
		}).catch(function(err) {
			if (err.message === 'No messages to receive') { return receiveMessage(); }
			throw err;
		});
	}

	function processMessage(message) {
		var now = new Date();
		var time = process.hrtime();
		var messageQueueDate = new Date(message.brokerProperties.EnqueuedTimeUtc);
		var delay = Math.max(0, now - messageQueueDate - 1000);
		self._log.debug({message: message, delay: delay, tries: message.brokerProperties.DeliveryCount}, 'incoming message', message.brokerProperties.MessageId);

		var error;
		var result;

		return Promise.resolve().then(function() {
			return callback(message);
		}).then(function(res) {
			result = res;
			return self._serviceBus.deleteMessageAsync(message).catch(function(err) {
				self._log.warn({err: err, message: message}, 'could not delete message', err.stack);
			});
		}).catch(function(err) {
			error = err;
			self._log.error({err: err, message: message}, 'error executing message callback');
			if (process.env.STOP_ON_ERROR === '1') { process.exit(1); }
			if (message.brokerProperties.DeliveryCount >= 5) {
				self._log.error({message: message}, 'removing poison message');
				return self._serviceBus.deleteMessageAsync(message).catch(function(err) {
					self._log.warn({err: err, message: message}, 'could not delete message:', err.stack);
				});
			} else {
				return self._serviceBus.unlockMessageAsync(message).catch(function(err) {
					self._log.warn({err: err, message: message}, 'could not unlock message:', err.stack);
				});
			}
		}).finally(function() {
			var diff = process.hrtime(time);
			var ms = diff[0] * 1000 + Math.floor(diff[1] / 1e6);
			self._log.debug({ms: ms, all: delay + ms}, 'message processed');

			// async run
			// reportMessage(delay, ms, message, error, result);
		});
	}

	function reportMessage(timeToReceive, timeToProcess, message, error, result) {
		return Promise.resolve().then(function() {
			if (!self._tables) { throw new Error('cannot report without opt.tables'); }

			var now = new Date();
			return self._tables.insertEntityAsync('tasks', {
				PartitionKey: entGen.String('' + now.getTime()),
				RowKey: entGen.String(message.brokerProperties.MessageId + '-' + message.brokerProperties.DeliveryCount),
				reported: entGen.DateTime(now),
				timeToReceive: entGen.Int64(timeToReceive),
				timeToProcess: entGen.Int64(timeToProcess),
				isSuccessful: entGen.Boolean(!!error),
				body: entGen.String(message.body),
				retry: entGen.Int32(message.brokerProperties.DeliveryCount),
				error: entGen.String('' + error),
				result: entGen.String(JSON.stringify(result))
			});
		}).catch(function(err) {
			self._log.warn({
				err: err,
				timeToReceive: timeToReceive,
				timeToProcess: timeToProcess,
				message: message,
				result: result,
				error: error
			}, 'could not report message');
		});
	}
};

module.exports = TaskQueue;
