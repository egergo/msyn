var Promise = require('bluebird');

var log = require('../log');

function TaskQueue(opt) {
	opt = opt || {};
	if (!opt.serviceBus) { throw new Error('opt.serviceBus must be defined'); }
	if (!opt.queueName) { throw new Error('opt.queueName must be defined'); }

	this._serviceBus = opt.serviceBus;
	this._queueName = opt.queueName;
	this._executor = opt.executor;
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
			log.error({err: err, backoff: backoff}, 'task queue error');
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
		log.debug({message: message, delay: delay, tries: message.brokerProperties.DeliveryCount}, 'incoming message', message.brokerProperties.MessageId);

		return Promise.resolve().then(function() {
			return callback(message);
		}).then(function() {
			return self._serviceBus.deleteMessageAsync(message).catch(function(err) {
				log.warn({err: err, message: message}, 'could not delete message', err.stack);
			});
		}).catch(function(err) {
			log.error({err: err, message: message}, 'error executing message callback');
			if (process.env.STOP_ON_ERROR === '1') { process.exit(1); }
			if (message.brokerProperties.DeliveryCount >= 5) {
				log.error({message: message}, 'removing poison message');
				return self._serviceBus.deleteMessageAsync(message).catch(function(err) {
					log.warn({err: err, message: message}, 'could not delete message:', err.stack);
				});
			} else {
				return self._serviceBus.unlockMessageAsync(message).catch(function(err) {
					log.warn({err: err, message: message}, 'could not unlock message:', err.stack);
				});
			}
		}).finally(function() {
			var diff = process.hrtime(time);
			var ms = diff[0] * 1000 + Math.floor(diff[1] / 1e6);
			log.debug({ms: ms, all: delay + ms}, 'message processed');
		});
	}
};

module.exports = TaskQueue;
