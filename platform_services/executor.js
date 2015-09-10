var Promise = require('bluebird');

/**
 * A service to execute up to opt.concurrency number of promises paralelly
 *
 * @param {object} opt
 * @param {number} opt.concurrency Number of tasks to run paralelly
 */
function Executor(opt) {
	opt = opt || {};
	if (!opt.concurrency) { throw new Error('opt.concurrency must be defined'); }

	this._available = opt.concurrency;
	this._id = 1;
	this._busy = {};
}

/**
 * Wait for a slot to become available. It is guaranteed to have an available
 * slot in the synchronous parts of callback.
 *
 * @param {function} [callback]
 * @returns {Promise} the result of the callback
 */
Executor.prototype.wait = function(callback) {
	var self = this;

	function waitForAvailable() {
		if (self._available > 0) {
			return callback instanceof Function ? callback() : undefined;
		}

		if (!self._waiter) {
			self._waiter = Promise.pending();
		}
		return self._waiter.promise.then(waitForAvailable);
	}

	return Promise.resolve(waitForAvailable());
};

/**
 * Checks if the Executor has any available slots
 *
 * @returns true if the executor can executor tasks now
 */
Executor.prototype.isAvailable = function() {
	return this._available > 0;
};

/**
 * Run a task in a slot. A task is done when the promise gets resolved. Throws
 * an error when there is no slow available.
 *
 * @param {Promise} promise
 * @returns {Promise} a Promise that gets resolved when the parameter promise
 *   is resolved
 */
Executor.prototype.execute = function(promise) {
	if (this._available <= 0) { throw new Error('no slots available'); }

	this._available--;
	var id = this._id++;
	this._busy[id] = promise;

	return Promise.bind(this).then(function() {
		return promise;
	}).finally(function() {
		delete this._busy[id];
		this._available++;
		if (this._waiter) {
			var waiter = this._waiter;
			delete this._waiter;
			waiter.resolve();
		}
	});
};

module.exports = Executor;
