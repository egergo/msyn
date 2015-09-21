var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');

var expect = chai.expect;
var should = chai.should();

var Promise = require('bluebird');

var log = require('../log');
var TaskQueue = require('../platform_services/task_queue.js');
var Executor = require('../platform_services/executor.js');

log.debug = function() {};
log.error = function() {};
log.warn = function() {};

describe('TaskQueue', function() {

	var done;
	var serviceBus;
	var taskQueue;

	beforeEach(function() {
		done = Promise.pending();

		serviceBus = {
			receiveQueueMessageAsync: sinon.stub(),
			deleteMessageAsync: sinon.stub(),
			unlockMessageAsync: sinon.stub()
		};

		serviceBus.receiveQueueMessageAsync.returns(Promise.pending().promise);
		serviceBus.deleteMessageAsync.returns(Promise.resolve());
		serviceBus.unlockMessageAsync.returns(Promise.resolve());

		taskQueue = new TaskQueue({
			azure: {
				serviceBus: serviceBus
			},
			queueName: 'rozsomak',
			executor: new Executor({concurrency: 1}),
			log: log
		});
	});

	it('should process all messages', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(serviceBusMessage('first'));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('second'));
		serviceBus.receiveQueueMessageAsync.onCall(2).returns(serviceBusMessage('third'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'third') {
				done.resolve();
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			callback.args[0][0].body.should.be.equal('first');
			callback.args[1][0].body.should.be.equal('second');
			callback.args[2][0].body.should.be.equal('third');
		});
	});

	it('should delete processed message', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(serviceBusMessage('first'));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('second'));

		taskQueue.run(function(message) {
			if (message.body === 'second') {
				done.resolve();
			}
		});

		return done.promise.then(function() {
			serviceBus.deleteMessageAsync.args[0][0].body.should.be.equal('first');
		});
	});

	it('should unlock failed message', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(serviceBusMessage('first'));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('second'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'second') {
				done.resolve();
			} else {
				throw new Error('retry please');
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			serviceBus.unlockMessageAsync.args[0][0].body.should.be.equal('first');
		});
	});

	it('should handle transient errors', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(serviceBusMessage('first'));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('second'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'second') {
				done.resolve();
			} else {
				var err = new Error();
				err.name = 'TransientError';
				err.cause = new Error('no network');
				throw err;
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			serviceBus.unlockMessageAsync.args[0][0].body.should.be.equal('first');
		});
	});

	it('should drop poison message', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(serviceBusMessage('first', 5));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('second'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'second') {
				done.resolve();
			} else {
				throw new Error('retry please');
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			serviceBus.deleteMessageAsync.args[0][0].body.should.be.equal('first');
		});
	});

	it('should retry on no message', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(Promise.reject(new Error('No messages to receive')));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('first'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'first') {
				done.resolve();
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			callback.args[0][0].body.should.be.equal('first');
		});
	});

	it('should recover from network error', function() {
		serviceBus.receiveQueueMessageAsync.onCall(0).returns(Promise.reject(new Error('socket hang up')));
		serviceBus.receiveQueueMessageAsync.onCall(1).returns(serviceBusMessage('first'));

		var callback = sinon.spy(function(message) {
			if (message.body === 'first') {
				done.resolve();
			}
		});
		taskQueue.run(callback);

		return done.promise.then(function() {
			callback.args[0][0].body.should.be.equal('first');
		});
	});

});

function serviceBusMessage(body, deliveryCount) {
	return Promise.resolve([{
		"body": body,
		"brokerProperties": {
			"DeliveryCount": deliveryCount || 1,
			"EnqueuedSequenceNumber": 0,
			"EnqueuedTimeUtc": "Fri, 07 Aug 2015 05:29:00 GMT",
			"LockToken": "55a13e85-9f7f-415a-9f78-b3356c1df820",
			"LockedUntilUtc": "Fri, 07 Aug 2015 05:29:30 GMT",
			"MessageId": "035195b1b4694982bbb0fb95ec9c224a",
			"SequenceNumber": 2028,
			"State": "Active",
			"TimeToLive": 1209600
		},
		"location": "https://mesellyounot-egergo.servicebus.windows.net/mytopic/messages/2028/55a13e85-9f7f-415a-9f78-b3356c1df820",
		"contentType": "application/xml; charset=utf-8",
		"customProperties": {
			"connection": null
		}
	}, {}]);
}

