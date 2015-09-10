/*jshint expr: true*/

var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');

var expect = chai.expect;
var should = chai.should();

var Promise = require('bluebird');

var Executor = require('../platform_services/executor.js');

describe('Executor', function() {

	it('should resolve a promise', function() {
		var executor = new Executor({concurrency: 1});
		var task = Promise.resolve().then(function() {
			return 'masagin';
		});
		return executor.execute(task).then(function(result) {
			result.should.be.equal('masagin');
		});
	});

	it('should resolve promises asynchronously', function() {
		var executor = new Executor({concurrency: 2});
		var firstRun = false;
		var secondRun = false;
		var firstTask = Promise.resolve().then(function() {
			firstRun = true;
			return 'first';
		});
		var secondTask = Promise.resolve().then(function() {
			secondRun = true;
			return 'second';
		});
		var promises = Promise.all([
			executor.execute(firstTask),
			executor.execute(secondTask)
		]);
		firstRun.should.be.false;
		secondRun.should.be.false;
		return promises.then(function(result) {
			result.should.be.eql(['first', 'second']);
			firstRun.should.be.true;
			secondRun.should.be.true;
		});
	});

	it('should reject a task with no slot available', function() {
		var executor = new Executor({concurrency: 1});
		var firstTask = Promise.resolve().then(function() {
			return 'first';
		});
		var secondTask = Promise.resolve().then(function() {
			return 'second';
		});
		executor.execute(firstTask);
		expect(function() { executor.execute(secondTask); }).to.throw(Error);
	});

	it('should resolve wait when a slot becomes available', function() {
		var executor = new Executor({concurrency: 1});
		var pending = Promise.pending();
		executor.execute(pending.promise);

		var first = executor.wait(function() {
			return executor.execute(Promise.resolve().then(function() {
				return 'first';
			}));
		});
		var second = executor.wait(function() {
			return executor.execute(Promise.resolve().then(function() {
				return 'second';
			}));
		});

		pending.resolve();

		return Promise.all([first, second]).then(function(result) {
			result.should.be.eql(['first', 'second']);
		});
	});

});


