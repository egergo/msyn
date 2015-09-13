var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');
var nock = require('nock');

var expect = chai.expect;
var should = chai.should();

var Promise = require('bluebird');
var azureStorage = require('azure-storage');
var util = require('util');
var zlib = require('zlib');

var log = require('../log');
var Azure = require('../platform_services/azure.js');

log.streams = [];

describe('Platform Service: Azure', function() {

	var azure;

	beforeEach(function() {
		nock.disableNetConnect();
		azure = Azure.createFromEnv();
	});

	describe('extension: blobs.lazyContainer', function() {
		it('should not create a container when it already exists', function() {
			var CONTAINER = 'asdf';
			var cb = sinon.stub();

			cb.returns(Promise.resolve());

			return azure.blobs.lazyContainer(CONTAINER, cb).then(function() {
				cb.callCount.should.be.equal(1);
			});
		});

		it('should create a container when it does not exist', function() {
			var CONTAINER = 'asdf';
			var cb = sinon.stub();
			azure.blobs.createContainerIfNotExistsAsync = sinon.stub();

			azure.blobs.createContainerIfNotExistsAsync.returns(Promise.resolve());
			cb.onCall(0).returns(Promise.reject(errorWithCode('The specified container does not exist.', 'ContainerNotFound')));
			cb.onCall(1).returns(Promise.resolve());

			return azure.blobs.lazyContainer(CONTAINER, cb).then(function() {
				azure.blobs.createContainerIfNotExistsAsync.callCount.should.be.equal(1);
				cb.callCount.should.be.equal(2);
			});
		});
	});

	function errorWithCode(message, code) {
		var err = new Error(message);
		err.code = code;
		return err;
	}

});

