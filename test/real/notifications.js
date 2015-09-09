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

var log = require('../../log');
var Auctions = require('../../auction_house.js').Auctions;
var SendNotifications = require('../../app_data/jobs/continuous/RealmAuctionFetcher/send_notifications.js');

//log.streams = [];

describe('SendNotifications', function() {

	var done;
	var auctionStore;
	var sendNotifications;
	var user;

	beforeEach(function() {
		done = Promise.pending();

		var azure = require('../../platform_services/azure').createFromEnv();
		user = new (require('../../user'))({id: 127047483, tables: azure.tables});
		auctionStore = new (require('../../auction_store'))({azure: azure, log: log});

		sendNotifications = new SendNotifications({
			auctionStore: auctionStore,
			log: log,
			region: 'eu',
			realm: 'lightbringer',
			user: user
		});
	});

	it('should send notifications', function() {
		this.timeout(20000);

		return sendNotifications.run();
	});
});

