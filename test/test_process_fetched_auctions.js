var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');

var expect = chai.expect;
var should = chai.should();

var Promise = require('bluebird');
var azureStorage = require('azure-storage');
var util = require('util');
var zlib = require('zlib');

var log = require('../log');
var Auctions = require('../auction_house.js').Auctions;
var ProcessFetchedAuctions = require('../app_data/jobs/continuous/RealmAuctionFetcher/process_fetched_auctions.js');

log.streams = [];

describe('ProcessFetchedAuctions', function() {

	var done;
	var tables;
	var blobs;
	var serviceBus;
	var azure;
	var auctionStore;
	var processFetchedAuctions;

	beforeEach(function() {
		done = Promise.pending();

		tables = {
			retrieveEntityAsync: sinon.stub(),
			queryEntitiesAsync: sinon.stub(),
			insertOrReplaceEntityAsync: sinon.stub()
		};

		blobs = {
			getBlobToBufferGzipAsync: sinon.stub()
		};

		serviceBus = {
			sendQueueMessageAsync: sinon.stub()
		};

		azure = {
			tables: tables,
			blobs: blobs,
			serviceBus: serviceBus,
			TableBatch: azureStorage.TableBatch,
			TableQuery: azureStorage.TableQuery,
			ent: azureStorage.TableUtilities.entityGenerator
		};

		auctionStore = {
			storeAuctions: sinon.stub(),
			loadRawAuctions: sinon.stub(),
			loadProcessedAuctions: sinon.stub(),
			getLastProcessedTime: sinon.stub(),
			getFetchedAuctionsSince: sinon.stub()
		};

		processFetchedAuctions = new ProcessFetchedAuctions({
			azure: azure,
			auctionStore: auctionStore,
			log: log,
			region: 'eu',
			realm: 'lightbringer'
		});
	});

	it('should not throw on first run', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_PROCESSED = new Date(0);
		var FIRST = new Date();

		var auctions = new Auctions({
			lastModified: new Date(),
			data: {
				realms: [{slug: 'lightbringer', name: 'Lightbringer'}],
				auctions: [
					{auc: 1, owner: 'Perlan', ownerRealm: 'Mazrigos', item: 1, quantity: 1}
				]
			}
		});
		auctionStore.loadRawAuctions.returns(Promise.resolve(auctions));

		//auctionStore.getLastProcessedTime.returns(Promise.resolve(new Date));
		auctionStore.getLastProcessedTime.withArgs(REGION, REALM).returns(Promise.resolve(LAST_PROCESSED));
		auctionStore.getFetchedAuctionsSince.withArgs(REGION, REALM, LAST_PROCESSED).returns(Promise.resolve([{
			lastModified: FIRST
		}]));
		//auctionStore.loadProcessedAuctions.returns(REGION, REALM, FIRST).returns(Promise.resolve(auctions));

		serviceBus.sendQueueMessageAsync.returns(Promise.resolve());
		auctionStore.storeAuctions.returns(Promise.resolve());

		return processFetchedAuctions.run().then(function() {
			auctionStore.storeAuctions.args[0][1].should.be.equal('eu');
			auctionStore.storeAuctions.args[0][2].should.be.equal('lightbringer');
		});
	});

	it('should create delta since last processed', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_PROCESSED = new Date(1424131200000);
		var LAST_MODIFIED = new Date(1424131200000 + 20 * 60 * 1000);

		auctionStore.getLastProcessedTime.withArgs(REGION, REALM).returns(Promise.resolve(LAST_PROCESSED));
		auctionStore.getFetchedAuctionsSince.withArgs(REGION, REALM, LAST_PROCESSED).returns(Promise.resolve([{
			lastModified: LAST_MODIFIED
		}]));

		var auctions = new Auctions({
			lastModified: LAST_MODIFIED,
			data: {
				realms: [{slug: 'lightbringer', name: 'Lightbringer'}],
				auctions: [
					{auc: 1, owner: 'Perlan', ownerRealm: 'Mazrigos', item: 1, quantity: 1}
				]
			}
		});
		auctionStore.loadRawAuctions.withArgs(REGION, REALM, LAST_MODIFIED).returns(Promise.resolve(auctions));
		auctionStore.loadProcessedAuctions.withArgs(REGION, REALM, LAST_PROCESSED).returns(Promise.resolve(auctions));

		serviceBus.sendQueueMessageAsync.returns(Promise.resolve());
		auctionStore.storeAuctions.returns(Promise.resolve());

		return processFetchedAuctions.run().then(function() {
			auctionStore.storeAuctions.args[0][1].should.be.equal('eu');
			auctionStore.storeAuctions.args[0][2].should.be.equal('lightbringer');
		});
	});
});

