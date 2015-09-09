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
var AuctionStore = require('../auction_store.js');
var Auctions = require('../auction_house.js').Auctions;

log.streams = [];

describe('AuctionStore', function() {

	var done;
	var tables;
	var blobs;
	var azure;
	var auctionStore;

	beforeEach(function() {
		done = Promise.pending();

		tables = {
			createTableIfNotExistsAsync: sinon.stub(),
			executeBatchAsync: sinon.stub(),
			retrieveEntityAsync: sinon.stub(),
			queryEntitiesAsync: sinon.stub(),
			insertOrReplaceEntityAsync: sinon.stub()
		};

		blobs = {
			getBlobToBufferGzipAsync: sinon.stub(),
			createBlockBlobFromTextGzipAsync: sinon.stub()
		};

		azure = {
			tables: tables,
			blobs: blobs,
			TableBatch: azureStorage.TableBatch,
			TableQuery: azureStorage.TableQuery,
			ent: azureStorage.TableUtilities.entityGenerator
		};

		auctionStore = new AuctionStore({
			azure: azure,
			log: log
		});
	});

	it('#storeAuctions()', function() {
		// TODO: test if delta stored
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date(1441794872336);
		var EXPECTED_NAME = 'XAuctions20150910';

		var lastModified = LAST_MODIFIED;
		var auctionsRaw = {
			auctions: [
				{"auc":661631959,"item":87475,"owner":"Perlan","ownerRealm":"Mazrigos","bid":1694000,"buyout":2420000,"quantity":1,"timeLeft":"VERY_LONG","rand":0,"seed":0,"context":0},
				{"auc":662914213,"item":62669,"owner":"Perlan","ownerRealm":"Mazrigos","bid":803500,"buyout":880000,"quantity":1,"timeLeft":"VERY_LONG","rand":0,"seed":1770755456,"context":0},
				{"auc":662550080,"item":62669,"owner":"Ermizhad","ownerRealm":"Lightbringer","bid":201780,"buyout":212420,"quantity":20,"timeLeft":"VERY_LONG","rand":0,"seed":0,"context":0},
			]
		};
		var auctions = new Auctions({
			data: auctionsRaw,
			lastModified: lastModified
		});

		tables.insertOrReplaceEntityAsync.withArgs(AuctionStore.CacheTableName, sinon.match({
			PartitionKey: sinon.match.has('_', 'current-' + REGION + '-' + REALM),
			RowKey: sinon.match.has('_', ''),
			lastProcessed: sinon.match.has('_', LAST_MODIFIED)
		})).returns(Promise.resolve());

		return auctionStore.storeAuctions(auctions, REGION, REALM).then(function() {
			tables.createTableIfNotExistsAsync.args[0][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[0][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[0][1].operations[0].entity.PartitionKey._.should.be.equal('eu-lightbringer-items-1441794872336');
			tables.executeBatchAsync.args[0][1].operations[0].entity.RowKey._.should.be.equal('62669');
			tables.executeBatchAsync.args[0][1].operations[1].entity.RowKey._.should.be.equal('87475');
			tables.executeBatchAsync.args[1][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[1][1].operations[0].entity.PartitionKey._.should.be.equal('eu-lightbringer-owners-1441794872336');
			tables.executeBatchAsync.args[1][1].operations[0].entity.RowKey._.should.be.equal('Perlan-Mazrigos');
			tables.executeBatchAsync.args[1][1].operations[1].entity.RowKey._.should.be.equal('Ermizhad-Lightbringer');
			blobs.createBlockBlobFromTextGzipAsync.args[0][1].should.contain('processed/');

			var items = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[0][1].operations[0].entity.Auctions._));
			items.length.should.equal(2);
			var perlans = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[1][1].operations[0].entity.Auctions._));
			perlans.should.contain(62669, 87475);
			var ermzs = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[1][1].operations[0].entity.Auctions._));
			ermzs.should.contain(62669);
		});
	});

	it('#loadProcessedAuctions()', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date();
		var EXPECTED_NAME = auctionStore._getStorageName(REGION, REALM, AuctionStore.Type.Processed, LAST_MODIFIED);

		blobs.getBlobToBufferGzipAsync.withArgs(EXPECTED_NAME.container, EXPECTED_NAME.path).returns(Promise.resolve([JSON.stringify({
			1: {item: 1, owner: 'Perlan-Mazrigos', quantity: 1, buyoutPerItem: 1, timeLeft: 10, timeLeftSince: LAST_MODIFIED.getTime()}
		})]));

		return auctionStore.loadProcessedAuctions(REGION, REALM, LAST_MODIFIED).then(function(auctions) {
			auctions._lastModified.should.be.equal(LAST_MODIFIED);
			auctions.auctions[1].owner.should.be.equal('Perlan-Mazrigos');
			auctions.index.owners.should.exist;
		});
	});

	it('#loadRawAuctions()', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date();
		var EXPECTED_NAME = auctionStore._getStorageName(REGION, REALM, AuctionStore.Type.Raw, LAST_MODIFIED);

		blobs.getBlobToBufferGzipAsync.withArgs(EXPECTED_NAME.container, EXPECTED_NAME.path).returns(Promise.resolve([JSON.stringify({
			realms: {slug: 'lightbringer', name: 'Lightbringer'},
			auctions: [
				{"auc":661631959,"item":87475,"owner":"Perlan","ownerRealm":"Mazrigos","bid":1694000,"buyout":2420000,"quantity":1,"timeLeft":"VERY_LONG","rand":0,"seed":0,"context":0},
			]
		})]));

		return auctionStore.loadRawAuctions(REGION, REALM, LAST_MODIFIED).then(function(auctions) {
			auctions._lastModified.should.be.equal(LAST_MODIFIED);
			auctions.auctions[661631959].owner.should.be.equal('Perlan-Mazrigos');
			auctions.index.owners.should.exist;
		});
	});

	it('#getLastProcessedTime()', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date();
		var EXPECTED_NAME = 'current-' + REGION + '-' + REALM;

		tables.retrieveEntityAsync.withArgs(AuctionStore.CacheTableName, EXPECTED_NAME, '').returns(Promise.resolve([{
			lastProcessed: {_: LAST_MODIFIED}
		}]));

		return auctionStore.getLastProcessedTime(REGION, REALM).then(function(lastProcessed) {
			lastProcessed.should.be.equal(LAST_MODIFIED);
		});
	});

	it('#getFetchedAuctionsSince()', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date();
		var FIRST = new Date(LAST_MODIFIED.getTime() + 1);
		var SECOND = new Date(LAST_MODIFIED.getTime() + 2);


		tables.queryEntitiesAsync.returns(Promise.resolve([{
			entries: [{
				lastModified: {_: FIRST}
			}, {
				lastModified: {_: SECOND}
			}]
		}]));

		return auctionStore.getFetchedAuctionsSince(REGION, REALM, LAST_MODIFIED).then(function(items) {
			items.length.should.be.equal(2);
			items[0].lastModified.should.be.equal(FIRST);
			items[1].lastModified.should.be.equal(SECOND);
		});
	});
});

