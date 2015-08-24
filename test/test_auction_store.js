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
	var azure;
	var auctionStore;

	beforeEach(function() {
		done = Promise.pending();

		tables = {
			createTableIfNotExistsAsync: sinon.stub(),
			executeBatchAsync: sinon.stub()
		};

		azure = {
			tables: tables,
			TableBatch: azureStorage.TableBatch,
			ent: azureStorage.TableUtilities.entityGenerator
		};

		auctionStore = new AuctionStore({azure: azure});
	});

	it('#storeAuctions()', function() {
		var REGION = 'eu';
		var REALM = 'lightbringer';
		var LAST_MODIFIED = new Date();
		var EXPECTED_NAME = auctionStore._getAuctionsTableName(REGION, REALM, LAST_MODIFIED);

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

		return auctionStore.storeAuctions(auctions, REGION, REALM).then(function() {
			tables.createTableIfNotExistsAsync.args[0][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[0][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[0][1].operations[0].entity.PartitionKey._.should.be.equal('items');
			tables.executeBatchAsync.args[0][1].operations[0].entity.RowKey._.should.be.equal('62669');
			tables.executeBatchAsync.args[0][1].operations[1].entity.RowKey._.should.be.equal('87475');
			tables.executeBatchAsync.args[1][0].should.be.equal(EXPECTED_NAME);
			tables.executeBatchAsync.args[1][1].operations[0].entity.PartitionKey._.should.be.equal('owners');
			tables.executeBatchAsync.args[1][1].operations[0].entity.RowKey._.should.be.equal('Perlan-Mazrigos');
			tables.executeBatchAsync.args[1][1].operations[1].entity.RowKey._.should.be.equal('Ermizhad-Lightbringer');

			var items = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[0][1].operations[0].entity.Auctions._));
			items.length.should.equal(2);
			var perlans = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[1][1].operations[0].entity.Auctions._));
			perlans.should.contain(62669, 87475);
			var ermzs = JSON.parse(zlib.inflateRawSync(tables.executeBatchAsync.args[1][1].operations[0].entity.Auctions._));
			ermzs.should.contain(62669);
		});
	});
});

