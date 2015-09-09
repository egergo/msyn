return;

var sinon = require('sinon');
var nock = require('nock');
var fs = require('fs');
var util = require('util');
var chai = require('chai');
var zlib = require('zlib');
var AuctionHouse = require('../auction_house.js');

var expect = chai.expect;
var should = chai.should();

var fix1 = {
    "files": [{
        "url": "http://eu.battle.net/auction-data/57304a565824f2d778f9ad106430c98c/auctions.json",
        "lastModified": 1437479516000
    }]
};

nock.disableNetConnect();
nock('https://eu.api.battle.net')
	.get('/wow/auction/data/mazrigos')
	.query(true)
	.reply(200, {
		files: [{
	  	url: 'http://eu.battle.net/auction-data/57304a565824f2d778f9ad106430c98c/auctions.json',
	    lastModified: 1437479516000
	  }]
	});

nock('http://eu.battle.net')
	.get('/auction-data/57304a565824f2d778f9ad106430c98c/auctions.json')
	.reply(200, fs.readFileSync('mazrigos-1437485272000.json'));

var pastfix = {
	auctions: {
		auctions: [
			{auc:1,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},
			{auc:2,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':10,quantity:1,timeLeft:'VERY_LONG'},
			{auc:3,item:200,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},
			{auc:4,item:100,owner:'Ermizhad',ownerRealm:'Mazrigos','buyout':7,quantity:1,timeLeft:'VERY_LONG'},
			{auc:5,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},

			{auc:6,item:300,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'SHORT'}, // expired
			{auc:7,item:300,owner:'Perlan',ownerRealm:'Mazrigos','buyout':15,quantity:1,timeLeft:'VERY_LONG'}, // sold
			{auc:8,item:300,owner:'Perlan',ownerRealm:'Mazrigos','buyout':20,quantity:1,timeLeft:'VERY_LONG'}, // reposted
			{auc:9,item:300,owner:'Ermizhad',ownerRealm:'Mazrigos','buyout':15,quantity:1,timeLeft:'VERY_LONG'}, // intact
		]
	}
};

var presentfix = {
	auctions: {
		auctions: [
			{auc:1,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},
			{auc:2,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':10,quantity:1,timeLeft:'VERY_LONG'},
			{auc:3,item:200,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},
			{auc:4,item:100,owner:'Ermizhad',ownerRealm:'Mazrigos','buyout':7,quantity:1,timeLeft:'VERY_LONG'},
			{auc:5,item:100,owner:'Perlan',ownerRealm:'Mazrigos','buyout':5,quantity:1,timeLeft:'VERY_LONG'},

			{auc:10,item:300,owner:'Perlan',ownerRealm:'Mazrigos','buyout':14,quantity:1,timeLeft:'VERY_LONG'}, // reposted
			{auc:9,item:300,owner:'Ermizhad',ownerRealm:'Mazrigos','buyout':15,quantity:1,timeLeft:'VERY_LONG'}, // intact
		]
	}
};

describe('AuctionHouse', function() {

	it('zlib test code', function() {
		var a = JSON.stringify({a:1});
		var zlib = require('zlib');
		var abuf = new Buffer(a, 'binary');
		var acomp = zlib.deflateRawSync(abuf, {level:7});
		var adecom = zlib.inflateRawSync(acomp);
		console.log(JSON.parse(adecom));
	});

	it('mock everything', function() {

		var redis = {
			get: sinon.stub(),
			getBuffer: sinon.stub(),
			set: sinon.stub()
		};

		var past = new AuctionHouse.Auctions({
			data: fs.readFileSync('mazrigos-1437483712000.json'),
			lastModified: 1437483712000
		});
		var comp = zlib.deflateRawSync(new Buffer(JSON.stringify(past.auctions), 'binary'), {level: 7});

		redis.get.onCall('realms:eu:mazrigos:auc:lastModified').returns(Promise.resolve('1437483712000'));
		//redis.getBuffer.onCall('realms:eu:mazrigos:auc:past').returns(Promise.resolve(comp));
		redis.getBuffer.returns(Promise.resolve(comp));
		//redis.getBuffer.returns(Promise.resolve());
		redis.set.returns(Promise.resolve());

		//redis.get.returns(Promise.resolve());
		//redis.get.throws();

		var ah = new AuctionHouse({
			redis: redis,
			region: 'eu',
			realm: 'mazrigos',
			key: 'asdf'
		});
		return ah.load().then(function(present) {
			console.log(util.inspect(process.memoryUsage()));
			//console.log(redis.set.printf('%n %c %C %t'));
			//console.log(present._changes);

			should.exist(present._changes);
			//present._changes.should.not.be.undefined;

		})
	});

	it('load two fixated auctions and apply past', function() {
		var past = new AuctionHouse.Auctions({
			data: JSON.stringify(pastfix),
			lastModified: 1437395516000
		});
		var present = new AuctionHouse.Auctions({
			data: JSON.stringify(presentfix),
			lastModified: 1437404516000
		});
		present.applyPast(past);

		console.log('changes', util.inspect(present._changes, {depth:null}));
		console.log('price changes', util.inspect(present._priceChanges, {depth:null}));
	});

	it('load small fixate and test auctions', function() {
		var auctions = new AuctionHouse.Auctions({
			data: JSON.stringify(pastfix),
			lastModified: new Date().getTime()
		});
		console.log('Perlan-Mazrigos', auctions.getOwnerItems('Perlan-Mazrigos'));
		console.log('Ermizhad-Mazrigos', auctions.getOwnerItems('Ermizhad-Mazrigos'));
		console.log('Outcast-Mazrigos', auctions.getOwnerItems('Outcast-Mazrigos'));
		console.log('item: 100', auctions.getItemAuctions(100));
		console.log('item: 200', auctions.getItemAuctions(200));
		console.log('item: 300', auctions.getItemAuctions(300));

		expect(auctions.getAuction.bind(auctions, 0)).to.throw(Error);

		console.log(util.inspect(process.memoryUsage()));
	});

	it('load local files and apply past', function() {
		var past = new AuctionHouse.Auctions({
			data: fs.readFileSync('draenor-1437485509000.json'),
			lastModified: 1437485509000
		});
		var present = new AuctionHouse.Auctions({
			data: fs.readFileSync('draenor-1437493009000.json'),
			lastModified: 1437493009000
		});
		present.applyPast(past);

		console.log(util.inspect(process.memoryUsage()));
	});
});
