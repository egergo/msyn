/*jshint expr: true*/

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
var Auctions = require('../auction_house.js').Auctions;
var SendNotifications = require('../app_data/jobs/continuous/RealmAuctionFetcher/send_notifications.js');

log.streams = [];

describe('SendNotifications', function() {

	var done;
	var auctionStore;
	var sendNotifications;
	var user;
	var slackNock;
	var slackNockRequest;
	var sendgridNock;
	var sendgridNockRequest;

	var LAST_PROCESSED = new Date();
	var WEBHOOK = 'https://hooks.slack.com/services/XXXXXXXX/XXXXXXXX/XXXXXXXXXXXXXXXX';

	beforeEach(function() {
		done = Promise.pending();

		auctionStore = {
			storeAuctions: sinon.stub(),
			loadRawAuctions: sinon.stub(),
			loadProcessedAuctions: sinon.stub(),
			getLastProcessedTime: sinon.stub(),
			getFetchedAuctionsSince: sinon.stub()
		};

		var auctionsRaw = {
			auctions: {
				1: {"item": 10940,"owner": "Perlan-Mazrigos","quantity": 1,"buyoutPerItem": 1000,"timeLeft": 1,"timeLeftSince": "2015-09-06T10:27:52.000Z"},
				2: {"item": 10940,"owner": "Wobblegob-Lightbringer","quantity": 1,"buyoutPerItem": 100,"timeLeft": 1,"timeLeftSince": "2015-09-06T10:27:52.000Z"},
			},
			priceChanges: {
				10940: -50
			},
			changes: {
				sold: {
					765: [
						{"owner": "Kenetek-Lightbringer","quantity": 20,"buyoutPerItem": 24500},
					]
				},
				relisted: {
					2841: {
						2020133740: {"buyoutPerItem": 20080},
					}
				}
			}
		};
		var auctions = new Auctions({
			processed: auctionsRaw,
			lastModified: LAST_PROCESSED
		});
		auctionStore.loadProcessedAuctions.returns(Promise.resolve(auctions));

		nock.disableNetConnect();
		slackNock = nock('https://hooks.slack.com')
			.post('/services/XXXXXXXX/XXXXXXXX/XXXXXXXXXXXXXXXX')
			.reply(200, function(uri, body) {
				slackNockRequest = JSON.parse(body);
				return 'ok';
			});
		sendgridNock = nock('https://api.sendgrid.com')
			.post('/api/mail.send.json')
			.reply(200, function(uri, body) {
				sendgridNockRequest = body;
				return '{"message":"success"}';
			});

		// var azure = require('../platform_services/azure').createFromEnv();
		// user = new (require('../user'))({id: 127047483, tables: azure.tables});
		// auctionStore = new (require('../auction_store'))({azure: azure, log: log});

		user = {
			getSettings: sinon.stub(),
			getCharactersOnRealm: sinon.stub()
		};

		sendNotifications = new SendNotifications({
			auctionStore: auctionStore,
			log: log,
			region: 'eu',
			realm: 'lightbringer',
			user: user
		});
	});

	it('should send notifications', function() {
		user.getSettings.returns(Promise.resolve({
			slackWebhook: WEBHOOK,
			email: 'egergo@mesellyounot.com'
		}));
		user.getCharactersOnRealm.returns(Promise.resolve([{
			name: 'Perlan',
			region: 'eu',
			realm: 'mazrigos'
		}]));
		auctionStore.getLastProcessedTime.returns(Promise.resolve(LAST_PROCESSED));

		return sendNotifications.run().then(function() {
			slackNock.done();
			slackNockRequest.attachments.should.not.be.empty;
			sendgridNock.done();
		});
	});

	it('should not send notifications when not notifiers enabled', function() {
		user.getSettings.returns(Promise.resolve({}));
		user.getCharactersOnRealm.returns(Promise.resolve([{
			name: 'Perlan',
			region: 'eu',
			realm: 'mazrigos'
		}]));
		auctionStore.getLastProcessedTime.returns(Promise.resolve(LAST_PROCESSED));

		return sendNotifications.run().then(function() {
			slackNock.isDone().should.be.false;
		});
	});

	it('should not send notifications when no character on realm', function() {
		user.getSettings.returns(Promise.resolve({
			slackWebhook: WEBHOOK
		}));
		user.getCharactersOnRealm.returns(Promise.resolve([]));
		auctionStore.getLastProcessedTime.returns(Promise.resolve(LAST_PROCESSED));

		return sendNotifications.run().then(function() {
			slackNock.isDone().should.be.false;
		});
	});

	it('should not send notifications when nothing to notify', function() {
		user.getSettings.returns(Promise.resolve({
			slackWebhook: WEBHOOK
		}));
		user.getCharactersOnRealm.returns(Promise.resolve([{
			name: 'Ermizhad',
			region: 'eu',
			realm: 'mazrigos'
		}]));
		auctionStore.getLastProcessedTime.returns(Promise.resolve(LAST_PROCESSED));

		return sendNotifications.run().then(function() {
			slackNock.isDone().should.be.false;
		});
	});
});

