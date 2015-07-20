var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var Redis = require('ioredis');
var redis = new Redis(process.env.REDIS_URI);

var log = require('./log');

var key = process.env.BLIZZARD_KEY;


function processRealm(region, locale) {
	var endpoint = util.format('https://%s.api.battle.net/wow/realm/status?locale=%s&apikey=%s', region, encodeURIComponent(locale), encodeURIComponent(key));
	return Promise.resolve().then(function() {
		return request({
			uri: endpoint,
			gzip: true
		});
	}).then(function(res) {
		res = JSON.parse(res);

		var realms = {};
		res.realms.forEach(function(realm) {
			realms[realm.slug] = {
				name: realm.name,
				connections: realm.connected_realms
			};
		});

		return realms;

	}).then(function(res) {
		return redis.hset('realms', region, JSON.stringify(res));
	}).then(function() {
		log.info({region: region}, 'Updated region: %s', region);
	});
}

Promise.resolve().then(function() {
	return Promise.all([
		processRealm('eu', 'en_GB'),
		processRealm('us', 'en_US'),
		processRealm('tw', 'zh_TW'),
		processRealm('kr', 'ko_KR')
	]);
}).catch(function(err) {
	log.error({error: err}, 'error: ' + err.stack);
}).finally(function() {
	process.exit();
});
