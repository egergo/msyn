var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');
var path = require('path');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var log = require('../log');

function processRealm(region, locale) {
	var endpoint = util.format('https://%s.api.battle.net/wow/realm/status?locale=%s&apikey=%s', region, encodeURIComponent(locale), encodeURIComponent(process.env.BNET_ID));
	return Promise.resolve().then(function() {
		return request({
			uri: endpoint,
			gzip: true
		});
	}).then(function(res) {
		res = JSON.parse(res);

		var unique = 0;
		var uniqueDone = {};

		var realms = {};
		res.realms.forEach(function(realm) {
			if (!uniqueDone[realm.slug]) {
				unique++;
				uniqueDone[realm.slug] = true;
				realm.connected_realms.forEach(function(slug) { uniqueDone[slug] = true; });
			}

			realms[realm.slug] = {
				name: realm.name,
				slug: realm.slug,
				ah: realm.name.replace(/ /g, ''),
				connections: realm.connected_realms
			};
		});

		console.log(region, 'all realms:', res.realms.length, 'unique:', unique);

		require('fs').writeFileSync(path.join(__dirname, region + '.json'), JSON.stringify(realms, null, '\t'));

		return realms;

	}).then(function(res) {
		//return redis.hset('realms', region, JSON.stringify(res));
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
