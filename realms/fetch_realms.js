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

		var realms = {};
		res.realms.forEach(function(realm) {
			if (locale === 'ru_RU' && realm.locale !== 'ru_RU') {
				return;
			}

			realms[realm.slug] = {
				name: realm.name,
				slug: realm.slug,
				ah: realm.name.replace(/ /g, ''),
				connections: realm.connected_realms,
				locale: realm.locale
			};
		});

		var fileRegion = locale === 'ru_RU' ? 'ru' : 'eu';
		var fileName = path.join(__dirname, fileRegion + '.json');

		require('fs').writeFileSync(fileName, JSON.stringify(realms, null, '\t'));
		log.info({region: region, locale: locale, fileName: fileName}, 'updated %s region in %s locale: %s realms stored', region, locale, Object.keys(realms).length);

		return realms;
	});
}

Promise.resolve().then(function() {
	return Promise.all([
		processRealm('eu', 'en_GB'),
		processRealm('eu', 'ru_RU'),
		processRealm('us', 'en_US'),
		processRealm('tw', 'zh_TW'),
		processRealm('kr', 'ko_KR')
	]);
}).catch(function(err) {
	log.error({error: err}, 'error: ' + err.stack);
}).finally(function() {
	process.exit();
});
