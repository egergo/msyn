var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');
var path = require('path');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var log = require('../log');
var bnet = require('../bnet');

function nameToAH(name, region) {
	switch (name) {
		case "Cho'gall": return region === 'eu' ? "Cho’gall" : "Cho'gall";
		case "Der abyssische Rat": return "DerAbyssischeRat";
		case "Ner'zhul": return region === 'eu' ? "Ner’zhul" : "Ner'zhul";
	}

	return name.replace(/ /g, '');
}

function addMissingRealms(region, realms) {
	if (region === 'eu') {
		realms['suramar'] = {
			name: 'Suramar',
			slug: 'suramar',
			ah: 'Suramar',
			connections: ['medivh', 'suramar'],
			locale: 'fr_FR'
		};
	} else if (region === 'kr') {
		realms['알렉스트라자'] = {
			name: '알렉스트라자',
			slug: '알렉스트라자',
			ah: '알렉스트라자',
			connections: ["데스윙", "알렉스트라자"],
			locale: 'ko_KR'
		};
		realms['노르간논'] = {
			name: '노르간논',
			slug: '노르간논',
			ah: '노르간논',
			connections: ["세나리우스", "노르간논", "달라란", "말퓨리온"],
			locale: 'ko_KR'
		};
		realms['달라란'] = {
			name: '달라란',
			slug: '달라란',
			ah: '달라란',
			connections: ["세나리우스", "노르간논", "달라란", "말퓨리온"],
			locale: 'ko_KR'
		};
		realms['말퓨리온'] = {
			name: '말퓨리온',
			slug: '말퓨리온',
			ah: '말퓨리온',
			connections: ["세나리우스", "노르간논", "달라란", "말퓨리온"],
			locale: 'ko_KR'
		};
		realms['스톰레이지'] = {
			name: '스톰레이지',
			slug: '스톰레이지',
			ah: '스톰레이지',
			connections: ["스톰레이지", "불타는-군단"],
			locale: 'ko_KR'
		};
		realms['와일드해머'] = {
			name: '와일드해머',
			slug: '와일드해머',
			ah: '와일드해머',
			connections: ["와일드해머", "렉사르", "윈드러너"],
			locale: 'ko_KR'
		};
		realms['렉사르'] = {
			name: '렉사르',
			slug: '렉사르',
			ah: '렉사르',
			connections: ["와일드해머", "렉사르", "윈드러너"],
			locale: 'ko_KR'
		};
		realms['가로나'] = {
			name: '가로나',
			slug: '가로나',
			ah: '가로나',
			connections: ["줄진", "가로나", "굴단"],
			locale: 'ko_KR'
		};
		realms['굴단'] = {
			name: '굴단',
			slug: '굴단',
			ah: '굴단',
			connections: ["줄진", "가로나", "굴단"],
			locale: 'ko_KR'
		};
	}
}

function checkMissingRealms(region, realms) {
	var allRealms = {};
	Object.keys(realms).forEach(function(slug) {
		var realm = realms[slug];
		allRealms[realm.slug] = realm.slug;
		realm.connections.forEach(function(slug) { allRealms[slug] = realm.slug; });
	});

	Object.keys(realms).forEach(function(realm) {
		delete allRealms[realm];
	});
	Object.keys(allRealms).forEach(function(realm) {
		log.error({region: region, realm: realm, referringRealm: realms[allRealms[realm]].slug}, 'missing realm');
	});
}

function getRealRealmNames(region, realms) {

	return Promise.map(Object.keys(realms), getRealmName, {concurrency: 10});

	function getRealmName(slug) {
		return Promise.resolve().then(function() {
			var endpoint = bnet.mapRegionToEndpoint(region);
			return request({
				uri: endpoint.hostname + '/wow/auction/data/' + encodeURIComponent(slug),
				qs: {
					apikey: process.env.BNET_ID,
					locale: endpoint.defaultLocale
				},
				gzip: true
			}).then(function(auctionDesc) {
				auctionDesc = JSON.parse(auctionDesc);
				var file = auctionDesc.files[0];
				return {
					url: file.url,
					lastModified: new Date(file.lastModified)
				};
			});
		}).then(function(desc) {
			return request({
				uri: desc.url,
				gzip: true
			});
		}).then(function(ah) {
			ah = JSON.parse(ah);
			var real = ah.realm.slug
			realms[slug].real = real;
			console.log(region, slug, '->', real, ah.realm.name);
			return slug;
		});
	}

}

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
				ah: nameToAH(realm.name, region),
				connections: realm.connected_realms,
				locale: realm.locale
			};
		});

		if (locale !== 'ru_RU') {
			addMissingRealms(region, realms);
		}
		checkMissingRealms(region, realms);

		return getRealRealmNames(region, realms).then(function() {
			var fileRegion = locale === 'ru_RU' ? 'ru' : region;
			var fileName = path.join(__dirname, fileRegion + '.json');

			require('fs').writeFileSync(fileName, JSON.stringify(realms, null, '\t'));
			log.info({region: region, locale: locale, fileName: fileName}, 'updated %s region in %s locale: %s realms stored', region, locale, Object.keys(realms).length);

			return realms;
		});
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
