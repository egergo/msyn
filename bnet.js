/**
 * Battle.NET access
 * @module bnet
 */

var Promise = require('bluebird');
var request = require('request-promise');

var log = require('./log');
var realms = require('./realms');

function mapRegionToEndpoint(region) {
	var endpoints = {
		us: {
			hostname: 'https://us.api.battle.net',
			defaultLocale: 'en_US'
		},
		eu: {
			hostname: 'https://eu.api.battle.net',
			defaultLocale: 'en_GB'
		},
		sea: {
			hostname: 'https://sea.api.battle.net',
			defaultLocale: 'en_US'
		},
		kr: {
			hostname: 'https://kr.api.battle.net',
			defaultLocale: 'ko_KR'
		},
		tw: {
			hostname: 'https://tw.api.battle.net',
			defaultLocale: 'zh_TW'
		},
		cn: {
			hostname: 'https://api.battlenet.com.cn',
			defaultLocale: 'zh_CN'
		},
		ru: {
			hostname: 'https://eu.api.battle.net',
			defaultLocale: 'ru_RU'
		},
	};

	var result = endpoints[region];
	if (!result) { throw new Error('unknow region: ' + region); }
	return result;
};

function fetchUserCharacters(opt) {
	opt = opt || {};
	if (!opt.accessToken) { throw new Error('opt.accessToken must be specified'); }
	if (!opt.region) { throw new Error('opt.region must be specified'); }

	return Promise.resolve().then(function() {
		var endpoint = mapRegionToEndpoint(opt.region);
		return request({
			uri: endpoint.hostname + '/wow/user/characters',
			qs: {
				access_token: opt.accessToken,
				locale: endpoint.defaultLocale
			},
			gzip: true
		});
	}).then(function(res) {
		res = JSON.parse(res);

		var hasRussian = false;
		var characters = [];
		res.characters.forEach(function(character) {
			var realmName = character.realm;
			var realm = realms[opt.region].byName[realmName];
			if (!realm && opt.region === 'ru') {
				// Russian pseudo-region won't contain other eu realms
				return;
			}
			if (!realm) {
				log.error({region: opt.region, character: character}, 'realm not found: %s', realmName);
				return;
			}

			var guildRealmSlug = undefined;
			if (character.guild) {
				var guildRealmName = character.guildRealm;
				var guildRealm = realms[opt.region].byName[guildRealmName];
				if (!guildRealm) {
					log.error({region: opt.region, character: character}, 'guild realm not found: %s', guildRealmName);
					return;
				}
				guildRealmSlug = guildRealm.slug;
			}

			if (opt.region === 'eu' && realm.locale === 'ru_RU') {
				hasRussian = true;
				return;
			}

			characters.push({
				name: character.name,
				realm: realm.slug,
				region: opt.region,
				thumbnail: character.thumbnail,
				guild: character.guild,
				guildRealm: guildRealmSlug,
				lastModified: character.lastModified
			});
		});

		if (!hasRussian) {
			return characters;
		}

		return fetchUserCharacters({
			region: 'ru',
			accessToken: opt.accessToken
		}).then(function(russianCharacters) {
			characters.push.apply(characters, russianCharacters);
			return characters;
		});
	});
}

/**
 * @typedef AuctionDataStatus
 * @property {string} url
 * @property {number} lastModified
 */

/**
 * @param {object} opt
 * @param {string} opt.accessToken
 * @param {string} opt.region
 * @param {string} opt.realm
 * @param {string} [opt.locale]
 * @param {bool} [opt.raw]
 *
 * @returns {Promise.<AuctionDataStatus>}
 */
exports.getAuctionDataStatus = function(opt) {
	opt = opt || {};
	if (!opt.accessToken) { throw new Error('opt.accessToken must be specified'); }
	if (!opt.region) { throw new Error('opt.region must be specified'); }
	if (!opt.realm) { throw new Error('opt.realm must be specified'); }

	return Promise.resolve().then(function() {
		var endpoint = mapRegionToEndpoint(opt.region);
		return request({
			uri: endpoint.hostname + '/wow/auction/data/' + encodeURIComponent(opt.realm),
			qs: {
				apikey: opt.accessToken,
				locale: opt.locale || endpoint.defaultLocale
			},
			gzip: true
		});
	}).then(function(res) {
		if (opt.raw) { return res; }
		var json = JSON.parse(res);
		return json.files[0];
	});
};

module.exports.fetchUserCharacters = fetchUserCharacters;
module.exports.mapRegionToEndpoint = mapRegionToEndpoint;
