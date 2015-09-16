var Promise = require('bluebird');
var azureStorage = require('azure-storage');

var entGen = azureStorage.TableUtilities.entityGenerator;
var realms = require('./realms');

function User(opt) {
	opt = opt || {};
	if (!opt.tables) { throw new Error('opt.tables must be specified'); }
	if (!opt.id) { throw new Error('opt.id must be specified'); }

	this._tables = opt.tables;
	this._id = '' + opt.id;

	Object.defineProperty(this, 'id', {get: function() { return this._id; }});
}

User.prototype.getRaw = function() {
	if (!this._rawPromise) {
		this._rawPromise = this._tables.retrieveEntityAsync(User.TABLE_NAME, '' + this._id, '').spread(function(user) {
			return user;
		});
	}
	return this._rawPromise;
};


User.TABLE_NAME = 'users';

User.prototype.login = function(profile, accessToken) {
	// TODO: store registration and last login date
	return Promise.bind(this).then(function() {
		return this._tables.insertOrMergeEntityAsync(User.TABLE_NAME, {
			PartitionKey: entGen.String(this._id),
			RowKey: entGen.String(''),
			battletag: entGen.String(profile.battletag),
			accessToken: entGen.String(accessToken)
		});
	}).then(function() {
		this._accessTokenCache = accessToken;
	});
};

/**
 * Returns the user's characters for the connected realms.
 *
 * @param {string} region
 * @param {string} realm
 * @returns {[Character]}
 */
User.prototype.getCharactersOnRealm = function(region, realm) {
	return this.getRaw().then(function(raw) {
		if (!raw['characters_' + region]) { return []; }
		var reg = JSON.parse(raw['characters_' + region]._);
		return reg.characters.filter(function(character) {
			return realms[character.region].bySlug[character.realm].real === realm;
		});
	});
};

User.prototype.load = function() {
	return this._tables.retrieveEntityAsync(User.TABLE_NAME, this._id, '').spread(function(user) {
		return user;
	});
};

/**
 * User-wide settings
 * @typedef {object} Settings
 * @property {string} [email]
 * @property {string} [slackWebhook]
 * @property {string} [slackChannel]
 * @property {boolean} notificationsEnabled default true
 * @property {number} minValue minimum price threshold
 */

/**
 * @returns {Promise.<Settings>}
 */
User.prototype.getSettings = function() {
	return this.getRaw().then(function(raw) {
		if (!raw.Settings) { return {}; }
		var settings = JSON.parse(raw.Settings._);
		if (settings.notificationsEnabled === undefined) { settings.notificationsEnabled = true; }
		if (settings.minValue === undefined) { settings.minValue = 0; }
		return settings;
	});
};

/**
 * Saves settings with optimistic concurrency
 *
 * @param {function} modifier A function that gets the current settings as
 *        a parameter and returns a value that is resolved to the updated
 *        settings values. The modifier function might be invoket multiple
 *        times in case of conflict.
 */
User.prototype.saveSettings = function(modifier) {
	var self = this;
	var attempts = 5;
	return run();

	function run() {
		return self.getRaw().then(function(raw) {
			var etag = raw['.metadata'].etag;
			var settings = !raw.Settings ? {} : JSON.parse(raw.Settings._);
			return Promise.resolve(modifier(settings)).then(function(settings) {
				return self._tables.mergeEntityAsync(User.TABLE_NAME, {
					PartitionKey: {_: '' + self._id},
					RowKey: {_: ''},
					Settings: {_: JSON.stringify(settings)},
					'.metadata': {etag: etag}
				}).then(function() {
					return settings;
				});
			});
		}).catch(function(err) {
			if (--attempts && err.cause && err.cause.statusCode === 412) {
				delete self._rawPromise;
				return run();
			}
			throw err;
		});
	}
};

/**
 * @typedef {object} Character
 * @property {string} name
 * @property {string} realm
 * @property {string} region
 * @property {string} thumbnail
 * @property {string} guild
 * @property {string} guildRealm
 * @property {number} lastModified
 */

/**
 * @param {object} characters
 * @param {Character[]} characters.eu
 * @param {Character[]} characters.us
 */
User.prototype.saveCharacters = function(characters) {
	var o = {
		PartitionKey: entGen.String(this._id),
		RowKey: entGen.String('')
	};
	var now = new Date().getTime();
	Object.keys(characters).forEach(function(inrealm) {
		o['characters_' + inrealm] = entGen.String(JSON.stringify({
			lastModified: now,
			characters: characters[inrealm]
		}));
	});
	return this._tables.mergeEntityAsync(User.TABLE_NAME, o);
};

User.prototype.getUser = function() {
	return this._tables.retrieveEntityAsync(User.TABLE_NAME, '', this._id);
};

User.prototype.getAccessToken = function() {
	return Promise.resolve(this._accessTokenCache);
};

module.exports = User;
