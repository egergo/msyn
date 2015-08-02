var Promise = require('bluebird');
var azureStorage = require('azure-storage');

var entGen = azureStorage.TableUtilities.entityGenerator;

function User(opt) {
	opt = opt || {};
	if (!opt.tables) { throw new Error('opt.tables must be specified'); }
	if (!opt.id) { throw new Error('opt.id must be specified'); }

	this._tables = opt.tables;
	this._id = '' + opt.id;

	this._accessTokenCache;

	Object.defineProperty(this, 'id', {get: function() { return this._id; }});
}

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
	})
};

User.prototype.load = function() {
	return this._tables.retrieveEntityAsync(User.TABLE_NAME, this._id, '').spread(function(user) {
		return user;
	});
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
