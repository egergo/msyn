"use strict";

var util = require('util');
var Promise = require('bluebird');
var request = require('request-promise');
var encoder = new (require('node-html-encoder').Encoder)('entity');
var Auctions = require('../../../../auction_house').Auctions;
var realms = require('../../../../realms');
var itemDb = require('../../../../items');
var sendgrid = require('sendgrid')(process.env.SENDGRID_KEY);


Promise.promisifyAll(sendgrid);

/**
 * @param {object} opt
 * @param {AuctionStore} auctionStore
 * @param {Bunyan} log
 * @param {string} region
 * @param {string} realm
 * @param {User} user
 */
function SendNotifications(opt) {
	if (!opt.auctionStore) { throw new Error('opt.auctionStore must be defined'); }
	if (!opt.log) { throw new Error('opt.log must be defined'); }
	if (!opt.region) { throw new Error('opt.region must be defined'); }
	if (!opt.realm) { throw new Error('opt.realm must be defined'); }
	if (!opt.user) { throw new Error('opt.user must be defined'); }

	this._auctionStore = opt.auctionStore;
	this._log = opt.log;
	this._region = opt.region;
	this._realm = opt.realm;
	this._user = opt.user;
}

SendNotifications.prototype.run = function() {
	var self = this;

	var notifiers;
	var settings;

	return Promise.bind(this).then(function() {
		return this._checkIfNotificationsEnabled();
	}).then(function(settingsArg) {
		settings = settingsArg;
		return this._createNotifiers();
	}).then(function(notifiersArgs) {
		notifiers = notifiersArgs;
		this._log.debug({notifiers: Object.keys(notifiers)}, 'active notifiers');
		return Promise.all([
			this._getCharacterNames(),
			this._loadCurrentAuctions()
		]);
	}).spread(function(ahnames, auctions) {
		if (ahnames.length === 0) { throw new Error('NoCharacters'); }

		var tonotify = {
			lastModified: auctions._lastModified,
			region: self._region,
			characters: {}
		};

		ahnames.forEach(function(ahname) {
			var forChar = this._processForCharacter(ahname, auctions, settings);
			if (forChar) {
				tonotify.characters[ahname] = forChar;
			}
		}, this);

		if (Object.keys(tonotify.characters).length === 0) { throw new Error('NoAuctions'); }

		return tonotify;

	}).then(function(tonotify) {
		return Promise.each(Object.keys(notifiers), function(notifierName) {
			var notifier = notifiers[notifierName];
			return notifier.send(tonotify);
		});
	}).then(function() {
		return 'ok';
	}).catch(function(err) {
		if (err.message === 'NoNotifiers') { return; }
		if (err.message === 'NoCharacters') { return; }
		if (err.message === 'NoAuctions') { return; }
		if (err.message === 'NotificationsDisabled') { return; }
		throw err;
	});
};

/**
 * Check is user has enabled sending notifications.
 *
 * @return {Promise<settings>}
 * @throws Error('NotificationsDisabled')
 */
SendNotifications.prototype._checkIfNotificationsEnabled = function() {
	return this._user.getSettings().then(function(settings) {
		if (!settings.notificationsEnabled) { throw new Error('NotificationsDisabled'); }
		return settings;
	});
};

/**
 * Creates and objects with enabled notifiers. If no notifiers are enabled
 * it throws Error('NoNotifiers').
 *
 * @returns {object} Keys are notifier types, values the Notifiers
 * @throws Error('NoNotifiers')
 */
SendNotifications.prototype._createNotifiers = function() {
	var notifiers = {
		slack: new SlackNotifier({
			log: this._log,
			user: this._user
		}),
		sendgrid: new SendgridNotifier({
			log: this._log,
			user: this._user
		})
	};

	return Promise.each(Object.keys(notifiers), function(type) {
		return notifiers[type].isEnabled().then(function(enabled) {
			if (!enabled) {
				delete notifiers[type];
			}
		});
	}).then(function() {
		if (Object.keys(notifiers).length === 0) {
			throw new Error('NoNotifiers');
		}
		return notifiers;
	});
};

/**
 * @returns {[string]} Character names in Perlan-Mazrigos format
 */
SendNotifications.prototype._getCharacterNames = function() {
	return this._user.getCharactersOnRealm(this._region, this._realm).then(function(characters) {
		var ahnames = characters.map(function(character) {
			return character.name + '-' + realms[character.region].bySlug[character.realm].ah;
		});
		return ahnames;
	});
};

/**
 * @returns {Auctions} Last processed auctions
 */
SendNotifications.prototype._loadCurrentAuctions = function() {
	return Promise.bind(this).then(function() {
		return this._auctionStore.getLastProcessedTime(this._region, this._realm);
	}).then(function(lastProcessed) {
		return this._auctionStore.loadProcessedAuctions(this._region, this._realm, lastProcessed);
	});
};

SendNotifications.prototype._processForCharacter = function(ahname, auctions, settings) {
	var self = this;
	if (!auctions.index.owners[ahname]) { return; }

	var items = {};
	Object.keys(auctions.index.owners[ahname]).forEach(function(id) {

		// return if the owner's most expensive auction for the item is
		// below the notification threshold
		var myMaxPrice;
		auctions.index2.items[id].forEach(function(auction) {
			if (auction.owner !== ahname) { return; }
			if (!myMaxPrice || auction.buyoutPerItem > myMaxPrice) {
				myMaxPrice = auction.buyoutPerItem;
			}
		});
		if (myMaxPrice <= settings.minValue) { return; }


		if (auctions._priceChanges && auctions._priceChanges[id]) {
			items[id] = self._createItemNotification(auctions, id, ahname);
		}
	});

	if (!Object.keys(items).length) { return; }

	return {
		region: self._region,
		character: ahname,
		items: items
	};
};

SendNotifications.prototype._createItemNotification = function(auctions, itemId, characterName) {
	var itemAuctions = auctions.getItemAuctionIds(itemId).map(function(auctionId) {
		var result = {
			id: auctionId,
			auction: auctions.getAuction(auctionId)
		};
		if (auctions._changes.relisted[itemId] && auctions._changes.relisted[itemId][auctionId]) {
			result.relisted = auctions._changes.relisted[itemId][auctionId].buyoutPerItem;
		}
		if (result.auction.owner === characterName) {
			result.own = true;
		}
		return result;
	});

	var simplified = simplifyItems(itemAuctions);
	var sold = [];
	if (auctions._changes.sold[itemId]) {
		sold = auctions._changes.sold[itemId];
	}

	return {
		id: itemId,
		auctions: itemAuctions,
		simplified: simplified,
		sold: sold,
		item: itemDb[itemId],
	};



	/**
	 * Simplifies a list of auctions buy merging stacks with the same price
	 *
	 * @param {object[]} items
	 * @param {number} items[].id
	 * @param {Auction} items[].auction
	 * @param {number?} items[].relisted
	 * @returns {SimplifiedAuction[]}
	 */
	function simplifyItems(items) {
		var cur;
		var result = [];
		items.forEach(function(o) {
			var item = o.auction;
			if (!cur || cur.owner !== item.owner || cur.buyoutPerItem !== item.buyoutPerItem) {
				cur = item;
				cur.ids = [o.id];
				cur.stacks = {};
				cur.stacks[item.quantity] = 1;
				cur.sum = item.quantity;
				cur.relisted = o.relisted;
				result.push(cur);
			} else {
				cur.ids.push(o.id);
				cur.stacks[item.quantity] = (cur.stacks[item.quantity] ? cur.stacks[item.quantity] : 0) + 1;
				cur.sum += item.quantity;
				cur.relisted = cur.relisted || o.relisted;
			}
		});
		return result;
	}

};

function SlackNotifier(opt) {
	if (!opt.log) { throw new Error('opt.log must be defined'); }
	if (!opt.user) { throw new Error('opt.user must be defined'); }

	this._log = opt.log;
	this._user = opt.user;
}

SlackNotifier.prototype.isEnabled = function() {
	return this._user.getSettings().then(function(settings) {
		return !!settings.slackWebhook;
	});
};

SlackNotifier.prototype.send = function(owners) {
	return Promise.bind(this).then(function() {
		return SlackNotifier.createAttachments(owners);
	}).then(function(attachments) {

		return this._user.getSettings().bind(this).then(function(settings) {
			return request({
				method: 'post',
				uri: settings.slackWebhook,
				json: {
					username: 'Me Sell You Not',
					icon_url: 'https://mesellyounot.blob.core.windows.net/public/msyn48.png',
					text: 'Undercuts found at ' + owners.lastModified,
					attachments: attachments,
					channel: settings.slackChannel
				}
			}).then(function(res) {
				// TODO: remove last slack error
			}).catch(function(err) {
				self._log.error({err: err}, 'slack error');
				if (err.name === 'StatusCodeError') {
					// TODO: show this to the user
				}
			});
		});
	});


};

SlackNotifier.createAttachments = function(owners) {
	var attachments = [];
	Object.keys(owners.characters).forEach(function(characterName) {
		var owner = owners.characters[characterName];
		Object.keys(owner.items).forEach(function (itemId) {
			var a = SlackNotifier.forItem(owner.character, owners.region, itemId, owner.items[itemId]);
			attachments.push(a);
		});
	});
	return attachments;
};

SlackNotifier.forItem = function(owner, region, itemId, items) {
	var texts = [];
	items.sold.forEach(function(item) {
		var pluralized = item.quantity > 1 ? 'stacks' : 'stack';
		texts.push(util.format('%s sold %s %s for %s each', item.owner, item.quantity, pluralized, formatPrice(item.buyoutPerItem)));
	});

	var avail = 2;
	var skipped = 0;

	items.simplified.forEach(function(item, index, arr) {
		var own = item.owner === owner;
		if (avail > 0 || own || index === arr.length - 1) {
			if (skipped) {
				var plural = skipped > 1 ? 'items' : 'item';
				texts.push('[... ' + skipped + ' more ' + plural + ' ...]');
				skipped = 0;
			}

			var stacks = [];
			for (var x in item.stacks) {
					var pluralized = item.stacks[x] > 1 ? 'stacks' : 'stack';
					stacks.push(util.format('%s %s of %s', item.stacks[x], pluralized, x));
			}
			if (item.relisted) {
				stacks.push('relisted');
			}

			var txt = formatPrice(item.buyoutPerItem) + ': ' + item.owner + ' (' + stacks.join(', ') + ')';
			if (own) {
				txt = '*' + txt + '*';
			}
			texts.push(txt);
			avail--;
		} else {
			skipped += item.sum;
		}
	});
	if (skipped) {
		var plural = skipped > 1 ? 'items' : 'item';
		texts.push('[... ' + skipped + ' more ' + plural + ' ...]');
	}

	var text = texts.join('\n');
	return {
		author_name: items.item.n,
		author_link: 'https://' + region + '.battle.net/wow/en/vault/character/auction/browse?sort=buyout&reverse=false&itemId=' + itemId,
		author_icon: 'https://wow.zamimg.com/images/wow/icons/large/' + items.item.i + '.jpg',
		text: text,
		mrkdwn_in: ['text']
	};


	function formatPrice(price) {
		var gold = Math.floor(price / 10000);
		var silver = Math.floor(price % 10000 / 100);
		var copper = Math.floor(price % 100);
		return gold + 'g ' + silver + 's ' + copper + 'c';
	}
};


function SendgridNotifier(opt) {
	if (!opt.log) { throw new Error('opt.log must be defined'); }
	if (!opt.user) { throw new Error('opt.user must be defined'); }

	this._log = opt.log;
	this._user = opt.user;
}

SendgridNotifier.prototype.isEnabled = function() {
	return this._user.getSettings().then(function(settings) {
		return !!settings.email;
	});
};

SendgridNotifier.prototype.send = function(owners) {
	return Promise.bind(this).then(function() {
		var attachments = SlackNotifier.createAttachments(owners);

		var txt = attachments.map(function(att) {
			var txt = att.author_name + '\n' + Array(att.author_name.length + 1).join('-') + '\n';
			txt += att.text;
			return txt;
		}).join('\n\n');

		return txt;
	}).then(function(txt) {

		txt = '<pre>' + encoder.htmlEncode(txt) + '</pre>';

		return this._user.getSettings().bind(this).then(function(settings) {
			var email = new sendgrid.Email({
				to: settings.email,
				from: 'notifications+changes@mesellyounot.com',
				fromname: 'Me Sell You Not',
				subject: 'Changes detected',
				html: txt
			});
			return sendgrid.sendAsync(email).bind(this).then(function(res) {
				// report
				this._log.info({response: res, email: settings.email}, 'sendgrid success');
			}).catch(function(err) {
				// report
				this._log.error({err: err}, 'sendgrid error');
			});
		});
	});
};

module.exports = SendNotifications;
