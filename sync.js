var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var Redis = require('ioredis');
var redis = new Redis(process.env.REDIS_URI);

var log = require('./log');
var AuctionHouse = require('./auction_house');


var region = 'eu';
var realm = 'mazrigos';

var itemCache = {};

module.exports.reportToSlack2 = reportToSlack2;
if (require.main !== module) { return; }

return Promise.resolve().then(function() {
	var ah = new AuctionHouse({
		redis: redis,
		region: region,
		realm: realm,
		key: process.env.BLIZZARD_KEY
	});
	return ah.load();
}).then(function(auctions) {
	if (!auctions._changes) { throw new Error('no changes detected'); }

	return sendNotifications(region, realm, auctions);
}).catch(function(err) {
	log.error({err:err}, 'exception: ' + err.stack);
}).finally(function() {
	process.exit();
});

// auctions, region, user, owner
function reportToSlack2(auctions, region, user, owner) {
	return Promise.resolve().then(function() {
		if (!user || !user.slackHook) { return; }

		var itemsIdsToNotify = {};
		auctions.getOwnerItemIds(owner).forEach(function(itemId) {
			// only work on auctions where the min price has changed
			if (auctions._priceChanges[itemId]) {
				itemsIdsToNotify[itemId] = true;
			}
		});


		if (auctions._changes.owners[owner]) {
			auctions._changes.owners[owner].forEach(function(itemId) {
				itemsIdsToNotify[itemId] = true;
			});
		}

		return Promise.all(Object.keys(itemsIdsToNotify).map(function(itemId) {
			return createAttachment(itemId, auctions, region, owner);
		}));
	}).then(function(attachments) {
		attachments = attachments.filter(function(item) { return !!item; });
		if (!attachments.length) { return; }

		return request({
			method: 'post',
			uri: user.slackHook,
			json: {
				text: 'Undercuts found for ' + owner,
				channel: user.slackChannel,
				attachments: attachments
			}
		});
	}).then(function() {
		return auctions;
	});
}

function createAttachment(itemId, auctions, region, owner) {
	console.log('createAttachment', owner, itemId);
	return Promise.resolve().then(function() {
		return fetchItem(itemId);
	}).then(function(itemDesc) {

		var itemAuctions = auctions.getItemAuctionIds(itemId).map(function(auctionId) {
			var result = {
				id: auctionId,
				auction: auctions.getAuction(auctionId)
			};
			if (auctions._changes.relisted[itemId] && auctions._changes.relisted[itemId][auctionId]) {
				result.relisted = auctions._changes.relisted[itemId][auctionId].buyoutPerItem;
			}
			return result;
		});

		itemAuctions = simplifyItems(itemAuctions);
		var texts = [];

		if (auctions._changes.sold[itemId]) {
			auctions._changes.sold[itemId].forEach(function(item) {
				var pluralized = item.quantity > 1 ? 'stacks' : 'stack';
				texts.push(util.format('%s sold %s %s for %s each', item.owner, item.quantity, pluralized, formatPrice(item.buyoutPerItem)));
			});
		}

		itemAuctions.forEach(function(item) {

			var stacks = [];
			for (var x in item.stacks) {
					var pluralized = item.stacks[x] > 1 ? 'stacks' : 'stack';
					stacks.push(util.format('%s %s of %s', item.stacks[x], pluralized, x));
			}
			if (item.relisted) {
				stacks.push('relisted');
			}

			var txt = formatPrice(item.buyoutPerItem) + ': ' + item.owner + ' (' + stacks.join(', ') + ')';
			if (item.owner === owner) {
				txt = '*' + txt + '*';
			}
			texts.push(txt);
		});


		var text = texts.join('\n');

		return {
			author_name: itemDesc.name,
			author_link: 'https://' + region + '.battle.net/wow/en/vault/character/auction/browse?sort=buyout&reverse=false&itemId=' + itemId,
			author_icon: 'https://wow.zamimg.com/images/wow/icons/large/' + itemDesc.icon + '.jpg',
			text: text,
			mrkdwn_in: ['text']
		};
	});
}

function simplifyAllItems(allItems) {
	for (var x in allItems) {
		if (x.charAt(0) === '_') { continue; }
		allItems[x] = simplifyItems(allItems[x]);
	}
	return allItems;
}

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

function fetchItem(itemId) {
	return Promise.resolve().then(function() {
		if (itemCache[itemId]) {
			return itemCache[itemId];
		}

		return request({
			uri: 'http://www.wowhead.com/item=' + itemId + '&xml'
		}).then(function(xml) {
			return xml2js.parseStringAsync(xml);
		}).then(function(itemRaw) {
			if (itemRaw.wowhead.error) { throw new Error('Wowhead error (item=' + itemId + '): ' + itemRaw.wowhead.error[0]); }

			var item = {
				name: itemRaw.wowhead.item[0].name[0],
				icon: itemRaw.wowhead.item[0].icon[0]._
			};
			itemCache[itemId] = item;
			return item;
		});
	});
}

function formatPrice(price) {
	var gold = Math.floor(price / 10000);
	var silver = Math.floor(price % 10000 / 100);
	var copper = Math.floor(price % 100);
	return gold + 'g ' + silver + 's ' + copper + 'c';
}

function sendNotifications(region, realm, items) {
	return Promise.resolve().then(function() {
		return redis.smembers(util.format('realms:%s:%s:users', region, realm)).then(function(users) {
			return Promise.all(users.map(function(userId) {
				return sendNotificationToUser(region, realm, items, userId);
			}));
		});
	});
}

function sendNotificationToUser(region, realm, auctions, userId) {
	return Promise.resolve().then(function() {
		return redis.get(util.format('users:%s', userId))
	}).then(function(user) {
		user = JSON.parse(user);
		if (!user || !user.toons) { return; }
		return Promise.all(user.toons.map(function(toon) {
			var owner = toon.name + '-' + toon.realm;
			return reportToSlack2(auctions, region, user, owner);
		}));
	});
}
