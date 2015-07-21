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

return Promise.resolve().then(function() {
	var ah = new AuctionHouse({
		redis: redis,
		region: region,
		realm: realm,
		key: process.env.BLIZZARD_KEY
	});
	return ah.load();
}).then(function(auctions) {
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

		var items = auctions.getOwnerItems(owner);
		if (auctions._changes && auctions._changes.owners[owner]) {
			auctions._changes.owners[owner].forEach(function(itemId) {
				items.push(itemId);
			});
		}

		var processed = {};
		return Promise.all(items.map(function(itemId) {
			if (processed[itemId]) { return; }
			processed[itemId] = true;
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

		var render = false;
		 var itemAuctions = auctions.getItemAuctions(itemId);
		// if (auctions._priceChanges) {
		// 	if (auctions._priceChanges[itemId]) {
		// 		render = true;
		// 	}
		// } else {
		// 	if (itemAuctions.length > 0 && itemAuctions[0].owner !== owner) {
		// 		render = true;
		// 	}
		// }
		render = true;

		if (!render) { return; }

		//itemAuctions = simplifyItems(itemAuctions);
		var text = itemAuctions.map(function(auctionId) {
			var item = auctions.getAuction(auctionId);

			//var txt = formatPrice(item.buyoutPerItem) + ': ' + item.owner + ' (' + stacks.join(', ') + ')';
			var txt = formatPrice(item.buyoutPerItem) + ': ' + item.owner;
			if (auctions._changes && auctions._changes.relisted[itemId] && auctions._changes.relisted[itemId][auctionId]) {
				txt += ' relisted from ' + formatPrice(auctions._changes.relisted[itemId][auctionId].buyoutPerItem);
			}
			if (item.owner === owner) {
				txt = '*' + txt + '*';
			}
			return txt;
		}).join('\n');

		if (auctions._changes) {
			if (auctions._changes.sold[itemId]) {
				text += auctions._changes.sold[itemId].map(function(item) {
					return util.format('\n%s items sold by %s for %s each', item.quantity, item.owner, formatPrice(item.buyoutPerItem));
				}).join('');
			}
		}

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

function simplifyItems(items) {
	var cur;
	var result = [];
	items.forEach(function(item) {
		if (!cur || cur.fullOwner !== item.fullOwner || cur.itemPrice !== item.itemPrice) {
			cur = item;
			cur.ids = [item.auc];
			cur.stacks = {};
			cur.stacks[item.quantity] = 1;
			cur.sum = item.quantity;
			result.push(cur);
		} else {
			cur.ids.push(item.auc);
			cur.stacks[item.quantity] = (cur.stacks[item.quantity] ? cur.stacks[item.quantity] : 0) + 1;
			cur.sum += item.quantity;
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


