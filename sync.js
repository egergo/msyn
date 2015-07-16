var request = require('request-promise');
var urljoin = require('url-join');
var Promise = require('bluebird');
var util = require('util');

var xml2js = require('xml2js');
Promise.promisifyAll(xml2js);

var Redis = require('ioredis');
var redis = new Redis(process.env.REDIS_URI);

var key = process.env.BLIZZARD_KEY;
var endpoint = 'https://eu.api.battle.net';
var locale = 'en_GB';
var realm = 'Mazrigos';

var url = urljoin(endpoint, 'wow/auction/data', realm, '?locale=' + encodeURIComponent(locale) + '&apikey=' + encodeURIComponent(key));
Promise.resolve().then(function() {
	return request({
		uri: url,
		gzip: true
	});
}).then(function(res) {
	res = JSON.parse(res);
	console.log(res);
	return Promise.all(res.files.map(function(file) {

		return redis.get('realms:' + realm + ':auc:last').then(function(last) {
			if (last && last >= file.lastModified) { console.log('already checked'); return null; }
			redis.set('realms:' + realm + ':auc:last', file.lastModified);

			console.log('fetching', file.url);
			return request({
				uri: file.url,
				gzip: true
			});
		});
	}));
}).then(function(res) {

	var allItems = {_ownerIndex:{}};

	var items = [];
	res.forEach(function(res) {
		if (!res) { return; }

		res = JSON.parse(res);
		res.auctions.auctions.forEach(function(auc) {
			var arr = allItems[auc.item];
			if (!arr) {
				arr = allItems[auc.item] = [];
			}
			auc.fullOwner = auc.owner + '-' + auc.ownerRealm + '-' + 'eu';
			auc.itemPrice = auc.buyout / auc.quantity;
			var ownerIndex = allItems._ownerIndex[auc.fullOwner];
			if (!ownerIndex) {
				allItems._ownerIndex[auc.fullOwner] = ownerIndex = {};
			}
			ownerIndex[auc.item] = true;
			arr.push(auc);
		});
	});
	return allItems;
})
.then(sortAllItems)
.then(simplifyAllItems)
.then(function(allItems) {
	return sendNotifications('eu', 'Mazrigos', allItems);
})
.catch(function(err) {
	console.log('bazge', err, err.stack);
}).finally(function() {
	process.exit();
});

function reportToSlack2(allItems, watched, region, user) {
	return Promise.resolve().then(function() {
		if (!user || !user.slackHook) { return; }

		var ownToons = {};
		user.toons.forEach(function(toon) {
			ownToons[toon.name + '-' + toon.realm + '-' + toon.region] = true;;
		});

		var promises = [];
		for (var x in watched) {
			promises.push(createAttachment(x, allItems, region, ownToons));
		}
		return Promise.all(promises);
	}).then(function(attachments) {
		attachments = attachments.filter(function(item) { return !!item; });
		if (!attachments.length) { return; }

		return request({
			method: 'post',
			uri: user.slackHook,
			json: {
				text: 'Undercuts found for ' + user.battletag,
				channel: user.slackChannel,
				attachments: attachments
			}
		});
	}).then(function() {
		return allItems;
	});
}

function createAttachment(itemId, items, region, ownToons) {
	return Promise.resolve().then(function() {
		return fetchItem(itemId);
	}).then(function(itemDesc) {
		var first = true;
		var noNotificationNeeded = false;

		var text = items[itemId].map(function(item) {
			if (item.itemPrice == 0) { return undefined; }

			var stacks = [];
			for (var x in item.stacks) {
				var pluralized = item.stacks[x] > 1 ? 'stacks' : 'stack';
				stacks.push(util.format('%s %s of %s', item.stacks[x], pluralized, x));
			}
			var txt = formatPrice(item.itemPrice) + ': ' + item.owner + '-' + item.ownerRealm + ' (' + stacks.join(', ') + ')';
			if (ownToons[item.fullOwner]) {
				txt = '*' + txt + '*';
				if (first) { noNotificationNeeded = true; return undefined; }
			}
			first = false;
			return txt;
		}).join('\n');

		if (noNotificationNeeded || !text) { return undefined; }

		return {
			author_name: itemDesc.name,
			author_link: 'https://' + region + '.battle.net/wow/en/vault/character/auction/browse?sort=buyout&reverse=false&itemId=' + itemId,
			author_icon: 'https://wow.zamimg.com/images/wow/icons/large/' + itemDesc.icon + '.jpg',
			text: text,
			mrkdwn_in: ['text']
		};
	});
}

function sortAllItems(allItems) {
	for (var x in allItems) {
		if (x.charAt(0) === '_') { continue; }
		sortItems(allItems[x]);
	}
	return allItems;
}

function sortItems(items) {
	return items.sort(function(a, b) {
		var result = a.itemPrice - b.itemPrice;
		if (result == 0) {
			result = a.fullOwner.localeCompare(b);
		}
		return result;
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

var itemCache = {};
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
			console.log('all users', users);
			return Promise.all(users.map(function(userId) {
				return sendNotificationToUser(region, realm, items, userId);
			}));
		});
	});
}

function sendNotificationToUser(region, realm, items, userId) {
	return Promise.resolve().then(function() {
		return redis.get(util.format('users:%s', userId))
	}).then(function(user) {
		user = JSON.parse(user);
		if (!user || !user.toons) { return; }
		var futures = [];
		user.toons.forEach(function(toon) {
			if (toon.region !== region) { return; }

			var ownerIndex = items._ownerIndex[toon.name + '-' + toon.realm + '-' + region]
			console.log('full name', toon.name + '-' + toon.realm, ownerIndex);
			if (!ownerIndex) { return; }
			futures.push(reportToSlack2(items, ownerIndex, region, user));
		});
		return Promise.all(futures);
	});
}


