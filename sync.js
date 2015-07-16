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
		var text = items[itemId].map(function(item) {
			var txt = formatPrice(item.buyout / item.quantity) + ': ' + item.owner + '-' + item.ownerRealm;
			if (ownToons[item.fullOwner]) {
				txt = '*' + txt + '*';
			}
			return txt;
		}).join('\n');

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
		return (a.buyout / a.quantity) - (b.buyout / b.quantity);
	});
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


