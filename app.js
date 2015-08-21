var express = require('express');

var Passport = require('passport').Passport;
var request = require('request-promise');
var Promise = require('bluebird');
var ExpressHandlerbars = require('express-handlebars').ExpressHandlebars;
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');
var util = require('util');
var zlib = require('zlib');

var log = require('./log');
var Auth = require('./auth');
var User = require('./user');
var realms = require('./realms');
var bnet = require('./bnet');
var Auctions = require('./auction_house').Auctions;
var items = require('./items');
var Azure = require('./platform_services/azure');

var app = express();
app.use(log.requestLogger());
app.enable('trust proxy');
app.disable('x-powered-by');


var exphbs = new ExpressHandlerbars({
	defaultLayout: false,
	extname: '.hbs',
	helpers: {
		json: function(o) { return new exphbs.handlebars.SafeString(JSON.stringify(o)); }
	}
});
app.engine('.hbs', exphbs.engine);
app.set('view engine', '.hbs');

var retryOperations = new azureCommon.ExponentialRetryPolicyFilter();
var tables = azureStorage.createTableService(process.env.AZURE_STORAGE_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(tables);

var blobs = azureStorage.createBlobService(process.env.AZURE_STORAGE_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(blobs);

var azure = Azure.createFromEnv();

var passport = new Passport;
app.use(passport.initialize());

var auth = new Auth({
	tables: tables,
	passport: passport,
	secret: process.env.JWT_SECRET
});
auth.init();

app.use(express.static('public'));

var BnetStrategy = require('passport-bnet').Strategy;
passport.use(new BnetStrategy({
    clientID: process.env.BNET_ID,
    clientSecret: process.env.BNET_SECRET,
    callbackURL: process.env.BNET_CALLBACK,
    scope: ['wow.profile'],
    region: 'eu'
}, function(accessToken, refreshToken, profile, done) {
	var user = new User({
		id: profile.id,
		tables: tables
	});
	user.login(profile, accessToken).then(function() {
		done(null, user);
	}).catch(function(err) {
		done(err);
	});
}));

app.get('/', function(req, res) {
	res.render('index', {
		init: {
			defaultOrigin: process.env.DEFAULT_ORIGIN,
			secureOrigin: process.env.SECURE_ORIGIN
		}
	});
});

app.get('/characters', passport.authenticate('jwt', {session: false}), function(req, res, next) {
	req.user.load().then(function(user) {
		var characters = {};
		if (user.characters_eu) {
			characters.eu = JSON.parse(user.characters_eu._);
		}
		if (user.characters_us) {
			characters.us = JSON.parse(user.characters_us._);
		}
		if (user.characters_kr) {
			characters.kr = JSON.parse(user.characters_kr._);
		}
		if (user.characters_tw) {
			characters.tw = JSON.parse(user.characters_tw._);
		}
		res.send(characters);
	}).catch(function(err) {
		next(err);
	})
});

app.get('/auctions', passport.authenticate('jwt', {session: false}), function(req, res, next) {
	req.user.load().then(function(user) {
		var toons = gatherToons(user);
		return fetchAuctions(toons);

		return 'OK';
	}).then(function(result) {
		res.send(result);
	}).catch(function(err) {
		next(err);
	});

	function fetchAuctions(toons) {
		var result = {};
		result = [];
		return Promise.map(Object.keys(toons), function(key) {
			var toon = toons[key];
			return loadAH(toon.region, toon.realm).then(function(auctions) {
				if (!auctions) { return; }

				return toon.characters.forEach(function(character) {
					var name = character.name + '-' + realms[character.region].bySlug[character.realm].ah;
					var ownerIndex = auctions.index.owners[name];
					if (ownerIndex) {
						Object.keys(ownerIndex).forEach(function(itemId) {
							ownerIndex[itemId] = {
								item: items[itemId],
								auctions: auctions.index.items[itemId].map(function(auctionId) {
									return auctions.auctions[auctionId];
								})
							};
						});
					}
					result.push({
						character: character,
						auctions: ownerIndex,
						lastModified: auctions.lastModified
					});
				});
			});
		}).then(function() {
			return result;
		});
	}

	function futureStream(stream) {
		var bufs = [];
		var resolver = Promise.pending();
		stream.on('data', function(d) {
			bufs.push(d);
		});
		stream.on('end', function() {
			var buf = Buffer.concat(bufs);
			resolver.resolve(buf);
		});
		return resolver.promise;
	}

	function loadFile(path) {
		var gunzip = zlib.createGunzip();
		var promise = futureStream(gunzip);

		var az = blobs.getBlobToStreamAsync('realms', path, gunzip);
		return Promise.all([promise, az]).spread(function(res) {
			return JSON.parse(res);
		});
	}

	function loadPastAuctions(region, realm, lastProcessed) {
		// TODO: return lastProcessed object from previous iteration
		if (!lastProcessed) { return Promise.resolve(); }
		// TODO: make lastProcessed a date
		var date = new Date(lastProcessed);
		var name = util.format('processed/%s/%s/%s/%s/%s/%s.gz', region, realm, date.getFullYear(), date.getMonth() + 1, date.getDate(), date.getTime());
		return loadFile(name).catch(function(err) {
			if (err.name === 'Error' && err.message === 'NotFound') {
				log.error({region: region, realm: realm, lastProcessed: lastProcessed, name: name}, 'last processed not found');
				return;
			}
			throw err;
		});
	}

	function loadAH(region, realm) {
		return tables.retrieveEntityAsync('cache', 'current-' + region + '-' + realm, '').spread(function(result) {
			return result.lastProcessed._.getTime();
		}).catch(function() {
			// TODO: check error
			//throw new Error('realm not found: ' + region + '-' + realm);
			return undefined;
		}).then(function(lastProcessed) {
			if (!lastProcessed) { return; }

			return loadPastAuctions(region, realm, lastProcessed).then(function(ah) {
				// check if notfound
				return new Auctions({
					lastModified: lastProcessed,
					past: ah
				});
			})
		});
	}

	function gatherToons(user) {
		var result = {};
		['us', 'eu', 'kr', 'tw'].forEach(function(region) {
			if (!user['characters_' + region]) { return; }
			var chars = JSON.parse(user['characters_' + region]._);
			chars.characters.forEach(function(toon) {
				var real = realms[toon.region].bySlug[toon.realm].real;
				var desc = result[region + '-' + real];
				if (!desc) {
					desc = result[region + '-' + real] = {
						region: region,
						realm: real,
						characters: []
					};
				}
				desc.characters.push(toon);
			})
		});
		return result;
	}
});

app.get('/auth/bnet', passport.authenticate('bnet'));
app.get('/auth/bnet/callback', function(req, res, next) {
	passport.authenticate('bnet', function(err, user) {
		if (err) { return next(err); }

		user.getAccessToken().then(function(accessToken) {
			var regions = ['us', 'eu', 'kr', 'tw'];
			var proms = regions.map(function(region) {
				return bnet.fetchUserCharacters({
					accessToken: accessToken,
					region: region
				});
			})
			return Promise.settle(proms).then(function(results) {
				var characters = {};
				for (var x = 0; x < regions.length; x++) {
					if (results[x].isFulfilled()) {
						characters[regions[x]] = results[x].value();
					} else {
						var err = results[x].reason();
						log.error({region: regions[x], userId: user.id, err: err}, 'cannot fetch toons from region %s for user %s', regions[x], user.id);
					}
				}
				return characters;
			});
		}).then(function(characters) {
			user.saveCharacters(characters);
		}).catch(function(err) {
			log.error({err: err, userId: user.id}, 'could not save characters');
		});

		var token = auth.issueToken({userId: user.id});
		res.render('auth_callback', {
			targetOrigin: process.env.DEFAULT_ORIGIN,
			token: token
		});
	})(req, res);
});

//passport.authenticate('jwt', {session: false})
app.get('/realmStatus', function(req, res, next) {
	return azure.tables.queryEntitiesAsync('RealmFetches', null, null).spread(function(r) {
		var index = {};
		r.entries.forEach(function(entry) {
			index[entry.RowKey._] = entry;
		});

		var result = {};
		realms.regions.forEach(function(region) {
			result[region] = Object.keys(realms[region].bySlug).map(function(slug) {
				var realm = realms[region].bySlug[slug];
				var entry = index[region + '-' + realm.real];
				return {
					name: realm.name,
					slug: slug,
					real: realm.real,
					enabled: entry ? entry.Enabled._ : undefined,
					lastModified: entry ? (entry.LastModified ? entry.LastModified._.getTime() : undefined) : undefined,
					lastFetched: entry ? (entry.LastFetched ? entry.LastFetched._.getTime() : undefined) : undefined,
					url: entry ? (entry.URL ? entry.URL._ : undefined) : undefined
				};
			});
		});
		return result;

	}).then(function(result) {
		res.send(result);
	}).catch(function(err) {
		next(err);
	});
});



// log errors
app.use(log.errorLogger());

// error handler
app.use(function(err, req, res, next) {
	// don't do anything if the response was already sent
	if (res.headersSent) {
		return;
	}

	res.status(500);

	if (req.accepts('html')) {
		res.send('Internal Server Error. Request identifier: ' + req.id);
		return;
	}

	if (req.accepts('json')) {
		res.json({ error: 'Internal Server Error', requestId: req.id });
		return;
	}

	res.type('txt').send('Internal Server Error. Request identifier: ' + req.id);
});

var port = process.env.PORT || 3000;

app.listen(port, function(err) {
	if (err) { return log.error(err); }
	log.info({port: port}, 'listening on %s', port);
});

module.exports = {
	app: app
};
