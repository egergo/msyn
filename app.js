var express = require('express');

var Passport = require('passport').Passport;
var request = require('request-promise');
var Promise = require('bluebird');
var exphbs = require('express-handlebars');
var azureCommon = require('azure-common');
var azureStorage = require('azure-storage');

var log = require('./log');
var Auth = require('./auth');
var User = require('./user');
var realms = require('./realms');
var bnet = require('./bnet');

var app = express();
app.use(log.requestLogger());
app.enable('trust proxy');
app.disable('x-powered-by');

app.engine('.hbs', exphbs({defaultLayout: false, extname: '.hbs'}));
app.set('view engine', '.hbs');

var retryOperations = new azureCommon.ExponentialRetryPolicyFilter();
var tables = azureStorage.createTableService(process.env.AZURE_STORAGE_CONNECTION_STRING)
	.withFilter(retryOperations);
Promise.promisifyAll(tables);

var passport = new Passport;
app.use(passport.initialize());

var auth = new Auth({
	tables: tables,
	passport: passport,
	secret: process.env.JWT_SECRET
});
auth.init();


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
