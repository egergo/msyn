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

function refreshToons(profile, accessToken) {

	return request({
		uri: 'https://eu.api.battle.net/wow/user/characters?access_token=' + encodeURIComponent(accessToken),
		gzip: true
	}).then(function(res) {

		realms = {};
		res = JSON.parse(res);
		var characters = res.characters.map(function(character) {
			realms[realmToSlug(character.realm)] = true;
			return {
				name: character.name,
				realm: character.realm
			}
		});


		console.log(realms);
		console.log(characters);
	});

}

function realmToSlug(realmName) {
	return realmName.toLowerCase();
}


app.get('/auth/bnet', passport.authenticate('bnet'));
app.get('/auth/bnet/callback', function(req, res, next) {
	passport.authenticate('bnet', function(err, user) {
		if (err) { return next(err); }

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
