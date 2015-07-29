var express = require('express');

var passport = require('passport');
var request = require('request-promise');
var Promise = require('bluebird');

var log = require('./log');

var app = express();
app.use(log.requestLogger());
app.enable('trust proxy');
app.disable('x-powered-by');

app.use(passport.initialize());



var BnetStrategy = require('passport-bnet').Strategy;

// Use the BnetStrategy within Passport.
passport.use(new BnetStrategy({
    clientID: process.env.BNET_ID,
    clientSecret: process.env.BNET_SECRET,
    callbackURL: "https://egergo.localtunnel.me/auth/bnet/callback",
    scope: ['wow.profile'],
    region: 'eu'
}, function(accessToken, refreshToken, profile, done) {
	console.log('auth', accessToken, refreshToken, profile);

    return done(null, profile, {accessToken: accessToken});
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

app.get('/auth/bnet/callback', function(req, res) {

	passport.authenticate('bnet', function(err, profile, info) {
		Promise.resolve().then(function() {
			console.log('authenticate', err, profile, info, arguments);
			refreshToons(profile, info.accessToken);
		});
	})(req, res);

  res.redirect('/');
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
