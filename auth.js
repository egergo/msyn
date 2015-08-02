var JwtStrategy = require('passport-jwt').Strategy;
var JWT = require('jsonwebtoken');

var User = require('./user');

/**
 * @param {object} opt
 * @param {Passport} opt.passport
 * @param {string} opt.secret
 */
function Auth(opt) {
	opt = opt || {};
	if (!opt.tables) { throw new Error('opt.tables must be specified'); }
	if (!opt.passport) { throw new Error('opt.passport must be specified'); }
	if (!opt.secret) { throw new Error('opt.secret must be specified'); }

	this._tables = opt.tables;
	this._passport = opt.passport;
	this._secret = opt.secret;
}

Auth.prototype.init = function() {
	var self = this;
	this._passport.use(new JwtStrategy({
		secretOrKey: this._secret,
		authScheme: 'Bearer'
	}, function(jwt, done) {
		var user = new User({
			tables: self._tables,
			id: jwt.sub
		});
		return done(null, user);
	}));
};

Auth.prototype.issueToken = function(opt) {
	opt = opt || {};
	if (!opt.userId) { throw new Error('opt.userId must be specified'); }

	return JWT.sign({}, this._secret, {subject: opt.userId});
};

module.exports = Auth;