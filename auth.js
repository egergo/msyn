var JwtStrategy = require('passport-jwt').Strategy;
var JWT = require('jsonwebtoken');

/**
 * @param {object} opt
 * @param {Passport} opt.passport
 * @param {string} opt.secret
 */
function Auth(opt) {
	opt = opt || {};
	if (!opt.passport) { throw new Error('opt.passport must be specified'); }
	if (!opt.secret) { throw new Error('opt.secret must be specified'); }

	this._passport = opt.passport;
	this._secret = opt.secret;
}

Auth.prototype.init = function() {
	this._passport.use(new JwtStrategy({
		secretOrKey: this._secret,
		authScheme: 'Bearer'
	}, function(jwt, done) {
		return done(null, {
			id: jwt.sub
		});
	}));
};

Auth.prototype.issueToken = function(opt) {
	opt = opt || {};
	if (!opt.userId) { throw new Error('opt.userId must be specified'); }

	return JWT.sign({}, this._secret, {subject: opt.userId});
};

module.exports = Auth;