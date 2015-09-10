/*jshint expr: true*/

var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');

var expect = chai.expect;
var should = chai.should();

var Auth = require('../auth');
var Passport = require('passport').Passport;
var JWT = require('jsonwebtoken');
var express = require('express');
var errorhandler = require('errorhandler');

var SECRET = 'muchsecret';

describe('Auth', function() {

	var passport;
	var auth;

	beforeEach(function() {
		passport = new Passport();
		auth = new Auth({
			tables: {},
			passport: passport,
			secret: SECRET
		});
		auth.init();
	});

	it('should issue a correct token', function() {
		var USER_ID = 17;

		var token = auth.issueToken({userId: USER_ID});
		var data = JWT.verify(token, SECRET);
		data.sub.should.be.equal(USER_ID);
		data.iat.should.exist;
	});

	it('should accept a correct token', function(done) {
		var TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE0MzgzMjQ1NTAsInN1YiI6MTd9.ZQ2ezJ8AdpUhSx99_6HXgzGvVtpjsedMSPEphkEGdVI';
		var USER_ID = 17;

		var app = express();
		app.use(passport.initialize());
		app.get('/', passport.authenticate('jwt', {session: false}), function(req, res) {
			res.send(req.user);
		});
		app.use(errorhandler({log: true}));

		supertest(app)
			.get('/')
			.set('Authorization', 'Bearer ' + TOKEN)
			.expect(200)
			.end(function(err, res) {
				if (err) { done(err); }
				res.body._id.should.be.equal('' + USER_ID);
				done();
			});
	});

});

