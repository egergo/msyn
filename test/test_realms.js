var chai = require('chai');
var sinon = require('sinon');
var supertest = require('supertest');

var expect = chai.expect;
var should = chai.should();

var realms = require('../realms');

describe('Realms', function() {

	it('eu', function() {
		realms.eu.byName['Aggra (Português)'].slug.should.be.equal('aggra-portugues');
		realms.eu.byAH['Aggra(Português)'].slug.should.be.equal('aggra-portugues');
		realms.eu.bySlug['aggra-portugues'].slug.should.be.equal('aggra-portugues');
		expect(realms.eu.byName['nonexistent']).to.not.exist;
	});

	it('us', function() {
		realms.us.byName['Aerie Peak'].slug.should.be.equal('aerie-peak');
		realms.us.byAH['AeriePeak'].slug.should.be.equal('aerie-peak');
		realms.us.bySlug['aerie-peak'].slug.should.be.equal('aerie-peak');
	});

	it('kr', function() {
		realms.kr.byName['헬스크림'].slug.should.be.equal('헬스크림');
		realms.kr.byAH['헬스크림'].slug.should.be.equal('헬스크림');
		realms.kr.bySlug['헬스크림'].slug.should.be.equal('헬스크림');
	});

	it('tw', function() {
		realms.tw.byName['血之谷'].slug.should.be.equal('血之谷');
		realms.tw.byAH['血之谷'].slug.should.be.equal('血之谷');
		realms.tw.bySlug['血之谷'].slug.should.be.equal('血之谷');
	});

});