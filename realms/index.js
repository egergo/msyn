
function Realms() {
	this.eu = new Region('eu');
	this.us = new Region('us');
	this.kr = new Region('kr');
	this.tw = new Region('tw');
	this.ru = new Region('ru');
	this.regions = ['eu', 'us', 'kr', 'tw'];
}

function Region(region) {
	// TODO: clean up this shit
	Object.defineProperty(this, 'bySlug', {
		get: function() {
			if (!this._bySlug) {
				this._bySlug = require('./' + region + '.json');
			}
			Object.keys(this._bySlug).forEach(function(slug) {
				var real = this._bySlug[slug].real;
				if (!this._bySlug[real]) {
					this._bySlug[real] = this._bySlug[slug];
				}
			}, this);
			return this._bySlug;
		}
	});

	Object.defineProperty(this, 'byName', {
		get: function() {
			if (!this._byName) {
				var bySlug = this.bySlug;
				var byName = this._byName = {};
				Object.keys(this.bySlug).forEach(function(key) {
					byName[bySlug[key].name] = bySlug[key];
				});
			}
			return this._byName;
		}
	});

	Object.defineProperty(this, 'byAH', {
		get: function() {
			if (!this._byAH) {
				var bySlug = this.bySlug;
				var byAH = this._byAH = {};
				Object.keys(this.bySlug).forEach(function(key) {
					byAH[bySlug[key].ah] = bySlug[key];
				});
			}
			return this._byAH;
		}
	});
}

module.exports = new Realms;
module.exports.Realms = Realms;
