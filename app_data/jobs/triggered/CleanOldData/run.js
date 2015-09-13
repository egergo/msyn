var Promise = require('bluebird');
var Azure = require('../../../../platform_services/azure');
var realms = require('../../../../realms');
var log = require('../../../../log').child({process: 'CleanOldData'});

var azure = Azure.createFromEnv();

module.exports = function() {

	return Promise.resolve().then(function() {
		return processTables(function(tables) {
			return Promise.each(tables, function(name) {
				var state = checkTable(name);
				if (state === 2) {
					log.info({action: 'delete', name: name}, 'deleting table: %s', name);
					return azure.tables.deleteTableAsync(name).catch(function(err) {
						log.error({name: name, err: err}, 'cannot delete table: %s', name);
					});
				} else if (!state) {
					log.error({action: 'unknown', name: name}, 'unknown table: %s', name);
				}
			});
		});
	}).then(function() {
		return processContainers(function(containers) {
			return Promise.each(containers, function(entry) {
				var name = entry.name;
				var state = checkContainer(name);
				if (state === 2) {
					log.info({action: 'delete', name: name}, 'deleting container: %s', name);
					return azure.containers.deleteContainerAsync(name).catch(function(err) {
						log.error({name: name, err: err}, 'cannot delete container: %s', name);
					});
				} else if (!state) {
					log.error({action: 'unknown', name: name}, 'unknown container: %s', name);
				}
			});
		});
	})

	function processTables(cb, next) {
		return azure.tables.listTablesSegmentedAsync(next).spread(function(res) {
			return Promise.resolve(cb(res.entries)).then(function() {
				if (res.continuationToken) {
					return processTables(cb, res.continuationToken);
				}
			});
		});
	}

	function processContainers(cb, next) {
		return azure.blobs.listContainersSegmentedAsync(next).spread(function(res) {
			return Promise.resolve(cb(res.entries)).then(function() {
				if (res.continuationToken) {
					return processContainers(cb, res.continuationToken);
				}
			});
		});
	}

	/**
	 * @returns 1 - keep, 2 - delete
	 */
	function checkContainer(name) {
		var valids = [];
		var m;
		var now = new Date;
		var sevenDaysAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);

		if (valids.indexOf(name) !== -1) {
			return 1;
		} else if (m = name.match(/^xauctions([0-9]{4})([0-9]{2})([0-9]{2})$/)) {
			var date = new Date(Date.UTC(parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3])));
			if (date < sevenDaysAgo) { return 2; }
			return 1;
		}
	}

	/**
	 * @returns 1 - keep, 2 - delete
	 */
	function checkTable(name) {
		var valids = ['users', 'cache', 'RealmFetches', 'tasks'];
		var m;
		var now = new Date;
		var sevenDaysAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);

		if (valids.indexOf(name) !== -1) {
			return 1;
		} else if (m = name.match(/^XAuctions([0-9]{4})([0-9]{2})([0-9]{2})$/)) {
			var date = new Date(Date.UTC(parseInt(m[1]), parseInt(m[2]) - 1, parseInt(m[3])));
			if (date < sevenDaysAgo) { return 2; }
			return 1;
		} else if (name.indexOf('Auctions') === 0) {
			return 2;
		}
	}

};

function safeExit(code) {
	setTimeout(function() {
		process.exit(code);
	}, 1000);
}


if (require.main === module) {
	module.exports().then(function() {
		safeExit();
	}).catch(function(err) {
		log.error({err: err}, 'createEnvironment error:', err.stack);
		safeExit(1);
	});
}
