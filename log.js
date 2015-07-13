var config = require('./config');
var bunyan = require('bunyan');
var bunyanLogger = require('express-bunyan-logger');
var uuid = require('node-uuid');

var log = bunyan.createLogger({
	name: 'app',
	level: config.get('log.level')
});

function defaultGenerateRequestId(req) {
	if (!req.id) {
		req.id = uuid.v4();
	}
	return req.id;
}

/**
 * Returns a request logger middleware.
 */
log.requestLogger = function() {
	return bunyanLogger({
		name: 'request',
		parseUA: false,
		format: ':remote-address :method :url :status-code :response-time ms',
		excludes: config.get('log.verbose') ? [] : ['body', 'short-body', 'http-version', 'response-hrtime', 'req-headers', 'res-headers', 'req', 'res', 'referer', 'incoming'],
		stream: process.stdout,
		level: config.get('log.level'),
		genReqId: defaultGenerateRequestId
	});
};

/**
 * Returns an error logger middleware.
 */
log.errorLogger = function() {
	return bunyanLogger.errorLogger({
		name: 'error',
		parseUA: true,
		format: ':remote-address :method :url :status-code :response-time ms :err[message]',
		excludes: ['short-body', 'incoming', 'response-hrtime'],
		stream: process.stdout,
		immediate: true,
		level: config.get('log.level'),
		genReqId: defaultGenerateRequestId
	});
};

module.exports = log;
