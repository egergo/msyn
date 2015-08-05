var bunyan = require('bunyan');
var bunyanLogger = require('express-bunyan-logger');
var uuid = require('node-uuid');

var streams = [{
	level: 'debug',
	stream: process.stdout
}];

if (process.env.LOG_LE_TOKEN) {
	var logentriesStream = require('bunyan-logentries').createStream({
		token: process.env.LOG_LE_TOKEN,
		timestamp: false,
		secure: true,
		withStack: true
	}, {
		transform: function(logRecord) {
			delete logRecord.v;
			return logRecord
		}
	});
	streams.push({
		level: 'warn',
		stream: logentriesStream,
		type: 'raw'
	});
}

var log = bunyan.createLogger({
	name: 'app',
	level: 'debug',
	streams: streams,
	serializers: {
		err: bunyan.stdSerializers.err
	}
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
		excludes: ['body', 'short-body', 'http-version', 'response-hrtime', 'req-headers', 'res-headers', 'req', 'res', 'referer', 'incoming', 'user-agent'],
		streams: streams,
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
		streams: streams,
		immediate: true,
		genReqId: defaultGenerateRequestId
	});
};

module.exports = log;
