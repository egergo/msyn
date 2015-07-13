var express = require('express');

var log = require('./log');

var app = express();
app.use(log.requestLogger());
app.enable('trust proxy');
app.disable('x-powered-by');


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
	log.info({ port: port }, "listening on %d!", port);
});

module.exports = {
	app: app
};
