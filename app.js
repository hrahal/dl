var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var routes = require('./routes/index');
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.get('/api/all/stats', routes.all_stats);
app.get('/api/:airport/stats', routes.airport_stats);
app.get('/api/:airport/reviews', routes.airport_reviews);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

/* 
* error handler
* no stacktraces leaked to user*/

app.use(function(err, req, res, next) {
  res.status(err.status || 500)
      .json({
          message: err.message,
          success: false
      });
});


module.exports = app;
