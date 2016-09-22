var sqlite = {};

sqlite.DB = {};

/**
 * @return {Promise}
 */
sqlite.DB.open = function (path, options) {};

var DBVal = {};
DBVal.run = function (sql, bindings) {};
DBVal.close = function () {};
DBVal.each = function (sql, bindings, cb) {};
