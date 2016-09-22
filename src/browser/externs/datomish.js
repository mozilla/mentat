var Object = {};
Object.keys = function (object) {};
Object.__proto__ = {};
Object.hasOwnProperty = function () {};
var Array = {};
Array.length = 0;
Array.isArray = function () {};

var SqliteStatic = {};

/**
 * @param {Object} options
 * @return {Promise.<Sqlite>}
 */
SqliteStatic.openConnection = function (options) {}

var Sqlite = {}

/**
 * @param {string} sql
 * @param {Array} bindings
 * @return {Promise}
 */
Sqlite.execute = function (sql, bindings) {}

/**
 * @return {Promise}
 */
Sqlite.close = function() {}

var StorageRow = {};

/**
 * @param {string} index
 */
StorageRow.getResultByIndex = function (index) {}

/**
 * @param {string} name
 */
StorageRow.getResultByName = function (name) {}
