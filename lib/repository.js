var _ = require('lodash');
var async = require('async');
var util = require('util');

module.exports = exports = Repository;

exports.define = function (prototype) {
    function DefinedRepository (controller, entityName, options) {
        Repository.call(this, controller, entityName, options);
    }

    util.inherits(DefinedRepository, Repository);

    if (typeof prototype === 'object') {
        _.extend(DefinedRepository.prototype, prototype);
    }

    return DefinedRepository;
};

exports.create = function (controller, entityName, options) {
    return new Repository(controller, entityName, options);
};




function Repository (controller, entityName, options) {
    this._controller = controller;
    this._entityName = entityName;
    this._options = options || {};

    this._persistences = [];
    this._removes = [];
}


Repository.prototype.setOptions = function (options) {
    this._options = options;
    return this;
};



Repository.prototype.getEntityName = function () {
    return this._entityName;
};

Repository.prototype.getController = function () {
    return this._controller;
};

Repository.prototype.getOptions = function () {
    return this._options;
};

Repository.prototype.getAdapter = function () {
    return this._controller.getAdapter();
};



Repository.prototype.session = function (callback) {
    this._controller.session(callback);
};

Repository.prototype.releaseSession = function (session, callback) {
    this._controller.releaseSession(session, callback);
};


Repository.prototype.find = function (id, callback) {
    var self = this;
    var params = self.getAdapter().createSelectParameter(self._options, { id: id } );

    self._findOne(params, callback);
};

Repository.prototype.findOneBy = function (criteria, callback) {
    var self = this;
    var params = self.getAdapter().createSelectParameter(self._options, criteria, null, 1, 0 );

    this._findOne(params, callback);
};

Repository.prototype.findAll = function (callback) {
    var self = this;
    var params = self.getAdapter().createSelectParameter(self._options, {} );

    self._find(params, callback);
};

Repository.prototype.findBy = function (criteria, orderBy, limit, offset, callback) {
    callback = arguments[ arguments.length - 1 ];

    if (!_.isObject(criteria) || _.isFunction(criteria)) criteria = {};
    if (!_.isObject(orderBy)  || _.isFunction(orderBy))  orderBy = {};
    if (!_.isNumber(limit)    || _.isFunction(limit))    limit = null;
    if (!_.isNumber(offset)   || _.isFunction(offset))   offset = null;

    var self = this;
    var params = self.getAdapter().createSelectParameter(self._options, criteria, orderBy, limit, offset);

    self._find(params, callback);
};

Repository.prototype.add = function (entity) {
    this._persistences.push(entity);
    this._controller.markPersist(this._entityName, this);
};

Repository.prototype.remove = function (entity) {
    this._removes.push(entity);
    this._controller.markRemove(this._entityName, this);
};


Repository.prototype.applyPersistence = function (callback) {
    var self = this;

    var createChecker = function () {
        return function () {
            return self._persistences.length > 0;
        };
    };

    var createTask = function () {
        return function (next) {
            var entity = self._persistences.shift();
            var params = self.getAdapter().createPersistenceParameter(self._options, entity);

            self.session(function (err, session) {
                if (err) {
                    next(err);
                } else {
                    session.execute(params, function (err) {
                        self.releaseSession(session, function () {
                            next(err);
                        });
                    });
                }
            });
        };
    };

    self._applyEntities(createChecker, createTask, callback);
};

Repository.prototype.applyRemove = function (callback) {
    var self = this;

    var createChecker = function () {
        return function () {
            return self._removes.length > 0;
        };
    };

    var createTask = function () {
        return function (next) {
            var entity = self._removes.shift();
            var params = self.getAdapter().createRemoveParameter(self._options, entity);

            self.session(function (err, session) {
                if (err) {
                    next(err);
                } else {
                    session.execute(params, function (err) {
                        self.releaseSession(session, function () {
                            next(err);
                        });
                    });
                }
            });
        };
    };

    self._applyEntities(createChecker, createTask, callback);
};



Repository.prototype._findOne = function (params, callback) {
    var self = this;

    self.session(function (err, session) {
        if (err) {
            callback(err);
            return;
        }

        session.find(params, function (err, rows) {
            self.releaseSession(session, function () {
                if (err) {
                    callback(err);
                    return;
                }

                if (rows && rows[0]) {
                    callback(null, self._controller.createEntity(self._entityName, rows[0]));
                } else {
                    callback();
                }
            });
        });
    });
};

Repository.prototype._find = function (params, callback) {
    var self = this;

    self.session(function (err, session) {
        if (err) {
            callback(err);
            return;
        }

        session.find(params, function (err, rows) {
            self.releaseSession(session, function () {
                if (err) {
                    callback(err);
                    return;
                }

                if (rows && rows.length) {
                    async.map(rows, function (row, next) {
                        next(null, self._controller.createEntity(self._entityName, row));
                    }, callback);
                } else {
                    callback(null, []);
                }
            });
        });
    });
};

Repository.prototype._applyEntities = function (createChecker, createTask, callback) {
    var self = this;
    var checker = createChecker();
    var task    = createTask();

    async.whilst(checker, task, callback);
};
