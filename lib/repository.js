var _ = require('lodash');
var async = require('async');
var util = require('util');
var format = util.format;

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



Repository.prototype.createQueryBuilder = function () {
    return this._controller.getAdapter().createQueryBuilder(this._options);
};





Repository.prototype.query = function (params, callback) {
    this._find(params, callback);
};


Repository.prototype.find = function (id, callback) {
    var self = this;

    var params = self.createQueryBuilder().
        select().
        where({ id: id }).
        limit(1).
        createParameter();

    if (!params) {
        callback(Error(format('invalid entity parameter "%s".', self._entityName)));
        return;
    }


    self._findOne(params, callback);
};

Repository.prototype.findOneBy = function (criteria, callback) {
    var self = this;

    var params = self.createQueryBuilder().
        select().
        where(criteria).
        limit(1).
        createParameter();

    if (!params) {
        callback(Error(format('invalid entity parameter "%s".', self._entityName)));
        return;
    }

    this._findOne(params, callback);
};

Repository.prototype.findAll = function (callback) {
    var self = this;

    var params = self.createQueryBuilder().
        select().
        createParameter();

    if (!params) {
        callback(Error(format('invalid entity parameter "%s".', self._entityName)));
        return;
    }

    self._find(params, callback);
};

Repository.prototype.findBy = function (criteria, orderBy, limit, offset, callback) {
    callback = arguments[ arguments.length - 1 ];

    if (!_.isObject(criteria) || _.isFunction(criteria)) criteria = {};
    if (!_.isObject(orderBy)  || _.isFunction(orderBy))  orderBy = null;
    if (!_.isNumber(limit)    || _.isFunction(limit))    limit = null;
    if (!_.isNumber(offset)   || _.isFunction(offset))   offset = null;

    var self = this;

    var params = self.createQueryBuilder().
        select().
        where(criteria).
        orderBy(orderBy).
        offset(offset).
        limit(limit).
        createParameter();

    if (!params) {
        callback(Error(format('invalid entity parameter "%s".', self._entityName)));
        return;
    }

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

    var checker = function () {
        return self._persistences.length > 0;
    };

    var task = function (next) {
        var entity = self._persistences.shift();
        var params = self.createQueryBuilder().persistence().createParameter(entity);

        if (!params) {
            next(Error(format('invalid entity parameter "%s".', self._entityName)));
            return;
        }

        self._controller.getSession(function (err, session) {
            if (err) {
                next(err);
            } else {
                session.execute(params, function (err) {
                    self._controller.releaseSession(session, function () {
                        next(err);
                    });
                });
            }
        });
    };

    async.whilst(checker, task, callback);
};

Repository.prototype.applyRemove = function (callback) {
    var self = this;

    var checker = function () {
        return self._removes.length > 0;
    };

    var task = function (next) {
        var entity = self._removes.shift();
        var params = self.createQueryBuilder().delete().createParameter(entity);

        if (!params) {
            next(Error(format('invalid entity parameter "%s".', self._entityName)));
            return;
        }

        self._controller.getSession(function (err, session) {
            if (err) {
                next(err);
            } else {
                session.execute(params, function (err) {
                    self._controller.releaseSession(session, function () {
                        next(err);
                    });
                });
            }
        });
    };

    async.whilst(checker, task, callback);
};



Repository.prototype._findOne = function (params, callback) {
    var self = this;

    self._controller.getSession(function (err, session) {
        if (err) {
            callback(err);
            return;
        }

        session.find(params, function (err, rows) {
            self._controller.releaseSession(session, function () {
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

    self._controller.getSession(function (err, session) {
        if (err) {
            callback(err);
            return;
        }

        session.find(params, function (err, rows) {
            self._controller.releaseSession(session, function () {
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
