var _ = require('lodash');
var async = require('async');
var format = require('util').format;

var Repository = require('./repository');

module.exports = TurretController;

function TurretController(turret, name) {
    this._turret = turret;
    this._name = name;

    this._persistences = {};
    this._removes = {};

    this._currentSession = null;
}

TurretController.prototype.getAdapter = function () {
    return this.getConfiguration().getAdapter();
};

TurretController.prototype.getRepository = function (name) {
    var parameter = this.getConfiguration().getEntityParameter(name);

    var repository;

    if (parameter.repository) {
        repository = newInstance(parameter.repository, [ this, name, parameter.options ]);
    } else {
        repository = Repository.create(this, name, parameter.options);
    }

    return repository;
};

TurretController.prototype.createEntity = function (name, values) {
    if (!values) values = {};

    var parameter = this.getConfiguration().getEntityParameter(name);
    var EntityFactor = parameter.entity;

    if (!EntityFactor) {
        return values;
    }

    var entity = newInstance(EntityFactor);

    return _.extend(entity, values);
};

TurretController.prototype.getConfiguration = function () {
    return this._turret.getConfiguration(this._name);
};


TurretController.prototype.session = function (callback) {
    if (!this._currentSession) {
        this.getAdapter().createSession(callback);
    } else {
        callback(null, this._currentSession);
    }
};

TurretController.prototype.releaseSession = function (session, callback) {
    if (!this._currentSession) {
        session.end(callback);
    } else {
        callback();
    }
};




TurretController.prototype.markPersist = function (name, repo) {
    this._persistences[name] = repo;
};

TurretController.prototype.markRemove = function (name, repo) {
    this._removes[name] = repo;
};

TurretController.prototype.flush = function (callback) {
    var self = this;

    async.series({
        persistence: function (next) {
            async.eachSeries(_.keys(self._persistences), function (name, next) {
                var repo = self._persistences[name];

                repo.applyPersistence(function (err) {
                    if (err) {
                        next(err);
                    } else {
                        delete self._persistences[name];
                        next();
                    }
                });
            }, next);
        },

        remove: function (next) {
            async.eachSeries(_.keys(self._removes), function (name, next) {
                var repo = self._removes[name];

                repo.applyRemove(function (err) {
                    if (err) {
                        next(err);
                    } else {
                        delete self._persistences[name];
                        next();
                    }
                });
            }, next);
        }
    }, callback);
};


TurretController.prototype.startSession = function (task, callback) {
    var self = this;
    var sessionController = new TurretController(this._turret, this._name);
    var adapter = sessionController.getAdapter();

    sessionController.session(function (err, session) {
        if (err) {
            callback(err);
            return;
        }

        session.begin(function (err) {
            if (err) {
                sessionController.releaseSession(session);
                callback(err);
                return;
            }

            sessionController._currentSession = session;

            task(sessionController, function (err) {
            //task(sessionController, function (err) {
                if (err) {
                    session.rollback(function () {
                        sessionController._currentSession = null;
                        sessionController.releaseSession(session);

                        callback(err);
                    });

                    return;
                }

                sessionController.flush(function (err) {
                    if (err) {
                        session.rollback(function () {
                            sessionController._currentSession = null;
                            sessionController.releaseSession(session);

                            callback(err);
                        });
                        return;
                    }

                    session.commit(function (err) {
                        sessionController._currentSession = null;
                        sessionController.releaseSession(session);

                        callback(err);
                    });
                });
            });
        });
    });
};





function newInstance (F, args) {
    if (typeof F === 'string') {
        F = require(F);
    }

    var o = Object.create(F.prototype);
    var r = F.apply(o, args);

    return (r != null && r instanceof Object) ? r : o;
}
