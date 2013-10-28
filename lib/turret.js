var format = require('util').format;

var Controller = require('./controller');
var Configuration = require('./configuration');

exports = module.exports = createTurret;

function createTurret() {
    return new Turret();
}

function Turret() {
    this.configurations = {};
}

Turret.prototype.configure = function (name, adapter, entity_parameters) {
    this.configurations[name] = new Configuration(adapter, entity_parameters);
    return this;
};

Turret.prototype.getConfiguration = function (name) {
    if (!this.configurations[name]) {
        throw Error(format('undefined controller configuration "%s".', name));
    }

    return this.configurations[name];
};

Turret.prototype.createController = function (name) {
    return new Controller(this, name);
};