var format = require('util').format;

module.exports = TurretConfiguration;

function TurretConfiguration(adapter, entities) {
    this.adapter = adapter;
    this.entities = entities;
}

TurretConfiguration.prototype.getAdapter = function () {
    return this.adapter;
};

TurretConfiguration.prototype.setAdapter = function (adapter) {
    this.adapter = adapter;
    return this;
};

TurretConfiguration.prototype.getEntityParameter = function (name) {
    if (!this.entities[name]) {
        throw Error(format('undefined entity "%s".', name));
    }

    return this.entities[name];
};

TurretConfiguration.prototype.setEntityParameter = function (name, parameter) {
    this.entities[name] = parameter;
    return this;
};