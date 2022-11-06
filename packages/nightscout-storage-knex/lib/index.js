'use strict';

const Knex = require('./knex')

module.exports = {
  default: Knex,
  sqlite: Knex
}
