'use strict';

const Fallback = require('./fallback')
const Proxy = require('./proxy')

module.exports = {
  default: Fallback,
  fallback: Fallback,
  proxy: Proxy
}
