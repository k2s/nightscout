'use strict';

const storages = require('..');
const assert = require('assert').strict;

assert.deepStrictEqual(Object.keys(storages), ['default', 'fallback']);
console.info("nightscout-storage-basic tests passed");
