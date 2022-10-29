'use strict';

const storages = require('..');
const assert = require('assert').strict;

assert.deepStrictEqual(Object.keys(storages), ['default', 'Fallback']);
console.info("nightscout-storage-basic tests passed");
