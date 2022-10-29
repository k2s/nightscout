'use strict';

const storages = require('..');
const assert = require('assert').strict;

assert.deepStrictEqual(Object.keys(storages), ['default', 'sqlite']);
console.info("nightscout-storage-sqlite tests passed");
