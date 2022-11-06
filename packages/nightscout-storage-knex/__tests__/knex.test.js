'use strict';

const storages = require('..');
const assert = require('assert').strict;

assert.deepStrictEqual(Object.keys(storages), ['default', 'cosmosdb']);
console.info("nightscout-storage-cosmosdb tests passed");
