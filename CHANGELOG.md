* improve `update()` function in `sqlite@nightscout-storage-sqlite`  
* added `knex@nightscout-storage-knex` driver
  * JSON indexes are not abstracted
  * `update()` is not working correctly because of Knex limitations
* sqlite driver uses native `JSON` type instead of `TEXT` 
* drivers should accept initiated log object 
* added `proxy@nightscout-storage-basic` driver
* storage driver will receive process.env variables prefixed with `STORAGE_`  
