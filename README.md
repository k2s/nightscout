# Nightscout Web Monitor with alternative storage

Sqlite3 supports JSON documents and is also able to apply indexes on structured data. For more information see https://www.sqlite.org/json1.html.

I created [simple changes](https://github.com/k2s/cgm-remote-monitor/tree/wip/custom-storage) in Nightscout to support loading of custom storage drivers from NodeJS modules. 

The drivers have to follow the exiting MongoDB driver. I created multiple development drivers before I created working SQLite driver which the community could benefit the most.

Now I moved my code to this monorepo and created Docker image for quick testing and to open discussion with Nightscout community.

**STATUS:** Highly experimental. I was running it for 2 weeks with minor issues with xDrip, without pump.
All issues I experienced are related to uniqueness of data that I am still trying to understand.

## Features (existing and planned)

TODO

## Quick start with Docker
 
I created Docker image that could be used to quickly evaluate this experimental functionality.

```
# start the Docker instance with Sqlite DB stored on local disk  
docker run --name ns --rm -v $PWD/ns-data:/mnt bigm/nightscout-experimental-sqlite:latest

# to see trace log messages
tail -f  $PWD/ns-data/storage-sqlite.log

# start ngrok tunnel to access the instance from Internet (you need their free auth token)
docker exec -ti ns ngrok http 1337 --authtoken $NGAUTH
```

**STATUS:** Highly experimental. This Docker image was not tested in production and configuration will probably be changed in the future.

## Import from MongoDB dump

* create backup of your MongoDB database with `mongodump` tool
* place the database to folder `./mydbdump` (this folder has to contain bson files)
* run following command

```bash
docker run --name ns --rm -v $PWD/ns-data:/mnt -v $PWD/mydbdump:/dump bigm/nightscout-experimental-sqlite:latest \
  /opt/packages/nightscout-storage-sqlite/bin/sqlite-storage.mjs import /mnt/nightscout.sqlite /dump/
```

* it will create `./ns-data/nightscout.sqlite` database
* process and errors are logged into `./ns-data/nightscout.sqlite.log`

## Install

```
git clone --recurse-submodules https://github.com/k2s/nightscout.git
cd nightscout
npx lerna bootstrap

# to build Docker image
docker build -t nightscout-sqlite .
```

## Added configuration params for Nightscout

This will be changed. 
I am not sure now what would be the best way to configure storage driver and its options.

- `STORAGE_CLASS`: activates custom storage engine
  - supported formats to reference driver:
    - `@nightscout-storage-sqlite`: NodeJS module that returns driver as `default` 
    - `fallback@nightscout-storage-basic`: NodeJS module that returns multiple named drivers 
    -`../../../../packages/nightscout-storage-sqlite`: path to NodeJS module on dist 
- `STORAGE_CLASS=@nightscout-storage-sqlite`: uses SQLite3 database 
  - `STORAGE_SQLITE_DB`: path to database file, absolute or relative to `process.cwd()` (default: `tmp/database.db`)
  - `STORAGE_SQLITE_LOGFILE`: path to Pino log file (default: `tmp/storage-sqlite.log`)
  - `STORAGE_SQLITE_LOGFILE_LEVEL`: log level for file output (see Pino) (default: `trace`)  
  - `STORAGE_SQLITE_CONSOLE_LEVEL`: log level for console output (see Pino) (default: `debug`)
- `STORAGE_CLASS=fallack@nightscout-storage-basic`: it uses the original MongoDB driver, easy proof that it works and usable with `multi@nightscout-storage-basic`

Currently, not committed storage drivers (mainly used for development):

- `STORAGE_CLASS=proxy@nightscout-storage-basic`: main purpose is to catch all calls to original MongoDB driver for analysis
- `STORAGE_CLASS=multi@nightscout-storage-basic`: distribute all writes to multiple storage drivers, reads are returned from first driver and other responses are compared to be the same with the first 

## TODO

- [ ] what would be correct way to configure storages with `process.env` ? (`STORAGE_CLASS`, `STORAGE_SQLITE_DB`)
- [ ] currently CUSTOMCONNSTR_mongo needs to be set to anything to bootstrap storage engine in Nightscout
- [ ] Nightscout will start also if there is error in storage, this needs change in NS and happens also with original storage
- [ ] import tool has problem with my MongoDB data because of duplicities which are not clear for me why they exists (maybe bugs in NS?)
- [ ] create export tool to move data from sqlite to MongoDB
- [ ] review `custom-storage.js` if it is optimal way (for me it seams to be simple and providing freedom for drivers)
- [ ] sqlite driver is very early prototype
- [ ] what other drivers could be of benefit (eg. rqlite)
- [ ] would cache driver help with load that will not use DB for repeating queries without write influencing this tables
