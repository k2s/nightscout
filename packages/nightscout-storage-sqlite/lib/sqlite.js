const path = require('path')
const sqlite3 = require('sqlite3')
const { open } = require('sqlite')
const {
  buildHints,
  createCollection
} = require('./_collection-hints')
const pino = require('pino')
const Collection = require('./collection')

// https://www.npmjs.com/package/rand-token
// https://www.npmjs.com/package/randomized-string
// https://www.npmjs.com/package/puid-js

class SqliteStorage {
  constructor (env, db, def = null, log = console) {
    this.log = log
    this._env = env
    this._d = db
    this._def = def || buildHints(env)
    log.debug('SqliteStorage constructed')
  }

  collection (name) {
    return Collection.factory(name, this, this._def[name])
  }

  ensureIndexes (collection, fields) {
    // all needed was created based on _collection-hints.js
  }

  get db () {
    return {
      stats: (options, cb) => {
        cb = typeof options === 'function' ? options : cb

        // https://www.sqlite.org/dbstat.html
        // TODO there is better way to detect index and data objects
        this._d.get('SELECT SUM(CASE WHEN name like \'IDX_%\' THEN payload ELSE 0 END) as indexSize, SUM(CASE WHEN name not like \'IDX_%\' and name not like \'sqlite%\' THEN payload ELSE 0 END) as dataSize FROM dbstat where aggregate=true')
          .then(d => cb(null, d))
          .catch(cb)
        // const dataSize = fs.statSync(this._d.db.filename).size
      }
    }
    // if (!this._db) {
    //   this._db = new ProxyDb(this.base.db) // new Proxy(this.base.db, handlerDb)
    // }
    // return this._db
  }
}

class BuildSqlite {
  constructor (env, useThisLog = false) {
    this.env = env

    this.log = useThisLog
      ? useThisLog
      : pino({
        name: 'sqlite',
        level: 'trace',
        transport: {
          targets: [
            {
              target: 'pino/file',
              level: env.SQLITE_LOGFILE_LEVEL || 'trace',
              options: {
                destination: path.resolve(process.cwd(), env.SQLITE_LOGFILE || 'tmp/storage-sqlite.log'),
                append: false
              }
            },
            {
              target: 'pino-pretty',
              level: env.SQLITE_CONSOLE_LEVEL || 'debug',
              options: {
                translateTime: true,
                // levelFirst: true,
                colorize: true
              }
            }
          ]
        }
      })
    this.log.debug('BuildSqlite constructed')
  }

  get needsFallback () {
    return false
  }

  init (cb) {
    // TODO this.env should contain configuration of the storage
    const filename = path.resolve(process.cwd(), this.env.SQLITE_DB || 'tmp/database.db')
    this.log.info('opening Sqlite3 database %s', filename)
    open({
      filename,
      driver: sqlite3.Database
    })
      .then(db => {
        // TODO should `teardown` event in NS be used instead
        const log = this.log
        process.on('SIGINT', function () {
          log.info('received SIGINT and closing DB')
          db.close().then(() => {
            log.info('DB closed')
            // process.exit(err ? 1 : 0)
          })
        })

        db.on('trace', data => {
          // TODO remove or make configurable or only in development?
          this.log.trace(data)
        })

        // create tables and indexes
        const def = buildHints(this.env)
        const a = []
        for (const [name, d] of Object.entries(def)) {
          a.push(createCollection(db, name, d))
        }

        Promise.all(a).then(() => { //
          cb(null, new SqliteStorage(this.env, db, def, this.log))
        })
      })
      .catch(err => {
        this.log.error(err, 'BuildSqlite.init')
        cb(err)
      })

  }
}

module.exports = BuildSqlite
