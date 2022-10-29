const path = require('path')
const fs = require('fs-extra')
const sqlite3 = require('sqlite3')
const { open } = require('sqlite')
const Find = require('./_find')
const { buildHints } = require('./_collection-hints')
const pino = require('pino')
const randomString = require('randomized-string')

// https://www.npmjs.com/package/rand-token
// https://www.npmjs.com/package/randomized-string
// https://www.npmjs.com/package/puid-js

let def
let log = console

const mapOperator = {
  $eq: '=',
  $gte: '>='
}

function operator (d, convertFn) {
  let [o, v] = typeof d === 'object' ? Object.entries(d)[0] : ['$eq', d]
  if (convertFn) {
    v = convertFn(v)
  }
  o = mapOperator[o]
  return o + (typeof v === 'string' ? `'${v}'` : v) // TODO string?
}

class Collection {
  static factory (name, driver) {
    return new Collection(name, driver)
  }

  constructor (name, driver) {
    this.name = name
    this.driver = driver
    this._d = driver._d
  }

  _makeSql (collection, query) {
    // if (collection ==='food') debugger
    const d = def[collection]
    if (d) {
      const where = []
      const sort = []
      const from = [collection]
      if (query.filter) {
        for (let [k, v] of Object.entries(query.filter)) {
          if (k === d.tsField) {
            if (d.tsAsString) {
              where.push('ts' + operator(v, v => Date.parse(v)))
            } else {
              where.push('ts' + operator(v))
            }
          } else if (k === '_id') {
            where.push('_id' + operator(v.toString()))
          } else {
            where.push(`JSON_EXTRACT(json, '$.${k}')` + operator(v))
          }
        }
      }
      // if

      /*
      SELECT profile
    FROM profile, json_each(profile.json)
   WHERE json_each.key = 'defaultProfile' and json_each.value = "Default"

       */

      if (query.sort) {
        if (typeof query.sort.keyOrList === 'object') {
          for (let [k, v] of Object.entries(query.sort.keyOrList)) {
            if (k === d.tsField) {
              sort.push('ts' + (v < 0 ? ' DESC' : ''))
            } else {
              // if (collection === this.driver.env.profile_collection) {
              //   if (k === 'startDate') {
              //     from.push(`json_each(${collection}.json)`)
              //     where.push(`key='${k}'`)
              //     sort.push('json_each.atom' + (v < 0 ? ' DESC' : ''))
              //   } else {
              //     sort.push(k + (v < 0 ? ' DESC' : ''))
              //   }
              // } else {
              //   log.debug(`maybe not indexed sort property ${this.name}.${k}`)
              //   sort.push(`JSON_EXTRACT(json, '$.${k}')` + (v < 0 ? ' DESC' : ''))
              // }
              // if (collection === this.driver.env.profile_collection) debugger
              log.debug(`maybe not indexed sort property ${this.name}.${k}`)
              sort.push(`JSON_EXTRACT(json, '$.${k}')` + (v < 0 ? ' DESC' : ''))
              // SELECT profile.* FROM profile, json_each(profile.json) WHERE key='startDate' ORDER BY json_each.atom DESC LIMIT 0, 1
            }
          }
        } else {
          throw new Error('xx')
        }
      }

      const parts = []
      if (where.length) {
        parts.push(' WHERE ' + where.join(' AND '))
      }
      if (sort.length) {
        parts.push('ORDER BY ' + sort.join(','))
      }
      if (query.limit) {
        parts.push('LIMIT 0, ' + query.limit)
      }

      return `FROM ${from.join(',')} ${parts.join(' ')}`
    }

    log.warn({
      collection,
      query
    }, 'unknown query')
    return false
  }

  _select (collection, query, cb) {
    let logSql = true
    let sql = this._makeSql(collection, query)
    if (!sql) {
      sql = `SELECT ${collection}.json FROM ${collection} WHERE 1=0`
      logSql = false
    } else {
      sql = `SELECT ${collection}.json ` + sql
    }

    this._d.all(sql)
      .then(r => {
        logSql && log.trace({
          collection,
          sql,
          r
        }, '_select')
        cb(null, r.map(d => this._unpack(collection, d.json)))
      })
      .catch(err => {
        logSql && log.error({
          collection,
          sql,
          err
        }, '_select')
        cb(null, [])
      })
  }

  _pack (collection, data) {
    return JSON.stringify(data)
  }

  _unpack (collection, data) {
    return JSON.parse(data)
  }

  find (query, options, callback) {
    if (!callback) {
      return new Find(this.name, (err, res, query, cb) => {
        if (err) {
          return cb(err)
        }
        this._select(this.name, query, cb)
      }).find(query, options)
    }

    throw new Error('sss')
  }

  /*
    findOne (query, options, callback) {
      this.base.findOne(query, options, (err, entry) => {
        log.info({
          callback: !!callback,
          query,
          options,
          err,
          entry,
          name: this.base.collectionName
        }, 'findOne')
      })

      return this.base.findOne(query, options, callback)
    }

    createIndex (fieldOrSpec, options, callback) {
      log.info({
        callback: !!callback,
        fieldOrSpec,
        options,
        name: this.base.collectionName
      }, 'createIndex')
      return this.base.createIndex(fieldOrSpec, options, callback)
    }
  */
  save (doc, options, callback) {
    this.insertOne(doc, options, (err, doc) => {
      doc = doc.ops[0]
      callback(err, doc)
    })
  }

  update (selector, update, options, callback) {
    // { result/*, connection, message*/ }
    const d = def[this.name]

    const id = update._id ? update._id.toString() : /*ObjectID*/(randomString.generate({
      charset: 'hex',
      lowerCaseOnly: true,
      length: 24
    }))
    update._id = id
    this._d.run(`INSERT INTO ${this.name} (_id, ts, json) VALUES (:id, :ts, :json) ON CONFLICT DO UPDATE SET ts=:ts, json=:json`, {
      ':id': id,
      ':ts': d.tsAsString ? Date.parse(selector[d.tsField]) : selector[d.tsField],
      ':json': this._pack(this.name, update)
    })
      .then(() => {
        log.trace({
          collection: this.name,
          selector,
          update,
          options
        }, 'update')

        const r = {
          result:
            {
              'n': 1,
              'nModified': 1, // TODO ?
              'upserted': [
                {
                  'index': 0,
                  '_id': id
                }
              ],
              'ok': 1
            }
        }
        callback(null, r)
      })
      .catch(err => {
        log.error({
          collection: this.name,
          selector,
          update,
          options,
          err
        }, 'update')
        callback(err)
      })
  }

  insert (doc, options, callback) {
    return this.insertOne(doc, options, callback)
  }

  insertOne (doc, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = undefined
    }

    // TODO use collection class
    const d = def[this.name]
    let tsName = d ? d.tsField : 'created_at'

    const id = doc._id ? doc._id.toString() : /*ObjectID*/(randomString.generate({
      charset: 'hex',
      lowerCaseOnly: true,
      length: 24
    }))

    doc = {
      ...doc,
      _id: id
    }
    this._d.run(`INSERT INTO ${this.name}(_id, ts, json) VALUES (:id, :ts, :json) ON CONFLICT(_id) DO UPDATE SET ts=:ts, json=:json`, {
      ':id': id,
      ':ts': d.tsAsString ? Date.parse(doc[tsName]) : doc[tsName],
      ':json': this._pack(this.name, doc)
    })
      .then(() => {
        log.trace({
          collection: this.name,
          doc
        }, 'insertOne')

        const r = {
          result: {
            ok: 1,
            n: 1
          },
          ops: [
            doc
          ],
          insertedCount: 1,
          insertedIds: [{
            _id: id
          }]
        }
        callback(null, r)
      })
      .catch(err => {
        log.error({
          collection: this.name,
          doc,
          err
        }, 'insertOne')
        callback(err)
      })
  }

  remove (selector, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = undefined
    }

    const sql = this._makeSql(this.name, { filter: selector })
    this._d.run('DELETE ' + sql)
      .then(() => {
        log.trace({
          collection: this.name,
          selector
        }, 'remove')
        callback(null)
      })
      .catch(err => {
        log.error({
          collection: this.name,
          selector,
          err
        }, 'remove')
        callback(err)
      })
  }
}

class SqliteStorage {
  constructor (env, db) {
    this.env = env
    this._d = db
    def = buildHints(env)
    log.debug('SqliteStorage constructed')
  }

  collection (name) {
    return Collection.factory(name, this)
  }

  ensureIndexes (collection, fields) {
    if (collection === def.entries_collection) {
      // this table was already created
      return
    }

    collection._d.exec(
      `CREATE TABLE IF NOT EXISTS '${collection.name}' 
(_id NOT NULL, ts INTEGER NOT NULL UNIQUE, json TEXT, PRIMARY KEY('_id')) ; 
CREATE UNIQUE INDEX IF NOT EXISTS 'IDX_${collection.name}' ON '${collection.name}'(ts);
`).then(r => { //
      console.log(collection.name, 'created') // fields, r
    }).catch(ex => {
      console.error(ex)
    })
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
  constructor (env) {
    this.env = env

    log = pino({
      name: 'sqlite',
      level: 'trace',
      transport: {
        targets: [
          {
            target: 'pino/file',
            level: process.env.STORAGE_SQLITE_LOGFILE_LEVEL || 'trace',
            options: {
              destination: path.resolve(process.cwd(), process.env.STORAGE_SQLITE_LOGFILE || 'tmp/storage-sqlite.log'),
              append: false
            }
          },
          {
            target: 'pino-pretty',
            level: process.env.STORAGE_SQLITE_CONSOLE_LEVEL || 'debug',
            options: {
              translateTime: true,
              // levelFirst: true,
              colorize: true
            }
          }
        ]
      }
    })
    log.debug('BuildSqlite constructed')
  }

  get needsFallback () {
    return false
  }

  init (cb) {
    // TODO this.env should contain configuration of the storage
    const filename = path.resolve(process.cwd(), process.env.STORAGE_SQLITE_DB || 'tmp/database.db')
    log.info('opening Sqlite3 database %s', filename)
    open({
      filename,
      driver: sqlite3.Database
    })
      .then(db => {
        // TODO should `teardown` event in NS be used instead
        process.on('SIGINT', function () {
          log.info('received SIGINT and closing DB')
          db.close().then(() => {
            log.info('DB closed')
            // process.exit(err ? 1 : 0)
          })
        })

        const collection = this.env.entries_collection
        return db.exec(`
CREATE TABLE IF NOT EXISTS 'auth_roles' (_id NOT NULL, ts INTEGER NOT NULL UNIQUE, json TEXT, PRIMARY KEY('_id')); 
CREATE UNIQUE INDEX IF NOT EXISTS 'IDX_auth_roles' ON 'auth_roles'(ts);

CREATE TABLE IF NOT EXISTS 'auth_subjects' (_id NOT NULL, ts INTEGER NOT NULL UNIQUE, json TEXT, PRIMARY KEY('_id')); 
CREATE UNIQUE INDEX IF NOT EXISTS 'IDX_auth_subjects' ON 'auth_subjects'(ts);

CREATE TABLE IF NOT EXISTS '${collection}' (_id NOT NULL, ts INTEGER NOT NULL UNIQUE, json TEXT, PRIMARY KEY('_id')); 
CREATE UNIQUE INDEX IF NOT EXISTS 'IDX_${collection}_ts_type' ON '${collection}'(ts, JSON_EXTRACT(json, '$.type'));
CREATE UNIQUE INDEX IF NOT EXISTS 'IDX_${collection}_date' ON '${collection}'(JSON_EXTRACT(json, '$.date'));
`).then(r => { //
          cb(null, new SqliteStorage(this.env, db))
        })
      })
      .catch(err => {
        log.error(err, 'BuildSqlite.init')
        cb(err)
      })

  }
}

module.exports = BuildSqlite
