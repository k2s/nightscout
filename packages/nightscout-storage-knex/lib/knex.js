const path = require('path')
const createKnex = require('knex')

const Find = require('./_find')
const {
  buildHints,
  createCollection
} = require('./_collection-hints')
const pino = require('pino')
const randomString = require('randomized-string')

// https://www.npmjs.com/package/rand-token
// https://www.npmjs.com/package/randomized-string
// https://www.npmjs.com/package/puid-js

let def
let log = console
let logSql = true

const mapOperator = {
  // see https://www.mongodb.com/docs/manual/reference/operator/query/
  $eq: '=',
  $gt: '>',
  $gte: '>=',
  $in: 'in',
  $lt: '<',
  $lte: '<=',
  $ne: '<>',
  $nin: 'not in'
}

function operator (colName, d, convertFn) {
  let [op, v] = typeof d === 'object' ? Object.entries(d)[0] : ['$eq', d]
  if (convertFn) {
    v = convertFn(v)
  }
  let o = mapOperator[op]
  if (!o) {
    log.error({
      collection: colName,
      op,
      d
    }, 'unknown condition operator')
    o = '='
  }
  return [colName, o, v]
}

class Collection {
  static factory (name, driver) {
    return new Collection(name, driver)
  }

  constructor (name, driver) {
    this.name = name
    this.driver = driver
    this.knex = driver.knex
  }

  _makeSql (collection, query) {
    // if (collection ==='food') debugger
    const d = def[collection]
    if (d) {
      const select = this.knex.from(collection)
      const sort = []
      if (query.filter) {
        for (let [k, v] of Object.entries(query.filter)) {
          if (k === d.tsField) {
            if (d.tsAsString) {
              select.where(...operator('ts', v, v => Date.parse(v)))
            } else {
              select.where(...operator('ts', v))
            }
          } else if (k === '_id') {
            select.where(...operator('_id', v.toString()))
          } else {
            select
              .jsonExtract('json', `$.${k}`, k)
            select.where(...operator(k, v))
          }
        }
      }

      if (query.sort) {
        if (typeof query.sort.keyOrList === 'object') {
          const sortFields = Object.keys(query.sort.keyOrList).join(',')
          if (sortFields !== d.tsField && (!d.index || !d.index.hasOwnProperty(sortFields))) {
            // TODO maybe remove later
            log.debug(`maybe not indexed sort search ${this.name}.${sortFields}`)
          }

          for (let [k, v] of Object.entries(query.sort.keyOrList)) {
            if (k === d.tsField) {
              sort.push({
                column: 'ts',
                order: (v < 0 ? 'desc' : 'asc')
              })
            } else {
              select.jsonExtract('json', '$.name', 'name')
              sort.push({
                column: 'name',
                order: v < 0 ? 'desc' : 'asc'
              })
            }
          }
          select.orderBy(sort)
        } else {
          throw new Error('xx')
        }
      }

      if (query.limit) {
        select.limit(query.limit)
      }

      return select
    }

    log.warn({
      collection,
      query
    }, 'unknown query')
    return false
  }

  _select (collection, query, cb) {
    const sql = this._makeSql(collection, query).select('json')
    sql
      .then(r => {
        logSql && log.trace({
          collection,
          sql: sql.toString(),
          r
        }, '_select')
        cb(null, r.map(d => this._unpack(collection, d.json)))
      })
      .catch(err => {
        log.error({
          collection,
          query,
          sql: sql.toString(),
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
    const sql = this._makeSql(this.name, { filter: selector })
    // TODO doesn't work, because Knex is not building filter with JSON_EXTRACT
    // TODO also update has to user json functions
    sql
      .update(update, ['_id'])
      .into(this.name)

    sql.then(res => {
      logSql && log.trace({
        collection: this.name,
        selector,
        update,
        options,
        sql: sql.toString(),
        res
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
          sql: sql.toString(),
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
      // options = undefined
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

    const sql = tsName
      ? this.knex
        .insert({
          '_id': id,
          'ts': d.tsAsString ? Date.parse(doc[tsName]) : doc[tsName],
          'json': this._pack(this.name, doc)
        })
        .into(this.name)
        .onConflict('_id')
        .merge(['ts', 'json'])
      : this.knex
        .insert({
          '_id': id,

          'json': this._pack(this.name, doc)
        })
        .into(this.name)
        .onConflict('_id')
        .merge(['json'])

    sql.then(() => {
      logSql && log.trace({
        collection: this.name,
        sql: sql.toString()
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
          sql: sql.toString(),
          err
        }, 'insertOne')
        callback(err)
      })
  }

  remove (selector, options, callback) {
    if (typeof options === 'function') {
      callback = options
      // options = undefined
    }

    const sql = this._makeSql(this.name, { filter: selector }).delete()
    sql
      .then(() => {
        logSql && log.trace({
          collection: this.name,
          sql: sql.toString()
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

class KnexStorage {
  constructor (env, knex, def = null) {
    this._env = env
    this.knex = knex
    this._def = def || buildHints(env)
    log.debug('KnexStorage constructed')
  }

  collection (name) {
    return Collection.factory(name, this)
  }

  ensureIndexes (collection, fields) {
    // all needed was created based on _collection-hints.js
  }

  get db () {
    return {
      stats: (options, cb) => {
        cb = typeof options === 'function' ? options : cb

        // TODO this needs to be done per knex driver

        // https://www.sqlite.org/dbstat.html
        // TODO there is better way to detect index and data objects
        this.knex.raw('SELECT SUM(CASE WHEN name like \'IDX_%\' THEN payload ELSE 0 END) as indexSize, SUM(CASE WHEN name not like \'IDX_%\' and name not like \'sqlite%\' THEN payload ELSE 0 END) as dataSize FROM dbstat where aggregate=true')
          .then(d => cb(null, d))
          .catch(cb)
        // const dataSize = fs.statSync(this._d.db.filename).size
      }
    }
  }
}

class BuildKnex {
  constructor (env, useThisLog = false) {
    this.env = env

    log = useThisLog
      ? useThisLog
      : pino({
        name: 'knex',
        level: 'trace',
        transport: {
          targets: [
            {
              target: 'pino/file',
              level: env.KNEX_LOGFILE_LEVEL || 'trace',
              options: {
                destination: path.resolve(process.cwd(), env.KNEX_LOGFILE || 'tmp/storage-knex.log'),
                append: false
              }
            },
            {
              target: 'pino-pretty',
              level: env.KNEX_CONSOLE_LEVEL || 'debug',
              options: {
                translateTime: true,
                // levelFirst: true,
                colorize: true
              }
            }
          ]
        }
      })
    log.debug('BuildKnex constructed')
  }

  get needsFallback () {
    return false
  }

  async _createKnex (opts) {
    const knex = createKnex({
      client: 'sqlite3',
      connection: {
        filename: opts.connection
      },
      useNullAsDefault: true,
      log: {
        warn: log.warn.bind(log),
        error: log.error.bind(log),
        deprecate: log.warn.bind(log),
        debug: log.debug.bind(log)
      }
    })

    // create tables and indexes
    def = buildHints(this.env)
    const a = []
    for (const [name, d] of Object.entries(def)) {
      a.push(createCollection(knex, name, d))
    }

    await Promise.all(a)

    return knex
  }

  init (cb) {
    const connection = this.env.KNEX_CONNECTION

    this._createKnex({ connection }).then(knex => {
      cb(null, new KnexStorage(this.env, knex, def))
    })
      .catch(err => {
        log.error(err, 'BuildCosmoDB.init')
        cb(err)
      })
  }
}

module.exports = BuildKnex
