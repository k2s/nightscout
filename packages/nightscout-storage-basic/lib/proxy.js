'use strict'
const pino = require('pino')
const path = require('path')

let log = console

const collectProps = {}
let saveTimer = false

const handleItem = {
  get (target, prop) {
    if (typeof target[prop] !== 'function' && ['__col'].indexOf(prop) === -1) {
      collectProps[target.__col] = collectProps[target.__col] || {}
      if (collectProps[target.__col].hasOwnProperty(prop)) {
        collectProps[target.__col][prop]++
      } else {
        collectProps[target.__col][prop] = 1
        if (saveTimer) {
          clearTimeout(saveTimer)
        }
        saveTimer = setTimeout(() => {
          log.info({ collectProps }, 'collectProps')
        }, 15000)
      }
    }
    return Reflect.get(...arguments)
  }
}

// noinspection JSUnusedLocalSymbols
/**
 * Used only to detect not already proxied calls
 * @type {{set(*, *, *): boolean, get(*, *): (any)}}
 */
const handlerDb = {
  get (target, prop) {
    if (['find', 'createIndex', 'save', 's', 'readPreference', 'collectionName', 'readConcern', 'writeConcern'].indexOf(prop) !== -1) {
      return Reflect.get(...arguments)
    }

    log.warn('get', prop, typeof target[prop])
    return Reflect.get(...arguments)
  },
  set (obj, prop, value) {
    log.warn('set', prop)
    obj[prop] = value
    return true
  }
}

class ProxyFind {
  constructor (base, cb) {
    this.cb = cb
    this.cur = base
    this.__name = base.collectionName
  }

  find (query, options, callback) {
    this.query = { filter: query }
    this.cur = this.cur.find(query, options, callback)
    return this
  }

  project (value) {
    this.query.project = value
    this.cur = this.cur.project(value)
    return this
  }

  sort (keyOrList, direction) {
    this.query.sort = {
      keyOrList,
      direction
    }
    this.cur = this.cur.sort(keyOrList, direction)
    return this
  }

  limit (value) {
    this.query.limit = value
    this.cur = this.cur.limit(value)
    return this
  }

  toArray (callback) {
    return this.cur.toArray((err, items) => {
      this.cb(err, items.map(o => new Proxy({
        ...o,
        __col: this.__name
      }, handleItem)), this.query, callback)
    })
  }

}

class ProxyCollection {
  constructor (collection) {
    this.base = collection
  }

  find (query, options, callback) {
    if (!callback) {
      return new ProxyFind(this.base, (err, res, query, cb) => {
        log.info({
          query,
          options,
          err,
          res,
          name: this.base.collectionName
        }, 'find')
        cb(err, res)
      }).find(query, options)
    }

    // this.base.find(query, options).toArray((err, res) => {
    //   log.info({
    //     callback: !!callback,
    //     query,
    //     options,
    //     err,
    //     res,
    //     name: this.base.collectionName
    //   }, 'find')
    // })
    log.error('find with callback')
    return this.base.find(query, options, callback)
  }

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

  save (doc, options, callback) {
    log.info({
      callback: !!callback,
      doc,
      options,
      name: this.base.collectionName
    }, 'save')
    return this.base.save(doc, options, callback)
  }

  update (selector, update, options, callback) {
    log.info({
      callback: !!callback,
      selector,
      update,
      options,
      name: this.base.collectionName
    }, 'update')
    return this.base.update(selector, update, options, callback)
  }

  insert (doc, options, callback) {
    log.info({
      callback: !!callback,
      doc,
      options,
      name: this.base.collectionName
    }, 'insert')
    return this.base.insert(doc, options, callback)
  }

  insertOne (doc, options, callback) {
    log.info({
      callback: !!callback,
      doc,
      options,
      name: this.base.collectionName
    }, 'insertOne')
    return this.base.insertOne(doc, options, callback)
  }

  remove (selector, options, callback) {
    log.info({
      callback: !!callback,
      selector,
      options,
      name: this.base.collectionName
    }, 'remove')
    return this.base.remove(selector, options, callback)
  }
}

class ProxyDb {
  constructor (db) {
    this.base = db
  }

  stats (options, cb) {
    log.info({
      options,
      name: this.base.collectionName
    }, 'stats')
    return this.base.stats(options, cb)
  }

  // get databaseName () {
  //   return this.base.databaseName
  // }
}

class ProxyStorage {
  constructor (mongoStorage) {
    this.base = mongoStorage
  }

  collection (name) {
    return new ProxyCollection(this.base.collection(name)) // new Proxy(this.base.collection(name), handlerDb)
  }

  ensureIndexes (collection, fields) {
    return this.base.ensureIndexes(collection, fields)
  }

  get db () {
    if (!this._db) {
      this._db = new ProxyDb(this.base.db) // new Proxy(this.base.db, handlerDb)
    }
    return this._db
  }
}

class BuildProxy {
  constructor (env, useThisLog = false) {
    this.env = env

    log = useThisLog
      ? useThisLog
      : pino({
        name: 'proxy',
        level: 'trace',
        transport: {
          targets: [
            {
              target: 'pino/file',
              level: env.PROXY_LOGFILE_LEVEL || 'trace',
              options: {
                destination: path.resolve(process.cwd(), env.PROXY_LOGFILE || 'tmp/storage-proxy.log'),
                append: false
              }
            },
            {
              target: 'pino-pretty',
              level: env.PROXY_CONSOLE_LEVEL || 'debug',
              options: {
                translateTime: true,
                // levelFirst: true,
                colorize: true
              }
            }
          ]
        }
      })
    log.debug('BuildProxy constructed')
  }

  get needsFallback () {
    return true
  }

  init (cb) {
    // if (this.env.PROXY_TIMING && this.env.PROXY_TIMING !== '0') {
    //   log.info('timing enabled')
    // }
    cb(null, new ProxyStorage(this._mongo))
  }

  setFallback (mongo) {
    this._mongo = mongo
  }
}

module.exports = BuildProxy
