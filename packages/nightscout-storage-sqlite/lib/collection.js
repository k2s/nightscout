const Find = require('./_find')
const randomString = require('randomized-string')

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

function operator (d, convertFn) {
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

  return o + (typeof v === 'string' ? `'${v}'` : v) // TODO string?
}

class Collection {
  static factory (name, driver, def) {
    return new Collection(name, driver, def)
  }

  constructor (name, driver, def) {
    this.name = name
    this.driver = driver
    this._d = driver._d
    this._def = def
    this.log = driver.log
    this.logSql = driver.logSql
  }

  _makeSql (collection, query) {
    // if (collection ==='food') debugger
    const d = this._def
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
          const sortFields = Object.keys(query.sort.keyOrList).join(',')
          if (sortFields !== d.tsField && (!d.index || !d.index.hasOwnProperty(sortFields))) {
            // TODO maybe remove later
            this.log.debug(`maybe not indexed sort search ${this.name}.${sortFields}`)
          }

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
              //   this.log.debug(`maybe not indexed sort property ${this.name}.${k}`)
              //   sort.push(`JSON_EXTRACT(json, '$.${k}')` + (v < 0 ? ' DESC' : ''))
              // }
              // if (collection === this.driver.env.profile_collection) debugger
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

    this.log.warn({
      collection,
      query
    }, 'unknown query')
    return false
  }

  _select (collection, query, cb) {
    let logSql = this.logSql
    let sql = this._makeSql(collection, query)
    if (!sql) {
      sql = `SELECT ${collection}.json FROM ${collection} WHERE 1=0`
      logSql = false
    } else {
      sql = `SELECT ${collection}.json ` + sql
    }

    this._d.all(sql)
      .then(r => {
        logSql && this.log.trace({
          collection,
          sql,
          r
        }, '_select')
        cb(null, r.map(d => this._unpack(collection, d.json)))
      })
      .catch(err => {
        this.log.error({
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
        this.log.info({
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
      this.log.info({
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
    if (options.upsert) {
      this._updateUsert(selector, update, options)
        .then(r => callback(null, r))
        .catch(err => callback(err))
    } else {
      this._update(selector, update, options, callback)
    }
  }

  async _updateUsert (selector, update, options) {
    // { result/*, connection, message*/ }
    const d = this._def

    // we try to find out if there will be update or insert
    // TODO this select should exclusively lock the table until the upsert is finished, but is problematic with sqlite3
    let sql = 'SELECT _id ' + this._makeSql(this.name, { filter: selector }) // .replace(' WHERE ', ' EXCLUSIVE WHERE ')
    const row = await this._d.get(sql)
    let id
    if (row) {
      id = row._id
    } else {
      id = update._id ? update._id.toString() : /*ObjectID*/(randomString.generate({
        charset: 'hex',
        lowerCaseOnly: true,
        length: 24
      }))
    }
    update._id = id

    let stmt
    if (d.tsField) {
      const tsField = d.tsField
      const tsAsString = d.tsAsString
      const tsValue = selector[tsField] || update[tsField]

      stmt = this._d.run(`INSERT INTO ${this.name} (_id, ts, json) VALUES (:id, :ts, :json) ON CONFLICT DO UPDATE SET ts=:ts, json=:json`, {
        ':id': id,
        ':ts': tsAsString ? Date.parse(tsValue) : tsValue,
        ':json': this._pack(this.name, update)
      })
    } else {
      stmt = this._d.run(`INSERT INTO ${this.name} (_id, json) VALUES (:id, :json) ON CONFLICT DO UPDATE SET json=:json`, {
        ':id': id,
        ':json': this._pack(this.name, update)
      })
    }
    try {
      const result = await stmt
      this.logSql && this.log.trace({
        collection: this.name,
        selector,
        update,
        options
      }, 'update')

      return {
        result:
          {
            'n': result.changes,
            'nModified': result.changes, // TODO ?
            'upserted': [
              {
                'index': 0,
                '_id': id // TODO this will not be true if it was update
              }
            ],
            'ok': 1
          }
      }
    } catch (err) {
      this.log.error({
        collection: this.name,
        selector,
        update,
        options,
        err
      }, 'update')
      throw err
    }
  }

  _update (selector, update, options, callback) {
    // { result/*, connection, message*/ }
    const d = this._def
    let tsName = d ? d.tsField : 'created_at'

    const sql = this._makeSql(this.name, { filter: selector })
    const flds = { json: {} }
    for (let [k, v] of Object.entries(update)) {
      if (k === '_id') continue // ???

      if (k === tsName) {
        flds.ts = typeof v === 'string' ? `'${v}'` : v
      }
      // flds.json[k] = typeof v === 'string' ? `'${v}'` : v
    }
    if (Object.keys(flds.json).length) {
      flds.json = JSON.stringify(flds.json) // .replace(/'/g, '\\\'')
    } else {
      delete flds.json
    }
    console.log(`UPDATE SET ${Object.keys(flds).map(x => x + '=' + (x === 'json' ? 'json_patch(json, ?)' : '?'))} ` + sql, Object.values(flds)[0])
    this._d.run(`UPDATE SET ${Object.keys(flds).map(x => x + '=' + (x === 'json' ? 'json_patch(json, ?)' : '?'))} ` + sql, Object.values(flds)[0])
      .then(result => {
        this.logSql && this.log.trace({
          collection: this.name,
          selector,
          update,
          options
        }, 'update')

        const r = {
          result:
            {
              'n': result.changes,
              'nModified': result.changes, // TODO ?
              'upserted': [
                {
                  'index': 0,
                  // '_id': id // TODO needs select
                }
              ],
              'ok': 1
            }
        }
        callback(null, r)
      })
      .catch(err => {
        this.log.error({
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
      // options = undefined
    }

    // TODO use collection class
    const d = this._def
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

    const stmt = tsName
      ? this._d.run(`INSERT INTO ${this.name}(_id, ts, json) VALUES (:id, :ts, :json) ON CONFLICT(_id) DO UPDATE SET ts=:ts, json=:json`, {
        ':id': id,
        ':ts': d.tsAsString ? Date.parse(doc[tsName]) : doc[tsName],
        ':json': this._pack(this.name, doc)
      })
      : this._d.run(`INSERT INTO ${this.name}(_id, json) VALUES (:id, :json) ON CONFLICT(_id) DO UPDATE SET json=:json`, {
        ':id': id,
        ':json': this._pack(this.name, doc)
      })
    stmt.then(result => {
      this.logSql && this.log.trace({
        collection: this.name,
        doc
      }, 'insertOne')

      const r = {
        result: {
          ok: 1,
          n: result.changes
        },
        ops: [
          doc
        ],
        insertedCount: result.changes,
        insertedIds: [{
          _id: id
        }]
      }
      callback(null, r)
    })
      .catch(err => {
        this.log.error({
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
      // options = undefined
    }

    const sql = this._makeSql(this.name, { filter: selector })
    this._d.run('DELETE ' + sql)
      .then(() => {
        this.logSql && this.log.trace({
          collection: this.name,
          selector
        }, 'remove')
        callback(null)
      })
      .catch(err => {
        this.log.error({
          collection: this.name,
          selector,
          err
        }, 'remove')
        callback(err)
      })
  }
}

module.exports = Collection
