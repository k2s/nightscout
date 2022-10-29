class Find {
  constructor (name, cb) {
    this.cb = cb
    this.__name = name
  }

  find (query, options, callback) {
    this.query = { filter: query }
    return this
  }

  project (value) {
    this.query.project = value
    return this
  }

  sort (keyOrList, direction) {
    this.query.sort = {
      keyOrList,
      direction
    }
    return this
  }

  limit (value) {
    this.query.limit = value
    return this
  }

  toArray (callback) {
    this.cb(null, [], this.query, callback)
    // return this.cur.toArray((err, items) => {
    //   this.cb(err, items.map(o => new Proxy({...o, __col: this.__name}, handleItem)), this.query, callback)
    // })
  }
}

module.exports = Find
