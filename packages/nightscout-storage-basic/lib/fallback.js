/**
 * Full fallback to original MongoDB driver
 */
class Fallback {
  get needsFallback () {
    return true
  }

  init (cb) {
    cb(null, this._mongo)
  }

  setFallback (mongo) {
    this._mongo = mongo
  }
}

module.exports = Fallback
