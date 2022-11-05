import fs from 'fs-extra'
import ndjson from 'ndjson'
import objectHash from 'object-hash'
import dot from 'dot-object'
import prettyMilliseconds from 'pretty-ms'

const knownOperationsMap = {
  '961c6611676b99270b961d90b2e99898c65b762d': 'load all auth data into memory (auth_roles)',
  '05511131e65a6d0bb6281d9e571983a6bab0e5da': 'load all auth data into memory (auth_subjects)',
  '74fe9482573a2cf59d30e4ce346a0a6f61434d5f': 'last treatments (treatments)',
  '46f578573c564fa55a0bff9ea70da41747486d33': 'by eventType eg. Sensor Start (treatments)',
  'fcb2178acb2a1b0f749e4fd82feea6aaacbe9be5': 'by eventType eg. Profile Switch (treatments)',
  '6f8a6877a7715eb3e91f041d7489453bbc4939a5': 'las 100 treatments (treatments)',
  '6c2965dd2974299ed6b9a3098996951968bee0a0': 'newest profile by startDate (profile)',
  'e6f3df0d182e97c500df137cb77d64f4b1f841b7': 'all data (food)',
  '8b56d502e2e173ce6839daf24a05235087f3f6a6': 'latest entries (entries)',
  '62f5c03ecfae7ae97c5610d0efa6ce29727e3522': 'latest entries (devicestatus)',
  'bed3602f54982e715ac367acff3aa2b201367c71': 'latest activities (activity)',
  'ea41c955527e00f9887878be36cb8503fa7f64e0': 'last data (entries)',
  'afd49fefd31b92564c005a0be28a7375c07c82b7': 'insertOne: phone battery (devicestatus)',
  'dd996f6f593132d0132899a926ddc50d7d7681d4': 'update: xDrip-Follower sensor value (devicestatus)'
}
const knownOperations = new Set(Object.keys(knownOperationsMap))

function commentFind (obj, hash) {
  const q = obj.query
  let s = []
  if (q.sort) {
    s.push('sort by ' + (typeof q.sort.keyOrList === 'object' ? Object.entries(q.sort.keyOrList).map(([k, v]) => k + ' ' + (v > 0 ? 'ASC' : 'DESC')).join(',') : q.sort.keyOrList))
  }
  if (q.limit) {
    s.push('limit ' + q.limit)
  }

  if (!q.filter) {
    if (!q.limit) {
      s.push('rist of too much data')
    } else if (q.limit === 1) {
      s.push('LIMIT 1')
    } else {
      s.push('without filter missing sort')
    }
  } else {
    if (Object.keys(q.filter).length === 0) {
      s.push('no filter')
      if (!q.limit) {
        s.push('PROBLEM too much data')
      }
    } else if (Object.keys(q.filter).length === 1 && q.filter.hasOwnProperty('created_at')) {
      s.push('simple date index')
    } else {
      if (q.filter.hasOwnProperty('created_at')) {
        s.push('has date index + MORE')
      }
    }
  }
  return s.length ? s.join('; ') : undefined
}

const processLog = async function (fn, excludeKnownOperations = false) {
  const ignoreKnown = excludeKnownOperations
  if (ignoreKnown) {
    console.warn('known operations will be ignored')
  }
  return new Promise(resolve => {
    let queries = {}
    let indexes = {}
    let modify = {}
    let all = {
      modify,
      queries,
      indexes,
      restarts: 0,
      unknown: new Set(),
      unknownHashes: new Set()
    }

    function reset () {
      queries = {}
      indexes = {}
      modify = {}
      all = {
        modify,
        queries,
        indexes,
        restarts: 0,
        unknown: new Set(),
        unknownHashes: new Set()
      }
    }

    reset()

    const hashKeys = function (query, name) {
      const o = dot.dot(query)
      if (name) {
        o['__' + name] = true
      }
      const hash = objectHash(o, { excludeValues: true })
      if (!knownOperations.has(hash)) {
        all.unknownHashes.add(hash)
      } else if (ignoreKnown) {
        return false
      }

      return hash
    }

    fs.createReadStream(fn)
      .pipe(ndjson.parse())
      .on('data', function (obj) {
        // // console.log(obj)
        // if (all.hasOwnProperty(obj._id)) {
        //   console.log(`duplicitny zaznam v ${obj._id}`, fn)
        // }
        // all[obj._id] = obj

        // standardize collection name
        if (obj.name === process.env.CUSTOMCONNSTR_mongo_collection) {
          obj.name = 'entries'
        }

        switch (obj.msg) {
          case 'find': {
            queries[obj.name] = queries[obj.name] || {}
            const hash = hashKeys(obj.query, obj.name)
            if (!hash) return // ignore

            // add propery of time distance
            let tm = dot.pick('query.filter.created_at', obj) || dot.pick('query.filter.created_at', obj,)
            if (tm) {
              tm = Object.values(tm).sort().reverse()[0]
              obj.query.__timeDistance = prettyMilliseconds(obj.time - Date.parse(tm))
            }

            // add to list of queries
            if (!queries[obj.name][hash]) {
              queries[obj.name][hash] = {
                item: obj.query,
                count: 1,
                comment: commentFind(obj, hash)
              }
            } else {
              if (Array.isArray(queries[obj.name][hash].item)) {
                queries[obj.name][hash].item.push(obj.query)
              }
              queries[obj.name][hash].count++
            }
            break
          }
          case 'createIndex':
            if (typeof obj.fieldOrSpec === 'object') {
              obj.fieldOrSpec = Object.keys(obj.fieldOrSpec)
            }
            indexes[`${obj.name}(${obj.fieldOrSpec})`] = true
            break
          case 'collectProps':
            all.collectProps = obj.collectProps
            break
          case 'insertOne':
          case 'save':
          case 'update': {
            modify[obj.msg] = modify[obj.msg] || {}
            modify[obj.msg][obj.name] = modify[obj.msg][obj.name] || {}
            const c = modify[obj.msg][obj.name]

            let o
            let hash
            switch (obj.msg) {
              case 'update':
                o = {
                  selector: obj.selector,
                  options: obj.options,
                  update: obj.update
                }
                hash = hashKeys({ ...o.selector, ...o.options, ...o.update }, obj.name)
                break
              case 'insertOne':
                o = { doc: obj.doc }
                hash = hashKeys(obj.doc, obj.name)
                break
              case 'save':
                o = { doc: obj.doc }
                hash = hashKeys(obj.doc, obj.name)
                break
              default:
                console.error('unknown', obj.msg)
                process.exit(1)
            }
            if (!hash) return // ignore

            if (!c[hash]) {
              c[hash] = {
                item: [o],
                count: 1
              }
            } else {
              if (Array.isArray(c[hash].item)) {
                c[hash].item.push(o)
              } else {
                // remember last
                c[hash].item = o
              }
              c[hash].count++
            }
            break
          }
          case 'stats':
          case 'BuildProxy constructed':
          case 'timing enabled':
            // ignore
            break
          case 'proxy-storage INIT': {
            const restarts = all.restarts
            reset()
            all.restarts = restarts + 1
            break
          }
          default:
            all.unknown.add(obj.msg)
            console.log('unknown', obj.msg)
        }
      })
      .on('end', function () {
        all.unknownHashes = Array.from(all.unknownHashes)
        resolve(all)
      })
  })
}

if (process.argv.length === 4 || process.argv.length === 5) {
  if (process.argv.length === 5)  {

  }
  await fs.writeJson(
    process.argv[3],
    await processLog(process.argv[2], !!process.argv[4]),
    { spaces: '  ' }
  )
} else {
  console.info(process.argv[3] + ' <storage-proxy.log> <storage-proxy.json> [excludeKnownOperations]')
  console.error('ERROR: input and output files required')
  process.exit(1)
}
