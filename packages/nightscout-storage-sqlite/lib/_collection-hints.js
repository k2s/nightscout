function buildHints (env) {
  return {
    [env.activity_collection]: {
      tsField: 'created_at',
      tsAsString: true,
      index: {
        'ts': { uniq: true }
      }
    },
    [env.profile_collection]: {
      tsField: 'created_at',
      index: {
        'ts': { uniq: true },
        'startDate': {}
      }
    },
    [env.entries_collection]: {
      tsField: 'date', // sysTime? fallbackDateField: 'date'
      index: {
        'ts,type': { uniq: true }, // dedupFallbackFields: ['date', 'type'], use also device?
        'date': {}
      }
    },
    [env.treatments_collection]: {
      tsField: 'created_at',
      tsAsString: true,
      index: {
        'ts,eventType': { uniq: true }
      }
    },
    [env.devicestatus_collection]: {
      tsField: 'created_at',
      tsAsString: true,
      index: {
        'ts,device': { uniq: true }
      }
    },
    [env.food_collection]: {
      // tsField: 'created_at',
      // tsAsString: true
    },
    [env.settings_collection]: {},
    [env.authentication_collections_prefix + 'subjects']: {
      // tsField: 'created_at',
      // tsAsString: true
      index: {
        name: {}
      }
    },
    [env.authentication_collections_prefix + 'roles']: {
      // tsField: 'created_at',
      // tsAsString: true
      index: {
        name: {}
      }
    }
  }
}

async function createCollection (db, name, def, withIndexes = true) {
  if (def.tsField) {
    // data are time series
    await db.exec(`CREATE TABLE IF NOT EXISTS '${name}' (_id NOT NULL, ts INTEGER NOT NULL, json TEXT, PRIMARY KEY('_id'))`)
  } else {
    // other type of data
    await db.exec(`CREATE TABLE IF NOT EXISTS '${name}' (_id NOT NULL, json TEXT, PRIMARY KEY('_id'))`)
  }

  if (withIndexes) {
    for (const [k, v] of Object.entries(def.index || {})) {
      const indexName = `IDX_${name}_${k.replace(/,/g, '_')}`
      const flds = k
        .split(',')
        .map(f => ['_id', 'ts'].indexOf(f) === -1 ? `JSON_EXTRACT(json, '$.${f}')` : f)
        .join(',')
      await db.exec(`CREATE ${v.uniq ? 'UNIQUE' : ''} INDEX IF NOT EXISTS '${indexName}' ON '${name}'(${flds})`)
    }
  }
}

module.exports = {
  buildHints,
  createCollection
}
