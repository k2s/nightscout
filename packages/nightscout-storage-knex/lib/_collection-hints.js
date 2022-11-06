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

async function createCollection (knex, name, def, withIndexes = true) {
  // TODO support schema for other DB engines .withSchema('main')
  await knex.schema.hasTable(name).then(function(exists) {
    if (!exists) {
      return knex.schema.createTable(name, function (table) {
        table.string('_id').notNullable()
        if (def.tsField) {
          table.integer('ts').notNullable()
        }
        table.json('json')
        table.primary('_id')

        if (withIndexes) {
          for (const [k, v] of Object.entries(def.index || {})) {
            let hasJsonFiels = false
            const indexName = `IDX_${name}_${k.replace(/,/g, '_')}`
            const flds = k
              .split(',')
              .map(f => {
                if (['_id', 'ts'].indexOf(f) === -1) {
                  hasJsonFiels = true
                  return `JSON_EXTRACT(json, '$.${f}')`
                } else {
                  return f
                }})
            if (hasJsonFiels) {
              // JSON indexes missing and we need to create with raw commands https://github.com/knex/knex/issues/4443
              console.warn('JSON idx')
            } else {
              if (v.uniq) {
                table.unique(flds, { indexName: indexName })
              } else {
                table.index(flds, indexName)
              }
            }
            // await db.exec(`CREATE ${v.uniq ? 'UNIQUE' : ''} INDEX IF NOT EXISTS '${indexName}' ON '${name}'(${flds})`)
          }
        }
      })
    }
  })
}

module.exports = {
  buildHints,
  createCollection
}
