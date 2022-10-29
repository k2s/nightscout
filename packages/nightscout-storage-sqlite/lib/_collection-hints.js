function buildHints(env) {
  return {
    [env.activity_collection]: {
      tsField: 'created_at',
      tsAsString: true
    },
    [env.profile_collection]: {
      tsField: 'created_at'
    },
    [env.entries_collection]: {
      tsField: 'date'
    },
    [env.treatments_collection]: {
      tsField: 'created_at',
      tsAsString: true
    },
    [env.food_collection]: {
      // tsField: 'created_at',
      // tsAsString: true
    },
    [env.entries_collection]: {
      tsField: 'sysTime',
      tsAsString: true
    },
    [env.devicestatus_collection]: {
      tsField: 'created_at',
      tsAsString: true
    },
    [env.authentication_collections_prefix + 'subjects']: {
      tsField: 'created_at',
      tsAsString: true
    },
    [env.authentication_collections_prefix + 'roles']: {
      tsField: 'created_at',
      tsAsString: true
    }
  }
}
module.exports = {
  buildHints
}
