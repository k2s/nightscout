#!/usr/bin/env node

import { join } from 'path'
import { createReadStream, readdirSync, existsSync, unlinkSync } from 'fs'
import fs from 'fs-extra'
import BSONStream from 'bson-stream'
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import untildify from 'untildify'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { buildHints, createCollection } from '../lib/_collection-hints.js'
import pino from 'pino'
import jsonDiff from 'json-diff'

let shadowEnv = {}
let log

function initLog (destination, levelConsole = 'info', levelFile = 'debug') {
  log = pino({
    name: 'sqlite-storage',
    level: 'trace',
    transport: {
      targets: [
        {
          target: 'pino/file',
          level: levelFile,
          options: {
            destination,
            append: false
          }
        },
        {
          target: 'pino-pretty',
          level: levelConsole,
          options: {
            translateTime: true,
            levelFirst: true,
            colorize: true
          }
        }
      ]
    }
  })
  log.debug('BuildSqlite constructed')
}

function _trim (string) {
  return string.trim()
}

Object.keys(process.env).forEach((key) => {
  shadowEnv[_trim(key)] = _trim(process.env[key])
})

function readENV (varName, defaultValue) {
  //for some reason Azure uses this prefix, maybe there is a good reason
  var value = shadowEnv['CUSTOMCONNSTR_' + varName] ||
    shadowEnv['CUSTOMCONNSTR_' + varName.toLowerCase()] ||
    shadowEnv[varName] ||
    shadowEnv[varName.toLowerCase()]

  if (varName === 'DISPLAY_UNITS') {
    if (value && value.toLowerCase().includes('mmol')) {
      value = 'mmol'
    } else {
      value = defaultValue
    }
  }

  return value != null ? value : defaultValue
}

yargs(hideBin(process.argv))
  .command('import <db-file> <dump-folder>', 'import Nightscout MongoDB dump folder into Sqlite database', yargs => {
    yargs
      .positional('db-file', {
        describe: 'path to Sqlite database file'
      })
      .positional('dump-folder', {
        describe: 'path to Sqlite database file'
      })
      .option('force', {
        alias: 'f',
        type: 'boolean',
        describe: 'will override existing Sqlite DB file',
      })
  }, async argv => {
    const dumpFolder = untildify(argv.dumpFolder)
    const dbFile = untildify(argv.dbFile)
    initLog(dbFile + '.log')
    log.info(`importing Nightscout data from ${dumpFolder} to ${dbFile}`)

    const entries_collection = readENV('ENTRIES_COLLECTION') || readENV('MONGO_COLLECTION', 'entries')
    const env = {
      entries_collection,
      authentication_collections_prefix: readENV('MONGO_AUTHENTICATION_COLLECTIONS_PREFIX', 'auth_'),
      treatments_collection: readENV('MONGO_TREATMENTS_COLLECTION', 'treatments'),
      profile_collection: readENV('MONGO_PROFILE_COLLECTION', 'profile'),
      settings_collection: readENV('MONGO_SETTINGS_COLLECTION', 'settings'),
      devicestatus_collection: readENV('MONGO_DEVICESTATUS_COLLECTION', 'devicestatus'),
      food_collection: readENV('MONGO_FOOD_COLLECTION', 'food'),
      activity_collection: readENV('MONGO_ACTIVITY_COLLECTION', 'activity')
    }
    const def = buildHints(env)

    if (existsSync(dbFile)) {
      if (argv.force) {
        unlinkSync(dbFile)
      } else {
        log.error(`DB file ${dbFile} already exists, use --force parameter to override`)
        process.exit(1)
      }
    }

    // create dataase
    const db = await open({
      filename: dbFile,
      driver: sqlite3.Database
    })

    // main import loop
    const files = readdirSync(dumpFolder)
    for (const fn of files) {
      if (!fn.endsWith('.metadata.json')) continue

      // read metadata and create
      const meta = await fs.readJson(join(dumpFolder, fn))
      if (meta.type !== 'collection') {
        log.warn(`skiping file ${fn}, type is not collection`)
        continue
      }
      const name = meta.collectionName
      const tblName = name
      const d = def[name]
      if (!d) {
        log.warn(`skipping file ${fn}, unknown collection`)
        continue
      }
      // if (name === 'entries') continue
      log.info(`importing collection ${name} from ${fn}`)

      await createCollection(db, name, d)

      let stmt
      if (d.tsField) {
        if (name === 'treatments' || name === 'entries') {
          // there are conflicts
          stmt = await db.prepare(`INSERT INTO ${tblName} VALUES(@id, @ts, @json) ON CONFLICT DO UPDATE SET json=excluded.json WHERE JSON_EXTRACT(excluded.json, '$.sysTime')>JSON_EXTRACT(json, '$.sysTime')`)
        } else {
          stmt = await db.prepare(`INSERT INTO ${tblName} VALUES(@id, @ts, @json)`)
        }
      } else {
        stmt = await db.prepare(`INSERT INTO ${tblName} VALUES(@id, @json)`)
      }
      await db.run('BEGIN TRANSACTION')

      await new Promise(resolve => {
        const rs = createReadStream(join(dumpFolder, fn.substring(0, fn.length - 14) + '.bson'))
        rs
          .pipe(new BSONStream())
          .on('data', async function (obj) {
            rs.pause()

            let data = {
              '@id': obj._id,
              '@json': JSON.stringify(obj)
            }
            if (d.tsField) {
              data['@ts'] = d.tsAsString ? Date.parse(obj[d.tsField]) : obj[d.tsField]
            }

            try {
              await stmt.run(data)
            } catch (error) {
              if (error.errno === 19) {
                let old = await db.get(`SELECT json FROM ${tblName} WHERE _id='${obj._id}'`)
                if (old) {
                  old = JSON.parse(old.json)
                  // console.log(jsonDiff.diffString(old, obj))
                  log.warn({
                    diff: jsonDiff.diff(old, obj),
                    error: error.message
                  }, 'duplicate of ' + tblName + '.' + obj._id.toString() + ', old record kept')
                } else {
                  log.warn({
                    error: error.message,
                    data
                  }, 'uniqueness problem in ' + tblName + ', record lost')
                }
              } else {
                log.warn({
                  obj,
                  error: error.message
                }, tblName + '.' + obj._id.toString()) // obj
              }
            } finally {
              rs.resume()
            }
          })
          .on('end', function () {
            resolve()
          })
      })

      // await new Promise(resolve => {
      //   const rawDb = db.getDatabaseInstance()
      //   rawDb.wait(() => resolve(1))
      // })
      await stmt.finalize()
      await db.run('COMMIT')
    }
    await db.close()
    log.info('done.')
  })
  .strictCommands()
  .demandCommand(1)
  .help('h')
  .parse()

// TODO why there are so many duplicities in 'entries'
// select ts, JSON_EXTRACT(json, '$.type') as f1, count(*) as c from entries group by ts, f1 having c>1
//select ts, JSON_EXTRACT(json, '$.type') as f1, JSON_EXTRACT(json, '$.device') as f2, count(*) as c from entries group by ts, f1, f2 having c>1
