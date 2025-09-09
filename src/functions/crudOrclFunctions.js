import logger from '../utils/logger.js'
import { decodeString } from './encodeDecodeFunctions.js'
import { valKeys } from '../utils/arrayFunctions.js'

const libName = '[[api/crudOrclFunctions.js]]'

export const functions = {
  jsonDateCam(date) {
    return date.substring(0, 4) + '-' + date.substring(4, 6) + '-' + date.substring(6, 8)
  },

  dateToYMD(date) {
    var d = date.getDate()
    var m = date.getMonth() + 1 //Month from 0 to 11
    var y = date.getFullYear()
    return '' + y + '-' + (m <= 9 ? '0' + m : m) + '-' + (d <= 9 ? '0' + d : d)
  },

  dateToYMD2(date) {
    var d = date.getDate()
    var m = date.getMonth() + 1 //Month from 0 to 11
    var y = date.getFullYear()
    return '' + y + (m <= 9 ? '0' + m : m) + (d <= 9 ? '0' + d : d)
  },

  keysToLowerCase(obj) {
    const keys = Object.keys(obj)
    const n = keys.length
    let i = 0
    let key = ''
    while (i < n) {
      key = keys[i] // "cache" it, for less lookups to the array
      obj[key.toLowerCase()] = obj[key] // swap the value to a new lower case key
      delete obj[key] // delete the old key
      if (Array.isArray(obj[key.toLowerCase()])) {
        obj[key.toLowerCase()] = this.arrayKeysToLowerCase(obj[key.toLowerCase()])
      }
      i++
    }
    return obj
  },

  arrayKeysToLowerCase(obj) {
    const n = obj.length
    let i = 0
    while (i < n) {
      obj[i] = this.keysToLowerCase(obj[i])
      i++
    }
    return obj
  },

  extractItem(obj, item) {
    for (let colum in obj) {
      if (colum == item) {
        delete obj[item]
      }
    }
    return obj
  },

  extractDbConn(dataConn) {
    const functionName = `${libName} [extractDbConn]`
    const requiredFields = ['dbuser', 'dbpassword', 'dbhost', 'dbport', 'dbsid']
    let err = {}
    if (!valKeys(dataConn, requiredFields, err)) {
      const error = `${functionName} - Error al validar la conexión a la base de datos por header`
      logger.error(error)
      return {}
    }
    const decodeKey = process.env.DECODEKEY ?? ''
    let dborcl = {
      user: decodeString(decodeKey, dataConn.dbuser),
      password: decodeString(decodeKey, dataConn.dbpassword),
      connectString: dataConn.dbhost + ':' + dataConn.dbport + '/' + dataConn.dbsid,
    }
    return dborcl
  },

  validateDbConn(dataConn, error) {
    if (process.env.DBCONN) return true

    let errorCount = 0
    if (!dataConn?.dbuser) {
      error.push('No dbUser especified')
      errorCount++
    }

    if (!dataConn?.dbpassword) {
      error.push('No dbPassword especified')
      errorCount++
    }

    if (!dataConn?.dbhost) {
      error.push('No dbHost especified')
      errorCount++
    }

    if (!dataConn?.dbport) {
      error.push('No dbPort especified')
      errorCount++
    }

    if (!dataConn?.dbsid) {
      error.push('No dbSid especified')
      errorCount++
    }

    if (errorCount) {
      return false
    }

    return true
  },

  convertDateCam(obj) {
    for (let colum in obj) {
      if (
        typeof obj[colum] === 'string' &&
        obj[colum].substring(0, 2) == '20' &&
        parseInt(obj[colum].substring(4, 6), 10) <= 12 &&
        parseInt(obj[colum].substring(6, 8), 10) <= 31
      ) {
        obj[colum] = `TO_DATE('${this.jsonDateCam(obj[colum])},'YYYY-MM-DD')`
      }
    }
    return obj
  },

  
}
