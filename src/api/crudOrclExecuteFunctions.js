import OracleDB from 'oracledb'
import logger from '../utils/logger.js'

import { functions } from './crudOrclFunctions.js'
import { sentences } from './crudOrclSentences.js'
import { isEmpty } from '../utils/index.js'

const libName = '[api/crudOrclExecuteFunctions]'

export const executeFunctions = {
  async executeOrclString(header, sql, bind, options = {}) {
    OracleDB.outFormat = OracleDB.OUT_FORMAT_OBJECT
    OracleDB.autoCommit = true
    let connection

    const functionName = `${libName} [executeOrclString]`
    try {
      const exteriorConn = functions.extractDbConn(header)
      if (!isEmpty(exteriorConn)){
        connection = await OracleDB.getConnection(exteriorConn)
        logger.info(`${functionName} - Connection success by 'Header`)
      }
      else{
        if (!isEmpty(header.dbpool)) {
          connection = await OracleDB.getConnection({ poolAlias: header.dbpool })
          logger.info(`${functionName} - Connection success by 'Pool`)
        }
        else {
          logger.error(`${functionName} - No DB connection`)
          return { success: false, error: 'No DB connection' }
        }          
      }
      logger.info(`${functionName} SQL= ${sql}`)
      const res = await connection.execute(sql, bind, options)
      return {
        success: true,
        data: functions.arrayKeysToLowerCase(res.rows ?? []),
        rowsAffected: res.rowsAffected ?? 0,
        outBinds: res.outBinds ?? {},
      }
    } catch (error) {
      const catchError = `${functionName} - ${error.message}`
      logger.error(catchError)
      return { success: false, error: error.message }
    } finally {
      if (connection) {
        await connection.close()
        logger.info(`${functionName} - Close connection`)
      }
    }
  },

  async executeOrclTransaction(table, header, data) {
    OracleDB.outFormat = OracleDB.OUT_FORMAT_OBJECT
    OracleDB.autoCommit = false
    let connection
    let operation
    let checkOpr
    let oprTable = false
    let logTransaction = []
    let countError = 0
    let sql = ''
    let res
    const functionName = `${libName} [executeOrclTransaction]`
    if (table === '') oprTable = true
    try {
      logger.info(`${functionName} - Transaction init`)
      connection = await OracleDB.getConnection(
        process.env.DBCONN ?? functions.extractDbConn(header)
      )
      logger.info(
        `${functionName} - Connection success by ${process.env.DBCONN ? 'ENV' : 'Header'}`
      )
      for (let opr in data) {
        operation = data[opr]
        checkOpr = true
        switch (operation.action ?? 'NONE') {
          case 'INSERT':
            if (oprTable && !operation?.object) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Object key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Object key not found`)
              countError++
              checkOpr = false
            }
            if (!operation?.body && checkOpr) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Body key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Body key not found`)
              countError++
              checkOpr = false
            }
            if (checkOpr)
              sql = sentences.insertString(oprTable ? operation.object : table, operation.body)
            break

          case 'UPDATE':
            if (oprTable && !operation?.object) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Object key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Object key not found`)
              countError++
              checkOpr = false
            }
            if (!operation?.body && checkOpr) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Body key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Body key not found`)
              countError++
              checkOpr = false
            }
            if (!operation?.query && checkOpr) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Query key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Query key not found`)
              countError++
              checkOpr = false
            }
            if (checkOpr)
              sql = sentences.updateString(
                oprTable ? operation.object : table,
                operation.body,
                operation.query
              )
            break

          case 'DELETE':
            if (oprTable && !operation?.object) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Object key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Object key not found`)
              countError++
              checkOpr = false
            }
            if (!operation?.query && checkOpr) {
              logTransaction.push({
                [`opr-${opr}-${operation.action}`]: 'Error - Query key not found',
              })
              logger.error(`${functionName} - operation ${opr} fail - Query key not found`)
              countError++
              checkOpr = false
            }
            if (checkOpr)
              sql = sentences.deleteString(oprTable ? operation.object : table, operation.query)
            break

          default:
            logTransaction.push({ [`opr-${opr}-${operation.action}`]: 'Error - Unknown action' })
            logger.error(`${functionName} - operation ${opr} fail - Unknown action`)
            countError++
            checkOpr = false
            break
        }

        if (checkOpr)
          try {
            res = await connection.execute(sql)
            logTransaction.push({
              [`opr-${opr}-${operation.action}`]: `Success - rowsAfffected:${res.rowsAffected ?? 0} `,
            })
            logger.info(`${functionName} - operation ${opr}: sucess`)
          } catch (error) {
            logTransaction.push({ [`opr-${opr}-${operation.action}`]: `Error - ${error.message}` })
            logger.error(`${functionName} - operation ${opr} fail - ${error.message}`)
            countError++
          }
      }
      if (!countError) {
        await connection.commit()
        logger.info(`${functionName} - Transaction commit`)
        return { success: true, data: [], logTransaction }
      }
      if (countError) {
        await connection.rollback()
        logger.info(`${functionName} - Transaction rollback`)
        return { success: false, data: [], logTransaction }
      }
    } catch (error) {
      logger.error(`${functionName} ${error.message}`)
      return { success: false, error: error.message }
    } finally {
      if (connection) {
        await connection.close()
        logger.info(`${functionName} - Close connection`)
      }
    }
  },
}
