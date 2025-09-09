import OracleDB from 'oracledb'
import { TIME_SLEEP_RETRY_CON } from '../utils/const.js'
import {
  extractTableName,
  isDate,
  isEmpty,
  sleep,
  stringifyWithCircularDetection,
  turnToCamelCase,
} from '../utils/index.js'
import { functions } from '../functions/crudOrclFunctions.js'
import Logger from '../utils/logger.js'

let pool = null
let connectionPools = {}

Logger.info('Inicializando el cliente de OracleDB')
OracleDB.initOracleClient()
// OracleDB.outFormat = OracleDB.OUT_FORMAT_OBJECT

/*
 * Funcion para crear un pool de conexiones a la base de datos Oracle. Esta funcion se debe
 * ejecutar al inicio de la aplicacion para que las conexiones a la base de datos sean mas
 * eficientes.
 */
export const initOraclePool = async ({ logger = Logger } = {}) => {
  const fName = '[initOraclePool]'

  let ORACLE_POOL_CONNECTIONS = []
  try {
    ORACLE_POOL_CONNECTIONS = JSON.parse(process.env.ORACLE_POOL_CONNECTIONS || '[]')
  } catch (err) {
    const error = `Error al parsear pool connections`
    logger.error(`${fName} ${error}`)
    console.error(err)
  }

  if (!isEmpty(ORACLE_POOL_CONNECTIONS) && Array.isArray(ORACLE_POOL_CONNECTIONS)) {
    for (const poolConfig of ORACLE_POOL_CONNECTIONS) {
      const { alias, user, password, connectString } = poolConfig ?? {}
      if (isEmpty(alias) || isEmpty(user) || isEmpty(password) || isEmpty(connectString)) {
        const error = 'No se han enviado los datos necesarios para crear el pool de conexiones'
        logger.error(`${fName} ${error}`)
        continue
      }

      let attempts = 1
      const maxAttempts = 3
      let created = false

      while (attempts < maxAttempts && !created) {
        try {
          logger.info(
            `${fName} Creando pool ${alias} de conexiones a la base de datos Oracle... (Intento ${attempts})`
          )
          connectionPools[alias] = await OracleDB.createPool({
            user,
            password,
            connectString,
            poolAlias: alias,
            poolMin: parseInt(process.env.ORACLE_POOL_MIN ?? 5, 10),
            poolMax: parseInt(process.env.ORACLE_POOL_MAX ?? 50, 10),
            poolIncrement: parseInt(process.env.ORACLE_POOL_INCREMENT ?? 5, 10),
          })

          logger.info(`${fName} Cantidad de conexiones en el pool: ${connectionPools[alias].connectionsOpen}`)
          created = true
        } catch (err) {
          attempts++
          const error = `Error al crear el pool de conexiones ${alias} a la base de datos Oracle (Intento ${attempts})`
          logger.error(`${fName} ${error}`)
          console.error(err)
          // Retry only if error is ORA-12528 (listener: all appropriate instances are blocking new connections)
          if (attempts < maxAttempts) {
            logger.info(`${fName} Esperando para reintentar crear el pool ${alias}...`)
            await sleep(TIME_SLEEP_RETRY_CON)
          } else {
            break
          }
        }
      }
    }

    if (isEmpty(connectionPools)) {
      const error = 'No se han podido crear los pools de conexiones a la base de datos Oracle'
      logger.error(`${fName} ${error}`)
      return { success: false, error }
    }
  }
  return { success: true }
}


/*
 * Funcion para cerrar el pool de conexiones a la base de datos Oracle. Esta funcion se debe ejecutar
 * al final de la aplicacion para liberar los recursos utilizados por las conexiones a la base de datos.
 */

export const closePoolAndExit = async ({ logger = Logger } = {}) => {
  const fName = '[closePoolAndExit]'
  let ORACLE_POOL_CONNECTIONS = []
  try {
    ORACLE_POOL_CONNECTIONS = JSON.parse(process.env.ORACLE_POOL_CONNECTIONS || '[]')
  } catch (err) {
    const error = `Error al parsear la pool connections`
    logger.error(`${fName} ${error}`)
    console.error(err)
  }

  if (!isEmpty(ORACLE_POOL_CONNECTIONS)) {
    for (const poolConfig of ORACLE_POOL_CONNECTIONS) {
      const { alias } = poolConfig ?? {}
      try {
        logger.info(`${fName} Cerrando pool de conexiones '${alias}' a la base de datos Oracle...`)
        await OracleDB.getPool(alias).close(20) // 20 segundos de espera para cerrar las conexiones
      } catch (err) {
        const error = `Error al cerrar el pool de conexiones '${alias}' a la base de datos Oracle`
        logger.error(`${fName} ${error}`)
        console.error(err)
      }
    }
  } 

  return { success: true }
}

/**
 * getConnection - Función para establecer la conexión con la base de datos Oracle
 */
const getConnection = async (
  { siifWebPoolName = DEFAULT_POOL_ALIAS, siifWebUser } = {},
  { logger = Logger } = {}
) => {
  const fName = '[getConnection]'

  if (isEmpty(siifWebPoolName)) {
    const error = `${fName} No se ha enviado el nombre del pool de conexiones`
    logger.error(error)
    return { success: false, error }
  }

  logger.info(
    `${fName} Estableciendo conexion con la base de datos por pool de conexiones '${siifWebPoolName}'`
  )

  let attempts = 0
  const maxAttempts = 5

  while (attempts < maxAttempts) {
    try {
      logger.info(`${fName} Intento ${attempts + 1} de ${maxAttempts}`)

      const connection = await OracleDB.getConnection({ poolAlias: siifWebPoolName })

      // Si se envía el usuario de la conexión, se establece el identificador de sesión
      if (!isEmpty(siifWebUser)) {
        logger.info(`${fName} Estableciendo el usuario de la conexión: ${siifWebUser}`)
        await connection.execute(`BEGIN DBMS_SESSION.SET_IDENTIFIER(:clientId); END;`, {
          clientId: siifWebUser,
        })
      } else {
        logger.info(
          `${fName} No se ha enviado el usuario de la conexión, se usara el usuario del pool`
        )
        await connection.execute(`BEGIN DBMS_SESSION.SET_IDENTIFIER(NULL); END;`)
      }

      return { success: true, connection }
    } catch (err) {
      attempts++
      logger.error(`${fName} Error en intento ${attempts}: ${err.message}`)

      if (attempts >= maxAttempts) {
        const error = `${fName} Error al establecer la conexion con la base de datos por pool de conexiones '${DEFAULT_POOL_ALIAS}'`
        logger.error(`${fName} ${error}`)
        console.error(err)
        return { success: false, error }
      }
      await sleep(TIME_SLEEP_RETRY_CON)
    }
  }
}



/**
 * tableMetaData - Función para obtener la metadata de una tabla en la base de datos Oracle
 * @param {tableName} - Nombre de la tabla a consultar -> Obligatorio
 * @param {schemaName} - Nombre del esquema de la tabla -> Opcional
 */
export const tableMetaData = async (header = {}, { tableName, schemaName } = {}, { logger = Logger } = {}) => {
  const fName = '[tableMetaData]'

  if (isEmpty(tableName)) {
    const error = `${fName} No se ha enviado el nombre de la tabla`
    logger.error(error)
    return {
      success: false,
      error,
    }
  }
  let statement = `select 
                      atc.column_name, 
                      atc.data_type, 
                      atc.data_length, 
                      atc.data_precision,
                      DECODE(atc.nullable, 'Y', 'true', 'false') is_nullable, 
                      atc.data_default, NVL2(acc.column_name, 'true', 'false') is_pk
                  from all_tab_columns atc
                  left join all_constraints acPK ON acPK.table_name = atc.table_name and
                             acPK.constraint_type = 'P'
                             ${!isEmpty(schemaName) ? 'AND acPK.owner = atc.owner' : ''}
                  left join all_cons_columns acc ON acc.table_name = atc.table_name and
                             acc.column_name = atc.column_name and
                             acc.constraint_name = acPK.constraint_name
                  where atc.table_name = '${tableName.toUpperCase()}'
                  ${!isEmpty(schemaName) ? `AND atc.owner = '${schemaName.toUpperCase()}'` : ''}`

  

  logger.info(`${fName} Consultando meta data de la tabla: ${tableName}`)
  try {
    logger.info(`${fName} Ejecutando la consulta: ${statement}`)
    const request = await executeOrclString(header, statement, {}, {}, { logger })
    if (!request?.success) {
      const error = `${fName} Error en la consulta de meta data de la tabla ${tableName}: ${request?.error ?? 'Error desconocido'}`
      logger.error(`${fName} ${error}`)
      return { success: false, error }
    }

    return { success: true, data: request.data }
  } catch (err) {
    const error = `${fName} Error consultando meta data de la tabla ${tableName}`
    logger.error(`${fName} ${error}`)
    console.error(err)
    return { success: false, error, exception: err.message ?? err }
  } 
}

export const executeProcedure = async (
  { procedure, params, siifWebPoolName } = {},
  { logger = Logger } = {}
) => {
  const fName = '[executeProcedure]'

  if (isEmpty(procedure)) {
    const error = `${fName} No se ha enviado el procedimiento a ejecutar`
    logger.error(error)
    return { success: false, error }
  }

  // parsed params
  let parsedParams = {}

  if (!isEmpty(params)) {
    // validate params
    let errors = []
    Object.keys(params).forEach((key) => {
      let paramsKey = params[key] ?? {}

      if (!['IN', 'OUT'].includes(paramsKey.type)) {
        const error = `El parametro ${key} no tiene un tipo valido. Los tipos validos son 'IN' y 'OUT'`
        errors.push(error)
      }

      if (
        paramsKey.type === 'IN' &&
        (paramsKey.value === undefined || typeof paramsKey.value === 'undefined')
      ) {
        const error = `El parametro ${key} no tiene un valor valido`
        errors.push(error)
      }
    })

    if (!isEmpty(errors)) {
      logger.error(`${fName} ${errors.join(', ')}`)
      return { success: false, error: errors.join(', ') }
    }

    Object.keys(params).forEach((key) => {
      let paramsKey = params[key] ?? {}

      if (paramsKey.type === 'IN') {
        if (isDate(paramsKey.value)) {
          parsedParams[key] = { type: OracleDB.DATE, val: new Date(paramsKey.value) }
        } else {
          parsedParams[key] = paramsKey.value
        }
      }

      if (paramsKey.type === 'OUT') {
        parsedParams[key] = { dir: OracleDB.BIND_OUT, type: OracleDB.STRING }
      }
    })
  }

  const connection = (await getConnection({ siifWebPoolName })) ?? {}

  if (!connection?.success) {
    const error = 'No se ha podido establecer la conexion con la base de datos'
    logger.error(`${fName} ${error}`)
    return { success: false, error }
  }

  const { connection: oracleConector } = connection

  try {
    logger.info(`${fName} Ejecutando el procedimiento: ${procedure}`)

    const data = await oracleConector.execute(procedure, parsedParams, { autoCommit: true })

    return { success: true, data }
  } catch (err) {
    const error = `${fName} Error al ejecutar el procedimiento`
    logger.error(`${fName} ${error}`)
    console.error(err)
    return { success: false, error, exception: err.message ?? err }
  } finally {
    try {
      logger.info(`${fName} Cerrando la conexion con la base de datos...`)
      await oracleConector.close()
    } catch (err) {
      const error = 'Error al cerrar la conexion con la base de datos'
      logger.error(`${fName} ${error}`)
      console.error(err)
    }
  }
}

export const executeOrclString = async (header = {}, sql, bind = {}, options = {}, { logger = Logger } = {}) => {
    OracleDB.outFormat = OracleDB.OUT_FORMAT_OBJECT
    OracleDB.autoCommit = true
    let connection

    const functionName = `[executeOrclString]`
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
  }

/*
 * Funcion para obtener las estadisticas del pool de conexiones a la base de datos Oracle
 */
export const getPoolStats = ({ logger = Logger } = {}) => {
  const fName = '[getPoolStats]'

  if (isEmpty(pool)) {
    const error = 'Oracle pool not found'
    logger.error(`${fName} ${error}`)
    return { success: false, error }
  }

  // Extract the info from the Oracle pool
  const data = {
    status: pool.status,
    connectionsInUse: pool.connectionsInUse,
    connectionsOpen: pool.connectionsOpen,
    poolMax: pool.poolMax,
    poolMin: pool.poolMin,
    poolAlias: pool.poolAlias,
    connectTraceConfig: pool.connectTraceConfig,
    poolIncrement: pool.poolIncrement,
    poolPingInterval: pool.poolPingInterval,
    thin: pool.thin,
  }

  return { success: true, data }
}
