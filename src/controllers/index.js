import { valKeys } from '../utils/arrayFunctions.js'
import logger from '../utils/logger.js'
import { executeOrclString, tableMetaData } from '../db/index.js'
import { isEmpty } from '../utils/index.js'

let resWords = ['pags', 'offset', 'numrows', 'orderby'] //Array de palabras reservadas para ser excluidas del WHERE

const libName = '[controllers/index.js]'
export const orclCtrl = {
  getCountObject: async (req, res) => {
    const functionName = `${libName} [getCountObject]`
    const requiredFields = ['object']
    let err = {}
    if (!valKeys(req.params, requiredFields, err)) {
      const error = `${functionName} - Error al validar params: ${JSON.stringify(err)}`
      logger.error(error)
      return res.status(400).json({ success: false, error })
    }

    try {
      const table = req.params.object

      // Validar que la tabla exista consultando los metadatos
      const meta = await tableMetaData(req.headers, { tableName: table })
      if (!meta.success || isEmpty(meta.data)) {
        const error = `${functionName} - Tabla o vista no válida o no tiene metadatos: ${table}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      const validColumns = meta.data.map((c) => c.column_name.toLowerCase())

      const binds = {}
      let whereClauses = []

      // Construcción segura de la cláusula WHERE
      for (const key in req.query) {
        if (
          Object.prototype.hasOwnProperty.call(req.query, key) &&
          !resWords.includes(key.toLowerCase())
        ) {
          // Validar que la columna exista en la tabla
          if (validColumns.includes(key.toLowerCase())) {
            whereClauses.push(`${key.toUpperCase()} = :${key}`)
            binds[key] = req.query[key]
          } else {
            logger.warn(`${functionName} - Se ignoró el parámetro de consulta no válido: ${key}`)
          }
        }
      }

      let sql = `SELECT COUNT(*) AS COUNT FROM ${table}`
      if (whereClauses.length > 0) {
        sql += ` WHERE ${whereClauses.join(' AND ')}`
      }

      const outData = await executeOrclString(req.headers, sql, binds, {})
      if (outData.success === false) {
        const error = `${functionName} - Error en la consulta: ${JSON.stringify(outData.error)}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }

      logger.info(`${functionName} - Consulta exitosa`)
      return res.status(200).json(outData)
    } catch (error) {
      const catchError = `${functionName} - ${error.message}`
      logger.error(catchError)
      return res.status(500).json({ success: false, error: error.message })
    }
  },

  getMaxObject: async (req, res) => {
    const functionName = `${libName} [getMaxObject]`
    const requiredFields = ['object', 'field']
    let err = {}
    if (!valKeys(req.params, requiredFields, err)) {
      const error = `${functionName} - Error al validar params: ${JSON.stringify(err)}`
      logger.error(error)
      return res.status(400).json({ success: false, error })
    }
    try {
      const { object: table, field } = req.params

      // Validar que la tabla exista consultando los metadatos
      const meta = await tableMetaData(req.headers, { tableName: table })
      if (!meta.success || isEmpty(meta.data)) {
        const error = `${functionName} - Tabla o vista no válida o no tiene metadatos: ${table}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      const validColumns = meta.data.map((c) => c.column_name.toLowerCase())

      // Validar el campo para MAX()
      if (!validColumns.includes(field.toLowerCase())) {
        const error = `${functionName} - Campo no válido para MAX(): ${field}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }

      const binds = {}
      let whereClauses = []

      // Construcción segura de la cláusula WHERE
      for (const key in req.query) {
        if (
          Object.prototype.hasOwnProperty.call(req.query, key) &&
          !resWords.includes(key.toLowerCase())
        ) {
          // Validar que la columna exista en la tabla
          if (validColumns.includes(key.toLowerCase())) {
            whereClauses.push(`${key.toUpperCase()} = :${key}`)
            binds[key] = req.query[key]
          } else {
            logger.warn(`${functionName} - Se ignoró el parámetro de consulta no válido: ${key}`)
          }
        }
      }

      let sql = `SELECT NVL(MAX(${field}), 0) AS MAX FROM ${table}`
      if (whereClauses.length > 0) {
        sql += ` WHERE ${whereClauses.join(' AND ')}`
      }

      const outData = await executeOrclString(req.headers, sql, binds, {})
      if (outData.success === false) {
        const error = `${functionName} - Error en la consulta: ${JSON.stringify(outData.error)}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }

      logger.info(`${functionName} - Consulta exitosa`)
      return res.status(200).json(outData)
    } catch (error) {
      const catchError = `${functionName} - ${error.message}`
      logger.error(catchError)
      return res.status(500).json({ success: false, error: error.message })
    }
  },

  getAllObjects: async (req, res) => {
    const functionName = `${libName} [getAllObjects]`
    const requiredFields = ['object']
    let err = {}
    if (!valKeys(req.params, requiredFields, err)) {
      const error = `${functionName} - Error al validar params: ${JSON.stringify(err)}`
      logger.error(error)
      return res.status(400).json({ success: false, error })
    }
    try {
      const table = req.params.object

      // Validar que la tabla exista consultando los metadatos
      const meta = await tableMetaData(req.headers, { tableName: table })
      if (!meta.success || isEmpty(meta.data)) {
        const error = `${functionName} - Tabla o vista no válida o no tiene metadatos: ${table}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      
      const validColumns = meta.data.map((c) => c.column_name.toLowerCase())

      const binds = {}
      let whereClauses = []

      // Construcción segura de la cláusula WHERE
      for (const key in req.query) {
        if (
          Object.prototype.hasOwnProperty.call(req.query, key) &&
          !resWords.includes(key.toLowerCase())
        ) {
          // Validar que la columna exista en la tabla
          if (validColumns.includes(key.toLowerCase())) {
            whereClauses.push(`${key.toUpperCase()} = :${key}`)
            binds[key] = req.query[key]
          } else {
            logger.warn(`${functionName} - Se ignoró el parámetro de consulta no válido: ${key}`)
          }
        }
      }

      let sql = `SELECT * FROM ${table}`
      if (whereClauses.length > 0) {
        sql += ` WHERE ${whereClauses.join(' AND ')}`
      }

      // Lógica de paginación y ordenamiento segura
      if (req.query.orderby) {
        const orderByColumn = req.query.orderby.split(' ')[0]
        // Validar que la columna de ordenamiento exista
        if (validColumns.includes(orderByColumn.toLowerCase())) {
          const sortOrder = req.query.orderby.toLowerCase().endsWith(' desc') ? ' DESC' : ' ASC'
          sql += ` ORDER BY ${orderByColumn}${sortOrder}`
        } else {
          logger.warn(`${functionName} - Se ignoró el parámetro de ordenamiento no válido: ${req.query.orderby}`)
        }
      }
      if (req.query.pags === 'S') {
        sql += ` OFFSET :offset ROWS FETCH NEXT :numrows ROWS ONLY`
        binds.offset = parseInt(req.query.offset, 10) || 0
        binds.numrows = parseInt(req.query.numrows, 10) || 10
      }

      const outData = await executeOrclString(req.headers, sql, binds, {})
      if (outData.success === false) {
        const error = `${functionName} - Error en la consulta: ${JSON.stringify(outData.error)}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      logger.info(`${functionName} - Consulta exitosa`)
      return res.status(200).json(outData)
    } catch (error) {
      const catchError = `${functionName} - ${error.message}`
      logger.error(catchError)
      return res.status(500).json({ success: false, error: error.message })
    }
  },

  postCustomObjects: async (req, res) => {
    const functionName = `${libName} [postCustomObjects]`
    const requiredFields = ['object']
    let err = {}
    if (!valKeys(req.params, requiredFields, err)) {
      const error = `${functionName} - Error al validar params: ${JSON.stringify(err)}`
      logger.error(error)
      return res.status(400).json({ success: false, error })
    }

    if (!req.body.fields || typeof req.body.fields !== 'object' || Array.isArray(req.body.fields)) {
      const error = `${functionName} - Error al validar body: Se requiere un objeto 'fields'`
      logger.error(error)
      return res.status(400).json({ success: false, error })
    }
    try {
      const table = req.params.object
      const fields = Object.keys(req.body.fields)

      if (fields.length === 0) {
        const error = `${functionName} - El array 'fields' no puede estar vacío`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      // Validar que la tabla exista consultando los metadatos
      const meta = await tableMetaData(req.headers, { tableName: table })
      if (!meta.success || isEmpty(meta.data)) {
        const error = `${functionName} - Tabla o vista no válida o no tiene metadatos: ${table}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      const validColumns = meta.data.map((c) => c.column_name.toLowerCase())

      // Validar los campos solicitados
      const invalidFields = fields.filter(f => !validColumns.includes(f.trim().toLowerCase()))
      if (invalidFields.length > 0) {
        const error = `${functionName} - Campos no válidos para seleccionar: ${invalidFields.join(', ')}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }

      const binds = {}
      let whereClauses = []

      // Construcción segura de la cláusula WHERE
      for (const key in req.query) {
        if (
          Object.prototype.hasOwnProperty.call(req.query, key) &&
          !resWords.includes(key.toLowerCase())
        ) {
          // Validar que la columna exista en la tabla
          if (validColumns.includes(key.toLowerCase())) {
            whereClauses.push(`${key.toUpperCase()} = :${key}`)
            binds[key] = req.query[key]
          } else {
            logger.warn(`${functionName} - Se ignoró el parámetro de consulta no válido: ${key}`)
          }
        }
      }

      const validFields = fields.filter(f => validColumns.includes(f.trim().toLowerCase()))
      let sql = `SELECT ${validFields.join(', ')} FROM ${table}`
      if (whereClauses.length > 0) {
        sql += ` WHERE ${whereClauses.join(' AND ')}`
      }

      const outData = await executeOrclString(req.headers, sql, binds, {})
      if (outData.success === false) {
        const error = `${functionName} - Error en la consulta: ${JSON.stringify(outData.error)}`
        logger.error(error)
        return res.status(400).json({ success: false, error })
      }
      logger.info(`${functionName} - Consulta exitosa`)
      return res.status(200).json(outData)
    } catch (error) {
      const catchError = `${functionName} - ${error.message}`
      logger.error(catchError)
      return res.status(500).json({ success: false, error: error.message })
    }
  },
}
