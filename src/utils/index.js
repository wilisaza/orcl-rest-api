import camelcase from 'camelcase'
import crypto from 'crypto'
import { DateTime } from 'luxon'
import logger from './logger.js'

const dateRegex =
  /^\d\d\d\d-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01]) (0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$/

/*
 * Fuction to check if the *arg* parameter is empty no matter what king of type
 * the parameter arg is
 */
export const isEmpty = (arg) => {
  let isEmpty = false
  if (
    typeof arg === 'boolean' ||
    arg instanceof Date ||
    arg instanceof DateTime ||
    arg instanceof Function
  ) {
    isEmpty = false
  } else if (!arg && typeof arg !== 'number') {
    isEmpty = true
  } else if (typeof arg === 'string' || Array.isArray(arg)) {
    isEmpty = arg.length === 0
  } else if (typeof arg === 'object') {
    isEmpty = Object.keys(arg).length === 0
  }
  return isEmpty
}

export const isNVL = (arg) => {
  return arg === null || arg === undefined || arg === ''
}

/**
 * circularReference - Function to detect circular references in an object
 * @param {*} obj
 * @returns
 */
export const stringifyWithCircularDetection = async (obj) => {
  const formattedResult = obj.rows.map(async (row) => {
    const formattedRow = {}
    for (let i = 0; i < obj.metaData.length; i++) {
      const meta = obj.metaData[i]
      let value = row[i]
      if (value && value.constructor.name === 'Lob') {
        value = (await value.getData(1, value.length)).toString()
      }

      if (value && value === 'true') {
        value = true
      }

      if (value && value === 'false') {
        value = false
      }

      formattedRow[meta.name.toLowerCase()] = value
    }
    return formattedRow
  })
  return Promise.all(formattedResult)
}

export const extractTableName = (sqlStatement) => {
  const match = sqlStatement.match(/\b(?:from|join|into|update|UPDATE)\s+(\w+\.)?(\w+)/i)
  return match ? (match[1] ? match[1] + match[2] : match[2]) : null
}

export const whereBuilder = (where) => {
  if (!Array.isArray(where)) {
    logger.error('La clausula WHERE debe ser un array de objetos')
    return ''
  }

  if (isEmpty(where)) {
    logger.info('La clausula WHERE esta vacia')
    return ''
  }

  let whereStatement = 'WHERE'

  where.forEach((obj) => {
    Object.keys(obj).forEach((key) => {
      if (key === 'logical' && logicalOperators.includes(obj[key])) {
        whereStatement += ` ${obj[key]}`
      } else {
        whereStatement += ` ${key}`
      }

      if (typeof obj[key] === 'object' && !Array.isArray(obj[key]) && obj[key] !== null) {
        whereStatement += recursiveBuilder(obj[key])
      }
    })
  })

  return whereStatement
}

const recursiveBuilder = (obj) => {
  let statement = ''

  Object.keys(obj).forEach((key) => {
    if (Object.keys(operators).includes(key)) {
      if (typeof obj[key] === 'string' && dateRegex.test(obj[key])) {
        statement += `${operators[key]} TO_DATE('${obj[key]}', 'YYYY-MM-DD HH24:MI:SS')`
      }

      if (typeof obj[key] === 'string' && !dateRegex.test(obj[key])) {
        statement += ` ${operators[key]} '${obj[key]}'`
      }

      if (typeof obj[key] === 'number') {
        statement += ` ${operators[key]} ${obj[key]}`
      }
    }
  })
  return statement
}

const operators = {
  eq: '=',
  ne: '!=',
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
  like: 'LIKE',
  in: 'IN',
  nin: 'NOT IN',
}

const logicalOperators = ['AND', 'OR']
/**
 * insertStatementBuilder - Funcion para construir el statement de insert
 * @param {data} - Valores que se pasaron en el body para su inserccion
 * @param {metaData} - Metadata de la tabla a la que se le quiere hacer la inserccion
 */
export const insertStatementBuilder = ({ metaData, data } = {}) => {
  const columns = Object.keys(data)

  // Validar que los valores que se pasaron en el body sean del tipo correcto
  const errors = validateData({ data, metaData })

  if (!isEmpty(errors)) {
    const error = `Se encontraron los siguientes errores en los valores que se pasaron en el body: * ${errors.join(', * ')}`
    logger.error(error)
    return { success: false, error }
  }

  const parseData = parseDataToOracle(data)

  // construir el statement
  let statement = ` (${columns.join(', ')}) VALUES (${parseData.join(', ')})`

  return { success: true, statement }
}

/**
 * updateStatementBuilder - Funcion para construir el statement de update
 * @param {data} - Valores que se pasaron en el body para su actualizacion
 * @param {metaData} - Metadata de la tabla a la que se le quiere hacer la actualizacion
 */
export const updateStatementBuilder = ({ metaData, data } = {}) => {
  const columns = Object.keys(data)

  // Validar que los valores que se pasaron en el body sean del tipo correcto
  const errors = validateData({ data, metaData })

  const updateErrors = updateValidateData({ data, metaData })

  let error = 'Se encontraron los siguientes errores en los valores que se pasaron en el body:'
  if (!isEmpty(errors)) {
    error += ` * ${errors.join(', * ')}`
    logger.error(error)
    return { success: false, error }
  }

  if (!isEmpty(updateErrors)) {
    error += ` * ${updateErrors.join(', * ')}`
    logger.error(error)
    return { success: false, error }
  }

  const parseData = parseDataToOracle(data)

  // construir el statement
  let statement = ` SET ${columns.map((col, index) => `${col} = ${parseData[index]}`).join(', ')} `

  return { success: true, statement }
}

/**
 * updateValidateData - Funcion para validar que los valores no se puedan actualizar si son llaves primarias
 * @param {data} - Valores que se pasaron en el body para su actualizacion
 * @param {metaData} - Metadata de la tabla a la que se le quiere hacer la actualizacion
 */
const updateValidateData = ({ data, metaData } = {}) => {
  // Obtener llaves que pasaron en el body
  const columns = Object.keys(data)

  // Filtrar la metadata para obtener solo la informacion de las columnas que se pasaron en el body
  let info = metaData.filter((obj) => columns.includes(obj?.column_name.toLowerCase()))

  let errors = []

  // Validar que los valores que se pasaron en el body sean del tipo correcto
  info.forEach((obj) => {
    if (obj.is_pk) {
      const error = `La columna ${obj?.column_name} no se puede actualizar debido a que es llave primaria`
      errors.push(error)
    }
  })

  return errors
}

/**
 * validateData - Funcion para validar que los valores que se pasaron en el body sean del tipo y longitud correcta
 * @param {data} - Valores que se pasaron en el body para su inserccion/ actualizacion
 * @param {metaData} - Metadata de la tabla a la que se le quiere hacer la inserccion/ actualizacion
 */
const validateData = ({ data, metaData } = {}) => {
  // Obtener llaves que pasaron en el body
  const columns = Object.keys(data)

  // Filtrar la metadata para obtener solo la informacion de las columnas que se pasaron en el body
  let info = metaData.filter((obj) => columns.includes(obj?.column_name.toLowerCase()))

  let errors = []

  // Validar que los valores que se pasaron en el body sean del tipo correcto
  info.forEach((obj) => {
    const column = obj.column_name?.toLowerCase()

    if (obj?.data_type === 'NUMBER' && typeof data[column] !== 'number') {
      const error = `El valor de la columna '${obj?.column_name}' debe ser un número y se recibió '${typeof data[column]}'`
      errors.push(error)
    }

    if (obj?.data_type === 'VARCHAR2' && typeof data[column] !== 'string') {
      const error = `El valor de la columna '${obj?.column_name}' debe ser un string y se recibió '${typeof data[column]}'`
      errors.push(error)
    }

    if (obj?.data_type === 'DATE' && !dateRegex.test(data[column])) {
      const error = `El valor de la columna '${obj?.column_name}' debe ser un string con una fecha apropiada(YYYY-MM-DD HH:MM:SS) y se recibió '${data[column]}'`
      errors.push(error)
    }

    if (
      !isEmpty(obj?.data_precision) &&
      !isEmpty(data[column]) &&
      obj?.data_precision < data[column].toString().length
    ) {
      const error = `El valor de la columna ${obj?.column_name} excede la longitud máxima permitida (${obj?.data_precision})`
      errors.push(error)
    }

    if (
      !isEmpty(obj?.data_length) &&
      !isEmpty(data[column]) &&
      obj?.data_length < data[column].toString().length &&
      obj?.data_type !== 'DATE'
    ) {
      const error = `El valor de la columna ${obj?.column_name} supera la longitud máxima permitida (${obj?.data_length})`
      errors.push(error)
    }
  })

  return errors
}

/**
 * parseDataToOracle - Funcion para parsear los valores que se pasaron en el body a un formato que Oracle pueda entender
 * @param {data} - Valores que se pasaron en el body para su inserccion/ actualizacion
 */
const parseDataToOracle = (data) => {
  // Si no se pasaron valores en el body
  if (isEmpty(data)) {
    return []
  }

  // Obtener llaves que pasaron en el body
  const columns = Object.keys(data)

  let parseData = columns.map((obj) => {
    if (typeof data[obj] === 'string' && dateRegex.test(data[obj])) {
      return `TO_DATE('${data[obj]}', 'YYYY-MM-DD HH24:MI:SS')`
    }

    if (typeof data[obj] === 'string') {
      return `'${data[obj]}'`
    }
    return data[obj]
  })

  return parseData
}

// Funcion para encryptar la contraseña
export const hashPassword = (password) => {
  if (isEmpty(password)) {
    return ''
  }

  return crypto.createHash('sha1').update(password).digest('hex')
}

// Funcion para convertir un string a camelCase
export const turnToCamelCase = (string) => {
  if (isEmpty(string) || typeof string !== 'string') {
    return string
  }

  return camelcase(string)
}

export const isDate = (date) => {
  return dateRegex.test(date)
}

export const TIME_SLEEP_RETRY_CON = 200

/**
 * Funcion para dormir un proceso por un tiempo determinado
 * @param {*} ms
 * @returns
 */
export const sleep = (ms = 100) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/*
 * Funcion para verificar si un ID es valido.
 */
export const isRowId = (id) => {
  const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
  return !isEmpty(id) && regex.test(id)
}

/*
 * Funcion para verificar si un usuario tiene acceso a los datos de una compañia.
 */
export const hasAccessToCompany = (user, idCompany) => {
  let resp = false
  if (Array.isArray(user?.companies) && isRowId(idCompany)) {
    resp = user.companies.some((company) => company.companyId === idCompany)
  }
  return resp
}

/*
 * Funcion para verificar si un usuario esta activo.
 */
export const isActiveUser = (user) => {
  return isRowId(user?.id) && user?.isActive
}

/*
 * Funcion para verificar si el argumento es un arreglo, si no lo es, lo convierte en uno con un solo
 * elemento.
 */
export const toArray = (arg) => {
  if (arg === undefined || arg === null) {
    return []
  }
  return Array.isArray(arg) ? arg : [arg]
}

// Formatos de fecha que se van a validar
const dateFormats = ['yyyy-mm-dd', 'dd/mm/yyyy', 'mm/dd/yyyy', 'dd-mm-yyyy', 'mm-dd-yyyy']

// Validar si una fecha es valida
export function isValidDate(dateString) {
  let validacion = false
  dateFormats.forEach((formato) => {
    const dt = DateTime.fromFormat(dateString, formato)
    if (dt.isValid) {
      validacion = true
      return
    }
  })
  return validacion
}

// Funcion para convertir una fecha en un formato estandar YYYY-MM-DD
export const toValidDate = (dateString) => {
  if (isEmpty(dateString)) {
    return null
  }

  if (dateString.includes('/')) {
    const [day, month, year] = dateString.split('/')

    let wellFormatedDate = `${year}-${month}-${day}`
    if (parseInt(month) > 12) {
      wellFormatedDate = `${year}-${day}-${month}`
    }

    return wellFormatedDate
  } else if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
    const [year, month, day] = dateString.split('-')

    let wellFormatedDate = `${year}-${month}-${day}`
    if (parseInt(month) > 12) {
      wellFormatedDate = `${year}-${day}-${month}`
    }

    return dateString
  } else if (/^\d{2}-\d{2}-\d{4}$/.test(dateString)) {
    const [day, month, year] = dateString.split('-')

    let wellFormatedDate = `${year}-${month}-${day}`
    if (parseInt(month) > 12) {
      wellFormatedDate = `${year}-${day}-${month}`
    }

    return dateString
  } else return null
}
