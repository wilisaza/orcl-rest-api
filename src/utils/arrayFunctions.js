import logger from '../utils/logger.js'

const libName = '[arrayFunctions]'

export const keyExists = (obj, key) => {
  return !!obj && typeof obj === 'object' && typeof obj[key] !== 'undefined'
}

export const valKeys = (obj, key, err) => {
  const fName = `${libName} [valKeys]`
  let error = ''

  if (typeof obj !== 'object'){
    error = 'No object'
    logger.error(`${fName} ${error}`)
    err['1'] = error
    return false
  }
  let countError = 0
  for (let i in key) {
    if (!keyExists(obj, key[i])) {
      countError++
      err[countError] = `No ${key[i]}`
      error += `No ${key[i]} - `
    }
  }
  if (countError) {
    logger.error(`${fName} ${error}`)
    return false
  } 
  logger.info(`${fName} OK`)
  return true
}

export const objCompare = (obj1, obj2) => {
  const fName = `${libName} [objCompare]`
  if(!obj1){
    const error = 'obj es indefinido o null'
    logger.error(`${fName} ${error}`)
    return {
      success:false,
      error
    }
  }
  
  if(!obj2){
    const error = 'obj es indefinido o null'
    logger.error(`${fName} ${error}`)
    return {
      success:false,
      error
    }
  }
  const Obj1_keys = Object.keys(obj1)
  const Obj2_keys = Object.keys(obj2)
  if (Obj1_keys.length !== Obj2_keys.length) {
    return false
  }
  for (let k of Obj1_keys) {
    if (obj1[k] !== obj2[k]) {
      const error = 'objetos son diferentes'
      logger.error(`${fName} ${error}`)
      return false
    }
  }
  logger.info(`${fName} OK`)
  return true
}
