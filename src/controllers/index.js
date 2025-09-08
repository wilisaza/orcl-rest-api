import { orclApi } from '../api/crudOrclApi.js'
import { sentences } from '../api/crudOrclSentences.js'
import { functions } from '../api/crudOrclFunctions.js'
import { executeFunctions } from '../api/crudOrclExecuteFunctions.js'
import { valKeys } from '../utils/arrayFunctions.js'
import logger from '../utils/logger.js'

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
      const sql = sentences.filterStringCount(req.params.object, req.query, resWords)
      const outData = await executeFunctions.executeOrclString(req.headers, sql, {})
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
      const sql = sentences.filterStringMax(req.params.object, req.params.field, req.query, resWords)
      const outData = await executeFunctions.executeOrclString(req.headers, sql, {})
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
  }
}
