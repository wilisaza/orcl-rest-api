import { sentences } from './crudOrclSentences.js'
import { functions } from './crudOrclFunctions.js'
import { executeFunctions } from './crudOrclExecuteFunctions.js'

let connection

let resWords = ['pags', 'offset', 'numrows', 'orderby'] //Array de palabras reservadas para ser excluidas del WHERE

export const orclApi = {
  async getAll(table, header) {
    const sql = `SELECT * FROM ${table}`
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async getCount(table, where, header) {
    const sql = sentences.filterStringCount(table, where, resWords)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async getMax(table, field, where, header) {
    const sql = sentences.filterStringMax(table, field, where, resWords)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async getFiltered(table, where, header) {
    const sql = functions.paginationString(
      sentences.filterString(table, where, resWords),
      connection,
      where
    )
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async getFunction(nomFunction, params, header) {
    const sql = sentences.functionString(nomFunction, params)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async getCustomSelect(table, field, where, header) {
    const sql = functions.paginationString(
      sentences.customSelectString(table, field, where, resWords),
      connection,
      where
    )
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async executeQuery(data, header) {
    const sql = data.query ?? "SELECT 'No query' FROM dual"
    const bind = data.bind ?? {}
    const options = data.options ?? {}
    return await executeFunctions.executeOrclString(header, sql, bind, options)
  },

  async executeTransaction(table, data, header) {
    return await executeFunctions.executeOrclTransaction(table, header, data)
  },

  async insertOne(table, data, header) {
    const sql = sentences.insertString(table, data)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async updateFiltered(table, data, where, header) {
    const sql = sentences.updateString(table, data, where)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async deleteFiltered(table, where, header) {
    const sql = sentences.deleteString(table, where)
    return executeFunctions.executeOrclString(header, sql, {})
  },

  async postProcedure(nomProcedure, params, header) {
    const sql = sentences.procedureString(nomProcedure, params)
    const bindVars = sentences.procedureBind(params)
    return executeFunctions.executeOrclString(header, sql, bindVars)
  },
}
