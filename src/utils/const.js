// Fake user para proceso de autenticacion
export const fakeUser = '00000000-0000-1000-9000-000fa7e015e9'

export const TIME_SLEEP_RETRY_CON = 200

/*
 * This is used to check if the provided UUID strings are valid.
 */
export const uuidPattern =
  '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
export const uuidRegex = new RegExp(uuidPattern)
