import { Router } from 'express'

import { orclCtrl } from '../controllers/index.js'

const router = Router()

router.get('/count/:object', orclCtrl.getCountObject)

router.get('/max/:object/:field', orclCtrl.getMaxObject)

export default router
