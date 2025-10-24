import { Router } from 'express'

import { orclCtrl } from '../controllers/index.js'

const router = Router()

router.get('/count/:object', orclCtrl.getCountObject)

router.get('/max/:object/:field', orclCtrl.getMaxObject)

router.get('/:object', orclCtrl.getAllObjects)

router.post('/custom/:object', orclCtrl.postCustomObjects)

export default router
