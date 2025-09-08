import 'dotenv/config'
import express from 'express'
import { corsMiddleware } from './middleware/cors.js'
import { initOraclePool } from './db/index.js'
import orclRoutes from './routes/index.js'
import logger from './utils/logger.js'

const app = express()
const PORT = process.env.PORT || 3000

//app.use(express.json())
app.use(corsMiddleware)

app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf
    },
  })
)
app.use(
  express.urlencoded({
    extended: false,
    verify: (req, res, buf) => {
      req.rawBody = buf
    },
  })
)
app.use(
  express.raw({
    extended: false,
    verify: (req, res, buf) => {
      req.rawBody = buf
    },
  })
)

app.use('/orcl', orclRoutes)

// healthCheck for AWS ELB
app.get('/healthCheck', function (req, res) {
  logger.info('Health check requested')
  res.status(200).json({
    success: true,
    message: 'Service Running',
  })
})

app.get('/', function (req, res) {
  res.status(404).json({ error: 'Not Authorized X' })
})

app.use(function (req, res) {
  res.status(404).json({ error: 'Not Authorized' })
})
app.use(function (err, req, res) {
  console.error(err.stack)
  res.status(500).json({ error: 'Server Error' })
})

app.listen(PORT, async() => {
  await initOraclePool()
  logger.info(`Server listening on port:${PORT}`)
})

const processTerminate = async (signal) => {
  logger.info(`Received ${signal}. Terminating the process`)
  await closePoolAndExit()
  process.exit(0)
}

const handleSignal = (signal) => {
  processTerminate(signal).catch((err) => {
    logger.error('Error during shutdown', err)
    process.exit(1)
  })
}

process.once('SIGTERM', handleSignal).once('SIGINT', handleSignal)
