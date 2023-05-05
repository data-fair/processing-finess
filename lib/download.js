const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const iconv = require('iconv-lite')
const { Transform } = require('stream')
const byline = require('byline')
const join = require('./join')
// const streamToPromise = require('stream-to-promise')
const decode = iconv.decodeStream('utf-8')

const split = new Transform({
  objectMode: true,
  async transform (chunk, encoding) {
    const item = chunk.toString() // convert buffer to string
    const items = item.split(';').map(l => {
      const s = l.replace(/""""/g, '""')
      return s
    })
    return items
  }
})

const structureetHeader = ['NumET', 'NumEJ', 'Rs', 'Rsl', 'crs', 'cd', 'nvoie', 'tvoie', 'lvoie', 'cvoie', 'ld', 'com', 'dep', 'lddep', 'lach', 'tel', 'telc', 'catet', 'lcatet', 'catag', 'lcatag', 'nsiret', 'ape', 'mft', 'lmft', 'sph', 'lsph', 'douverture', 'dautorisation', 'dms', 'uai']

const structureetFilter = new Transform({
  objectMode: true,
  async transform (item, encoding) {
    if (item[0] === 'structureet') {
      return item.slice(1)
    } else {
      return null
    }
  }
})

const geolocalisationHeader = ['NumET', 'X', 'Y', 'src', 'dmaj']

const geolocalisationFilter = new Transform({
  objectMode: true,
  async transform (item, encoding) {
    if (item[0] === 'geolocalisation') {
      return item.slice(1)
    } else {
      return null
    }
  }
})

module.exports = async (processingConfig, dir = 'data', axios, log) => {
  let res
  try {
    res = await axios.get(processingConfig.url, { responseType: 'stream' })
  } catch (err) {
    if (err.status === 404) {
      await log.warning('Le fichier n\'existe pas')
      return
    }
    throw err
  }
  const fileName = 'finess' + '.csv'

  const file = `${dir}/${fileName}`
  if (await fs.pathExists(file)) {
    await log.warning(`Le fichier ${file} existe déjà`)
  } else {
    // creating empty file before streaming seems to fix some weird bugs with NFS
    await fs.ensureFile(file)
    await log.info(`Récupération du fichier ${file}`)
    await pump(res.data.pipe(decode), fs.createWriteStream(file))
    await log.info(`Fichier récupéré dans ${file}`)
  }
  log.step('Extracting etablissements and geolocation files')
  const readFile = fs.createReadStream(path.join(dir, 'finess.csv'))
  const decodeInput = readFile.pipe(byline()).pipe(split)

  const out1 = fs.createWriteStream(path.join(dir, 'structureet.csv'))
  const out2 = fs.createWriteStream(path.join(dir, 'geolocalisation.csv'))
  await new Promise((resolve, reject) => {
    decodeInput.on('end', resolve)
    decodeInput.on('error', reject)

    decodeInput.pipe(structureetFilter).pipe(join(structureetHeader)).pipe(out1)
    decodeInput.pipe(geolocalisationFilter).pipe(join(geolocalisationHeader)).pipe(out2)
  })
}
