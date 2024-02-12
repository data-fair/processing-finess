const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const iconv = require('iconv-lite')
const { Transform } = require('stream')
const byline = require('byline')
const decode = iconv.decodeStream('utf-8')

const structureetHeader = ['NumET', 'NumEJ', 'Rs', 'Rsl', 'crs', 'cd', 'nvoie', 'tvoie', 'lvoie', 'cvoie', 'ld', 'com', 'dep', 'lddep', 'lach', 'tel', 'telc', 'catet', 'lcatet', 'catag', 'lcatag', 'nsiret', 'ape', 'mft', 'lmft', 'sph', 'lsph', 'douverture', 'dautorisation', 'dms', 'uai']
const geolocalisationHeader = ['NumET', 'X', 'Y', 'src', 'dmaj']

module.exports = async (processingConfig, dir = 'data', axios, log) => {
  let res
  const fileName = 'finess.csv'
  const file = path.join(dir, fileName)
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(file)
  try {
    res = await axios.get(processingConfig.url, { responseType: 'stream', maxRedirects: 1 })
  } catch (err) {
    await log.error('Le fichier n\'existe pas, L\'url a peut-être changé ou a été mal renseigné')
  }

  await log.info(`Récupération du fichier ${file}`)
  await pump(res.data.pipe(decode), fs.createWriteStream(file))
  await log.info(`Fichier récupéré dans ${file}`)

  log.step('Extraction etablissements et geolocation')
  const readFile = fs.createReadStream(path.join(dir, 'finess.csv'))
  const decodeInputStream = readFile.pipe(byline())

  const outStructureet = fs.createWriteStream(path.join(dir, 'structureet.csv'))
  outStructureet.write(structureetHeader.join(';') + '\n')
  const outGeolocalisation = fs.createWriteStream(path.join(dir, 'geolocalisation.csv'))
  outGeolocalisation.write(geolocalisationHeader.join(';') + '\n')

  await pump(
    decodeInputStream,
    new Transform({
      objectMode: true,
      async transform (chunk, _, next) {
        const item = chunk.toString()
        const items = item.split(';').map(l => {
          const s = l.replace(/""""/g, '""')
          return s
        })
        const firstColumn = items.shift()
        if (firstColumn === 'structureet') {
          outStructureet.write(items.join(';') + '\n')
        } else if (firstColumn === 'geolocalisation') {
          outGeolocalisation.write(items.join(';') + '\n')
        }
        next()
      }
    })
  )
}
