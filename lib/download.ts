import fs from 'fs-extra'
import path from 'path'
import readline from 'node:readline'
import { promisify } from 'util'
import pumpCb from 'pump'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import { isStopped } from './process.ts'
import { displayBytes, startProgress } from './utils.ts'

const pump = promisify(pumpCb) as (...streams: unknown[]) => Promise<void>

const structureetHeader = ['NumET', 'NumEJ', 'Rs', 'Rsl', 'crs', 'cd', 'nvoie', 'tvoie', 'lvoie', 'cvoie', 'ld', 'com', 'dep', 'lddep', 'lach', 'tel', 'telc', 'catet', 'lcatet', 'catag', 'lcatag', 'nsiret', 'ape', 'mft', 'lmft', 'sph', 'lsph', 'douverture', 'dautorisation', 'dms', 'uai']
const geolocalisationHeader = ['NumET', 'X', 'Y', 'src', 'dmaj']

export default async (processingConfig: ProcessingConfig, dir: string = 'data', axios: AxiosInstance, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  const url = processingConfig.url
  if (!url) throw new Error('L\'URL de la source des données n\'est pas renseignée')

  await log.step('Téléchargement du fichier source')
  await log.info(`Téléchargement depuis ${url}`)

  const file = path.join(dir, 'finess.csv')
  await fs.ensureFile(file)

  let res
  try {
    res = await axios.get(url, { responseType: 'stream', maxRedirects: 5 })
  } catch (err) {
    await log.error('Le fichier n\'existe pas, l\'URL a peut-être changé ou a été mal renseignée')
    throw err
  }

  // le flux source reste en pause tant que `pump` ne l'a pas branché : la progression
  // est lue sur le fichier de destination, sans risque de perdre des chunks
  const writeStream = fs.createWriteStream(file)
  const contentLength = Number(res.headers?.['content-length']) || 0
  const endDownloadProgress = await startProgress(log, 'Téléchargement', contentLength, () => writeStream.bytesWritten)
  try {
    await pump(res.data, writeStream)
  } finally {
    await endDownloadProgress()
  }

  const sourceSize = (await fs.stat(file)).size
  await log.info(`Fichier récupéré (${displayBytes(sourceSize)})`)

  if (isStopped()) return

  await log.step('Extraction établissements et géolocalisation')
  const fileStream = fs.createReadStream(file, { encoding: 'utf8' })
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  })

  const outStructureet = fs.createWriteStream(path.join(dir, 'structureet.csv'))
  outStructureet.write(structureetHeader.join(';') + '\n')
  const outGeolocalisation = fs.createWriteStream(path.join(dir, 'geolocalisation.csv'))
  outGeolocalisation.write(geolocalisationHeader.join(';') + '\n')

  let structureetLines = 0
  let geolocalisationLines = 0
  const endExtractionProgress = await startProgress(log, 'Extraction', sourceSize, () => fileStream.bytesRead)

  try {
    for await (const line of rl) {
      if (isStopped()) break
      if (line.startsWith('structureet;')) {
        outStructureet.write(line.slice('structureet;'.length).replace(/""""/g, '""') + '\n')
        structureetLines++
      } else if (line.startsWith('geolocalisation;')) {
        outGeolocalisation.write(line.slice('geolocalisation;'.length).replace(/""""/g, '""') + '\n')
        geolocalisationLines++
      }
    }
  } finally {
    await endExtractionProgress()
    rl.close()
    fileStream.destroy()
    await new Promise<void>((resolve, reject) => {
      outStructureet.end((err?: Error | null) => err ? reject(err) : resolve())
    })
    await new Promise<void>((resolve, reject) => {
      outGeolocalisation.end((err?: Error | null) => err ? reject(err) : resolve())
    })
  }

  if (isStopped()) return
  await log.info(`${structureetLines.toLocaleString()} établissements et ${geolocalisationLines.toLocaleString()} géolocalisations extraits`)
}
