import fs from 'fs-extra'
import path from 'path'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify'
import proj4 from 'proj4'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }

// Projections from https://epsg.io/
proj4.defs('EPSG:2154', '+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4559', '+proj=utm +zone=20 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:2972', '+proj=utm +zone=22 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:2975', '+proj=utm +zone=40 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4467', '+proj=utm +zone=21 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4471', '+proj=utm +zone=38 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4326', '+proj=longlat +datum=WGS84 +no_defs')

const dep2epsg: Record<string, string> = {
  metro: 'EPSG:2154',
  '9A': 'EPSG:4559',
  '9B': 'EPSG:4559',
  '9C': 'EPSG:2972',
  '9D': 'EPSG:2975',
  '9E': 'EPSG:4467',
  '9F': 'EPSG:4471'
}

const convertDep: Record<string, string> = {
  '9A': '971',
  '9B': '972',
  '9C': '973',
  '9D': '974',
  '9E': '975',
  '9F': '976'
}

/**
 * `readStream.pipe(parser)` ne propage pas les erreurs de la source : une lecture
 * impossible partirait en 'error' non géré et ferait tomber le worker.
 */
const readCsv = (file: string) => {
  const parser = parse({ delimiter: ';', columns: true })
  fs.createReadStream(file).on('error', (err) => parser.destroy(err)).pipe(parser)
  return parser
}

/**
 * Les libellés FINESS contiennent des guillemets d'usage ('LABM"BIOCEA"'). On les
 * conserve en normalisant seulement l'espacement, au lieu de supprimer les
 * caractères alentour comme le faisait l'ancienne expression régulière.
 */
export const normalizeQuotes = (value: string): string => {
  if (!value.includes('"')) return value
  let out = ''
  let open = false
  for (const char of value) {
    if (char === '"') {
      if (open) out = out.replace(/\s+$/, '')
      else if (out && !out.endsWith(' ')) out += ' '
      out += '"'
      open = !open
    } else if (char === ' ' && open && out.endsWith('"')) {
      continue
    } else {
      out += char
    }
  }
  return out.replace(/ {2,}/g, ' ').trim()
}

interface GeoCoordinates {
  x: string
  y: string
  src: string
  dmaj: string
}

let shouldBeStopped = false
export const setShouldBeStopped = (v: boolean) => { shouldBeStopped = v }
export const isStopped = (): boolean => shouldBeStopped

export default async (dir: string, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Fusion établissements et géolocalisation')

  const geoMap = new Map<string, GeoCoordinates>()
  const geoStream = readCsv(path.join(dir, 'geolocalisation.csv'))

  for await (const row of geoStream as AsyncIterable<{ NumET: string, X: string, Y: string, src: string, dmaj: string }>) {
    if (shouldBeStopped) break
    geoMap.set(row.NumET, {
      x: row.X,
      y: row.Y,
      src: (row.src || '').replace(/,/g, '-'),
      dmaj: row.dmaj || ''
    })
  }

  if (shouldBeStopped) return

  const columns = schema.map((field: { key: string }) => field.key)
  const numericFields = new Set(schema.filter((f: any) => f.type === 'integer' || f.type === 'number').map((f: any) => f.key))
  const outputFile = path.join(dir, 'etablissements_geolocalises.csv')
  const writeStream = fs.createWriteStream(outputFile)
  const stringifier = stringify({ header: true, columns, quoted_string: true })
  stringifier.pipe(writeStream)

  const structStream = readCsv(path.join(dir, 'structureet.csv'))
  let ignoredLines = 0

  try {
    for await (const rawRow of structStream as AsyncIterable<Record<string, string>>) {
      if (shouldBeStopped) break

      const row: Record<string, string | number> = {}
      for (const [key, value] of Object.entries(rawRow)) {
        const val = normalizeQuotes(value)
        if (val !== '' && numericFields.has(key)) {
          const num = Number(val)
          row[key] = isNaN(num) ? val : num
        } else {
          row[key] = val
        }
      }

      if (!row.NumET) {
        ignoredLines++
        continue
      }

      const numET = row.NumET
      const geo = geoMap.get(numET)

      if (geo && geo.x && geo.y) {
        const xNum = Number(geo.x)
        const yNum = Number(geo.y)
        if (!Number.isNaN(xNum) && !Number.isNaN(yNum)) {
          const epsg = dep2epsg[row.dep] || dep2epsg.metro
          const reproject = proj4(epsg, 'EPSG:4326', [xNum, yNum])
          row.lat = String(reproject[1])
          row.lon = String(reproject[0])
        }
      }

      if (geo) {
        row.src = geo.src
        row.dmaj = geo.dmaj
      }

      const dep = convertDep[row.dep] || row.dep
      row.dep = dep
      row.codeCom = (dep && dep.length > 2 ? dep.substring(0, 2) : dep || '') + (row.com || '')
      row.tel = row.tel ? row.tel.padStart(10, '0') : ''
      row.telc = row.telc ? row.telc.padStart(10, '0') : ''

      stringifier.write(row)
    }
  } finally {
    stringifier.end()
    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', resolve)
      writeStream.on('error', reject)
    })
  }

  if (shouldBeStopped) return
  if (ignoredLines > 0) {
    await log.warning(`${ignoredLines} ligne(s) du fichier source ont été ignorée(s) car l'identifiant 'NumET' était manquant.`)
  }
  await log.info('Fichier CSV créé')
}
