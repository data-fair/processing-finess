const fs = require('fs-extra')
const path = require('path')
const parse = require('csv-parser')
const jsoncsv = require('json-2-csv')

const proj4 = require('proj4')
// This projections come from https://epsg.io/
proj4.defs('EPSG:2154', '+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4559', '+proj=utm +zone=20 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:2972', '+proj=utm +zone=22 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:2975', '+proj=utm +zone=40 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4467', '+proj=utm +zone=21 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4471', '+proj=utm +zone=38 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
proj4.defs('EPSG:4326', '+proj=longlat +datum=WGS84 +no_defs')

const dep2epsg = {
  metro: 'EPSG:2154',
  '9A': 'EPSG:4559',
  '9B': 'EPSG:4559',
  '9C': 'EPSG:2972',
  '9D': 'EPSG:2975',
  '9E': 'EPSG:4467',
  '9F': 'EPSG:4471'
}

module.exports = async (dir, log) => {
  try {
    await log.step('Merge etablissements et geolocation ')
    const readStreamG = fs.createReadStream(path.join(dir, 'geolocalisation.csv'))
    const csvParserG = parse({ separator: ';' })
    let etablissementsG = []
    for await (const row of readStreamG.pipe(csvParserG)) {
      etablissementsG[row.NumET] = etablissementsG[row.NumET] || {}
      for (let [key, value] of Object.entries(row)) {
        if (key === 'src') {
          value = value.replace(/,/g, '-')
        }
        etablissementsG[row.NumET][key] = value
      }
    }
    const readStream = fs.createReadStream(path.join(dir, 'structureet.csv'))
    const csvParser = parse({ separator: ';' })
    const convertDep = {
      '9A': '971',
      '9B': '972',
      '9C': '973',
      '9D': '974',
      '9E': '975',
      '9F': '976'
    }

    for await (const row of readStream.pipe(csvParser)) {
      const reproject = proj4(dep2epsg[row.dep] || dep2epsg.metro, 'EPSG:4326', [Number(etablissementsG[row.NumET].X), Number(etablissementsG[row.NumET].Y)]).map(c => c + '')
      row.lat = reproject[1]
      row.lon = reproject[0]
      row.dep = (convertDep[row.dep] ? convertDep[row.dep] : row.dep)
      row.codeCom = (row.dep.length > 2 ? row.dep.substring(0, 2) : row.dep) + row.com
      row.tel = `${row.tel.padStart(10, '0')}`
      row.telc = `${row.telc.padStart(10, '0')}`
      delete row.com
      delete etablissementsG[row.NumET].Y
      delete etablissementsG[row.NumET].X
      for (let [key, value] of Object.entries(row)) {
        value = value.replace(/([a-zA-Z\s])"([a-zA-Z\s])/g, '')
        etablissementsG[row.NumET][key] = value
      }
    }
    etablissementsG = Object.values(etablissementsG)
    const csv = await jsoncsv.json2csv(etablissementsG)
    await fs.writeFile(path.join(dir, 'etablissements_geolocalises.csv'), csv)
    await log.info('Fichier csv créé')
  } catch (err) {
    log.error(err)
  }
}
