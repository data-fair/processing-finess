import { describe, it, before, after } from 'node:test'
import assert from 'assert'
import fs from 'fs-extra'
import path from 'path'
import os from 'os'
import { Readable } from 'stream'
import { parse } from 'csv-parse/sync'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }
import schema from '../lib/schema.json' with { type: 'json' }
import download from '../lib/download.ts'
import processFiles, { normalizeQuotes, setShouldBeStopped } from '../lib/process.ts'
import { run, stop } from '../lib/execute.ts'

/** Une ligne `structureet` du fichier source, dans l'ordre attendu par l'extraction. */
const structureet = (fields: Record<string, string>) => ['structureet', ...[
  'NumET', 'NumEJ', 'Rs', 'Rsl', 'crs', 'cd', 'nvoie', 'tvoie', 'lvoie', 'cvoie', 'ld', 'com',
  'dep', 'lddep', 'lach', 'tel', 'telc', 'catet', 'lcatet', 'catag', 'lcatag', 'nsiret', 'ape',
  'mft', 'lmft', 'sph', 'lsph', 'douverture', 'dautorisation', 'dms', 'uai'
].map((key) => fields[key] ?? '')].join(';')

const geolocalisation = (numET: string, x: string, y: string, src = 'IGN', dmaj = '2026-05-04') =>
  ['geolocalisation', numET, x, y, src, dmaj].join(';')

/**
 * Échantillon représentatif : zéros de tête, guillemets d'usage, département
 * d'outre-mer (projection dédiée), téléphone court et établissement sans géoloc.
 */
const sourceFile = [
  'finess;etalab;98;2026-05-12',
  structureet({ NumET: '010009173', NumEJ: '010780054', Rs: '"LABM""BIOCEA"""', com: '451', dep: '01', tel: '474454647', telc: '474454114', mft: '03', catet: '611' }),
  structureet({ NumET: '970100012', NumEJ: '970100103', Rs: 'POLYCLINIQUE', crs: '"ANNEXE DU "" BOIS DU ROI"""', com: '302', dep: '9C', tel: '594000000', mft: '01' }),
  structureet({ NumET: '010011674', NumEJ: '010011666', Rs: 'PHARMACIE SANS GEOLOC', com: '288', dep: '01', mft: '09' }),
  geolocalisation('010009173', '871234.5', '6543210.2', 'BAN,V2'),
  geolocalisation('970100012', '300000', '500000'),
  geolocalisation('010011674', '', ''),
  geolocalisation('999999999', '100', '100')
].join('\n') + '\n'

const stubLog = (onInfo?: (msg: string) => void) => ({
  step: async () => {},
  task: async () => {},
  progress: async () => {},
  info: async (msg: string) => { onInfo?.(msg) },
  debug: async () => {},
  warning: async () => {},
  error: async () => {}
})

const stubAxios = (onGet?: () => void) => Object.assign(
  async () => ({ data: { id: 'created-id', title: 'FINESS' } }),
  {
    get: async () => {
      onGet?.()
      return { data: Readable.from([Buffer.from(sourceFile, 'utf8')]) }
    }
  }
)

let dir: string
/** Lignes du CSV produit, indexées par numéro FINESS. */
let rows: Record<string, Record<string, string>>

describe('traitement FINESS', () => {
  before(async () => {
    dir = await fs.mkdtemp(path.join(os.tmpdir(), 'finess-test-'))
    setShouldBeStopped(false)
    // @ts-expect-error stubs minimaux d'axios et du logger
    await download({ url: 'https://example.org/finess.csv' }, dir, stubAxios(), stubLog())
    // @ts-expect-error stub minimal du logger
    await processFiles(dir, stubLog())
    const parsed = parse(await fs.readFile(path.join(dir, 'etablissements_geolocalises.csv')), { columns: true }) as Record<string, string>[]
    rows = Object.fromEntries(parsed.map((row) => [row.NumET, row]))
  })

  after(async () => { await fs.remove(dir) })

  it('expose un schéma de configuration', () => {
    assert.equal(processingConfigSchema.type, 'object')
  })

  it('extrait les deux sections du fichier source', async () => {
    const structureets = await fs.readFile(path.join(dir, 'structureet.csv'), 'utf8')
    const geolocalisations = await fs.readFile(path.join(dir, 'geolocalisation.csv'), 'utf8')
    assert.equal(structureets.trim().split('\n').length, 4, 'en-tête + 3 établissements')
    assert.equal(geolocalisations.trim().split('\n').length, 5, 'en-tête + 4 géolocalisations')
  })

  it('produit une ligne par établissement, sans les géolocalisations orphelines', () => {
    assert.deepEqual(Object.keys(rows).sort(), ['010009173', '010011674', '970100012'])
  })

  it('écrit exactement les colonnes du schéma', () => {
    assert.deepEqual(Object.keys(rows['010009173']), schema.map((field) => field.key))
  })

  it('conserve les zéros de tête des identifiants et des codes', () => {
    // La régression corrigée : typés `integer`, ces champs perdaient leur zéro initial.
    assert.equal(rows['010009173'].NumET, '010009173')
    assert.equal(rows['010009173'].NumEJ, '010780054')
    assert.equal(rows['010009173'].mft, '03')
    assert.equal(rows['010011674'].mft, '09')
    for (const key of ['NumET', 'NumEJ', 'mft', 'dep', 'codeCom', 'tel', 'telc', 'nsiret', 'uai']) {
      assert.equal(schema.find((field) => field.key === key)?.type, 'string', `${key} doit être typé string`)
    }
  })

  it('complète les numéros de téléphone à 10 chiffres', () => {
    assert.equal(rows['010009173'].tel, '0474454647')
    assert.equal(rows['010009173'].telc, '0474454114')
    assert.equal(rows['010011674'].tel, '', 'un téléphone absent reste vide')
  })

  it('normalise les départements d\'outre-mer et le code commune', () => {
    assert.equal(rows['970100012'].dep, '973')
    assert.equal(rows['970100012'].codeCom, '97302')
    assert.equal(rows['010009173'].dep, '01')
    assert.equal(rows['010009173'].codeCom, '01451')
  })

  it('reprojette les coordonnées en WGS84 selon le département', () => {
    const metro = rows['010009173']
    assert.ok(Math.abs(Number(metro.lat) - 45.97) < 0.1, `lat métropole inattendue : ${metro.lat}`)
    assert.ok(Math.abs(Number(metro.lon) - 5.21) < 0.1, `lon métropole inattendue : ${metro.lon}`)

    // La Guyane utilise EPSG:2972, pas le Lambert-93 métropolitain.
    const guyane = rows['970100012']
    assert.ok(Math.abs(Number(guyane.lat) - 4.52) < 0.1, `lat Guyane inattendue : ${guyane.lat}`)
    assert.ok(Math.abs(Number(guyane.lon) + 52.80) < 0.1, `lon Guyane inattendue : ${guyane.lon}`)
  })

  it('laisse les coordonnées vides quand la géolocalisation est absente', () => {
    // L'ancienne version reprojetait Number('') === 0 et produisait un point aberrant.
    assert.equal(rows['010011674'].lat, '')
    assert.equal(rows['010011674'].lon, '')
  })

  it('reporte la source et la date de géolocalisation', () => {
    assert.equal(rows['010009173'].src, 'BAN-V2', 'les virgules sont remplacées par des tirets')
    assert.equal(rows['010009173'].dmaj, '2026-05-04')
  })

  it('conserve les guillemets d\'usage dans les libellés', () => {
    assert.equal(rows['010009173'].Rs, 'LABM "BIOCEA"')
    assert.equal(rows['970100012'].crs, 'ANNEXE DU "BOIS DU ROI"')
  })
})

describe('normalizeQuotes', () => {
  it('espace le guillemet ouvrant et resserre le contenu', () => {
    assert.equal(normalizeQuotes('IMMEUBLE"LE KEYNES"'), 'IMMEUBLE "LE KEYNES"')
    assert.equal(normalizeQuotes('LABM"BIOCEA"'), 'LABM "BIOCEA"')
    assert.equal(normalizeQuotes('ANNEXE DU " BOIS DU ROI"'), 'ANNEXE DU "BOIS DU ROI"')
  })

  it('laisse intacte une valeur déjà correcte', () => {
    assert.equal(normalizeQuotes('Lieu dit "LES BOURBES"'), 'Lieu dit "LES BOURBES"')
    assert.equal(normalizeQuotes('CH DE FLEYRIAT'), 'CH DE FLEYRIAT')
    assert.equal(normalizeQuotes('CH DE L\'AIN'), 'CH DE L\'AIN')
  })
})

describe('interruption', () => {
  it('remonte une erreur exploitable si un fichier intermédiaire manque', async () => {
    const empty = await fs.mkdtemp(path.join(os.tmpdir(), 'finess-empty-'))
    setShouldBeStopped(false)
    // @ts-expect-error stub minimal du logger
    await assert.rejects(processFiles(empty, stubLog()), /ENOENT/)
    await fs.remove(empty)
  })

  it('s\'arrête sans rien publier quand le stop tombe pendant le téléchargement', async () => {
    const stopped = await fs.mkdtemp(path.join(os.tmpdir(), 'finess-stop-'))
    let posted = 0
    const axios = Object.assign(
      async () => { posted++; return { data: { id: 'x', title: 'x' } } },
      { get: stubAxios().get }
    )
    // `stop()` est déclenché juste après l'écriture du fichier source.
    let stopping: Promise<void> | undefined
    const log = stubLog((msg) => { if (msg.startsWith('Fichier récupéré')) stopping = stop() })

    await run({
      processingConfig: { datasetMode: 'update', dataset: { id: 'd1' }, url: 'https://example.org/finess.csv' },
      tmpDir: stopped,
      axios,
      log,
      patchConfig: async () => {}
      // @ts-expect-error contexte de traitement réduit au strict nécessaire
    })

    await stopping
    assert.equal(posted, 0, 'aucun envoi vers data-fair')
    assert.equal(await fs.pathExists(path.join(stopped, 'etablissements_geolocalises.csv')), false)
    await fs.remove(stopped)
  })
})
