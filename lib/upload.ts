import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import FormData from 'form-data'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import schema from './schema.json' with { type: 'json' }
import { displayBytes } from './utils.ts'

export default async (
  processingConfig: ProcessingConfig,
  tmpDir: string,
  axios: AxiosInstance,
  log: ProcessingContext<ProcessingConfig>['log'],
  patchConfig: ProcessingContext<ProcessingConfig>['patchConfig']
): Promise<void> => {
  const formData = new FormData()
  const isUpdate = processingConfig.datasetMode === 'update'
  const datasetId = isUpdate ? processingConfig.dataset?.id : undefined

  formData.append('schema', JSON.stringify(schema))
  if (isUpdate) {
    await log.step('Mise à jour du jeu de données')
    await log.info('Mise à jour du schéma')
  } else {
    formData.append('title', processingConfig.datasetTitle || 'FINESS')
    await log.step('Création du jeu de données')
  }

  const filePath = path.join(tmpDir, 'etablissements_geolocalises.csv')
  formData.append('file', fs.createReadStream(filePath), { filename: path.parse(filePath).base })

  const contentLength = await promisify(formData.getLength).call(formData)
  await log.info(`Envoi du fichier à Data Fair (${displayBytes(contentLength)})`)

  const dataset = (await axios({
    method: 'post',
    url: datasetId ? `api/v1/datasets/${datasetId}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data

  if (isUpdate) {
    await log.info(`Jeu de données mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`Jeu de données créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
