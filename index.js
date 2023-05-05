const download = require('./lib/download')
const process = require('./lib/process')
const util = require('util')
const fs = require('fs-extra')
const path = require('path')
const schema = require('./lib/schema')
const FormData = require('form-data')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  const formData = new FormData()

  await download(processingConfig, tmpDir, axios, log)
  await process(tmpDir, log)

  if (processingConfig.datasetMode === 'update') {
    await log.step('Mise à jour du jeu de données')
    await log.info('Mise à jour du schéma')
    formData.append('schema', JSON.stringify(schema))
  } else {
    formData.append('schema', JSON.stringify(schema))
    formData.append('title', processingConfig.dataset.title)
    await log.step('Création du jeu de données')
  }
  const filePath = path.join(tmpDir, 'etablissements_geolocalises.csv')
  formData.append('file', await fs.createReadStream(filePath), { filename: path.parse(filePath).base })

  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()
  const dataset = (await axios({
    method: 'post',
    url: (processingConfig.dataset && processingConfig.dataset.id) ? `api/v1/datasets/${processingConfig.dataset.id}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), contentLength }
  })).data

  if (processingConfig.datasetMode === 'update') {
    await log.info(`jeu de donnée mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
