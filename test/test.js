process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
// const download = require('../lib/download')
// const processData = require('../lib/process')
const processFiness = require('../')

/**
describe('download test', function () {
  it('should download the finess file', async function () {
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://static.data.gouv.fr/resources/finess-extraction-du-fichier-des-etablissements/20230503-154418/etalab-cs1100507-stock-20230502-0337.csv'
      },
      tmpDir: 'data'
    }, config, false)
    await download(context.processingConfig, context.tmpDir, context.axios, context.log)
  })
})

describe('process test', function () {
  it('should create a new csv file', async function () {
    this.timeout(100000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {

      },
      tmpDir: 'data'
    }, config, false)
    await processData(context.processingConfig, context.tmpDir, context.log)
  })
})
*/
describe('global test', function () {
  it('should load data on the staging', async function () {
    this.timeout(100000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://static.data.gouv.fr/resources/finess-extraction-du-fichier-des-etablissements/20230503-154418/etalab-cs1100507-stock-20230502-0337.csv',
        datasetMode: 'create',
        dataset: { id: 'finess-test', title: 'finess test' }
      },
      tmpDir: 'data'
    }, config, false)
    await processFiness.run(context)
  })
})
