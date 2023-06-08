process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const processFiness = require('../')

describe('global test', function () {
  it('should load data on the staging', async function () {
    this.timeout(100000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://static.data.gouv.fr/resources/finess-extraction-du-fichier-des-etablissements/20230503-154418/etalab-cs1100507-stock-20230502-0337.csv',
        datasetMode: 'update',
        dataset: { id: 'finess-test1', title: 'finess test 1' }
      },
      tmpDir: 'data'
    }, config, false)
    await processFiness.run(context)
  })
})
