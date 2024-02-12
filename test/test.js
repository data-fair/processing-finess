process.env.NODE_ENV = 'test'
const config = require('config')
const processFiness = require('../')

describe('global test', function () {
  it('should load data on the staging', async function () {
    this.timeout(100000)

    const testsUtils = await import('@data-fair/lib/processings/tests-utils.js')
    const context = testsUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        url: 'https://www.data.gouv.fr/fr/datasets/r/98f3161f-79ff-4f16-8f6a-6d571a80fea2',
        datasetMode: 'create',
        dataset: { id: 'finess-test', title: 'finess test' }
      },
      tmpDir: 'data'
    }, config, false)
    await processFiness.run(context)
  })
})
