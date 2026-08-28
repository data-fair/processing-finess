import fs from 'fs-extra'
import path from 'path'
import type { RunFunction } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import download from './download.ts'
import processFiles, { setShouldBeStopped, isStopped } from './process.ts'
import upload from './upload.ts'

export const stop = async (): Promise<void> => { setShouldBeStopped(true) }

export const run: RunFunction<ProcessingConfig> = async (context) => {
  const { processingConfig, tmpDir, axios, log, patchConfig } = context
  setShouldBeStopped(false)

  await download(processingConfig, tmpDir, axios, log)

  if (isStopped()) {
    await log.warning('Traitement interrompu, pas de publication')
    return
  }

  const size = (await fs.stat(path.join(tmpDir, 'finess.csv'))).size
  if (size === 0) {
    await log.error('Fichier vide, l\'URL a peut-être changé ou a été mal renseignée')
    return
  }

  await processFiles(tmpDir, log)

  if (isStopped()) {
    await log.warning('Traitement interrompu, pas de publication')
    return
  }

  await upload(processingConfig, tmpDir, axios, log, patchConfig)
}
