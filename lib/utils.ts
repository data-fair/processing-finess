import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'

export const displayBytes = (aSize: number): string => {
  aSize = Math.abs(parseInt(String(aSize), 10))
  if (aSize === 0) return '0 octets'
  const def: [number, string][] = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
  return aSize + ' octets'
}

/**
 * `log.progress` est asynchrone : l'appeler à chaque ligne ou chaque chunk saturerait
 * le worker. On échantillonne la valeur courante une fois par seconde à la place.
 * Retourne la fonction à appeler pour arrêter l'échantillonnage et publier la
 * dernière valeur atteinte (qui n'est pas forcément le total en cas d'interruption).
 */
export const startProgress = async (log: LogFunctions, name: string, total: number, read: () => number): Promise<() => Promise<void>> => {
  if (!total) return async () => {}
  await log.task(name)
  const interval = setInterval(() => {
    log.progress(name, Math.min(read(), total), total).catch(() => {})
  }, 1000)
  return async () => {
    clearInterval(interval)
    await log.progress(name, Math.min(read(), total), total)
  }
}
