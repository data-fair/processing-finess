# @data-fair/processing-finess

Plugin data-fair-processings : télécharge, extrait et géolocalise la base FINESS (établissements sanitaires et médico-sociaux) dans un jeu de données data-fair.

## Fonctionnement

1. **Téléchargement** : récupération du fichier source CSV complet depuis data.gouv.fr (avec décodage UTF-8 et suivi des redirections).
2. **Extraction** : séparation des lignes de structures (`structureet`) et des coordonnées géographiques (`geolocalisation`).
3. **Géolocalisation & Transformation** :
   - Reprojection des coordonnées Lambert / UTM vers WGS84 (`lat` / `lon`).
   - Normalisation des codes départements et communes (DOM-TOM inclus).
   - Formatage des numéros de téléphone.
   - Typage en chaînes de caractères des identifiants et codes à zéro de tête (`NumET`, `NumEJ`, `mft`) : typés `integer`, ils perdaient leur zéro initial (`010000024` → `10000024`).
   - Conservation des guillemets d'usage présents dans les libellés (`LABM "BIOCEA"`).
4. **Publication** : création ou mise à jour du jeu de données Data Fair avec schéma typé.

## Développement

- Node 24 (`nvm use`), TypeScript natif (aucune étape de build).
- `npm install`
- `npm run build-types` — génère les types depuis les schémas JSON.
- `npm test` — tests `node:test`.
- `npm run lint` / `npm run lint-fix`.

## Publication

⚠️ La publication au registre est automatisée via GitHub Actions :
- Push sur `main` → staging.
- Tag `v*` → production.
