# COROS Training Hub — custom strength exercise API

Source: Training Hub web bundle (t.coros.com, `main-5sqL4v9u.js` + `index-H_BtFmz3.js`), read 7 Nov 2026.

**Live-tested 7 Nov 2026 (US region, `https://teamapi.coros.com`):** add → query → update → query → delete → query, all `result: "0000"`, on a throwaway exercise that was then deleted. Account left as before.

Headers used in the test (same as the web app's request interceptor):
- `accessToken: <token>` (web app reads it from the `CPL-coros-token` cookie)
- `YFHeader: {"userId":"<userId>","language":"en-US"}`
- `Content-Type: application/json`

Query used for verification: `GET /training/exercise/query?userId=<userId>&sportType=4`.

Gotcha: `userId` in responses is a 64-bit number; JavaScript's JSON.parse loses precision on it (Python is fine). Use the string `id` fields.

## Create
`POST /training/exercise/add`

Body (JSON). The web form starts from:
```json
{"sportType": 4, "exerciseType": 2, "name": "", "overview": "", "part": "", "muscle": "", "muscleRelevance": "", "equipment": ""}
```
Before sending, `part`, `muscle`, `muscleRelevance`, `equipment` are each wrapped into an array (`x ? [x] : []`), and for a NEW exercise these defaults are merged in:
```json
{"access": 1, "intensityCustom": 0, "intensityMultiplier": 0, "intensityType": 1,
 "intensityValue": 0, "intensityValueExtend": 0, "restType": 1, "restValue": 30,
 "targetType": 3, "targetValue": 15}
```
Validation in the UI: `name` and `part` are required; if `part` is 0, `muscle` is required.

Example (Seated DB Shoulder Press — Shoulders / Deltoids / Dumbbells, see [Category codes](#category-codes)):
```json
{"sportType": 4, "exerciseType": 2, "name": "Seated DB Shoulder Press", "overview": "",
 "part": [1], "muscle": [1], "muscleRelevance": [], "equipment": [2],
 "access": 1, "intensityCustom": 0, "intensityMultiplier": 0, "intensityType": 1,
 "intensityValue": 0, "intensityValueExtend": 0, "restType": 1, "restValue": 30,
 "targetType": 3, "targetValue": 15}
```

Create response: `data` is the full new exercise object, including its new `id`.

## Update
`POST /training/exercise/update`

Body: the full existing exercise object (including `id`), with the edited fields changed and the same array-wrapping for part/muscle/muscleRelevance/equipment. No defaults merged. (Tested: object from the query response, `userId` dropped, `name` and `equipment` changed → both updated.)

## Delete
`POST /training/exercise/delete?teamId=<teamId>&userId=<userId>`

Body: JSON array of exercise ids, e.g. `["<exerciseId>"]`.

Tested with only `?userId=<userId>` (no teamId, which is empty for a personal account) → worked.

## Description (`overview`)
For custom exercises, `overview` is a free-text description. Tested 7 Nov 2026: create with `overview` text → stored as sent; update with a new `overview` → stored and shown in Training Hub. Built-ins use `overview` for a `sid_strength_*` i18n key instead, so only treat it as a description when `userId` is non-zero. coros-mcp exposes it as the optional `description` parameter of `create_custom_exercise` and `update_custom_exercise`.

## Notes
- Custom exercises come back from `GET /training/exercise/query` with a non-zero `userId`, and their `name` is the display name (not a T-code); built-ins have `userId: 0`, T-code `name`, and `sid_strength_*` `overview`.
- The UI form sends one value per category, but stored exercises can hold several (e.g. a custom exercise with `part: [4, 5]`, `muscle: [14, 6, 10]`), so the arrays accept multiple codes.
- Custom exercises created in the app may lack the `equipment` / `muscleRelevance` keys entirely; treat missing as `[]`.

## Category codes

The web app builds its code→label map (`exerciseCategoryObj`) from a public dictionary:
`https://static.coros.com/coros-traininghub-v2/static/profile/activitySourceDataProfile_prod.json`, entry `subDataType: 20` (fileVersion `1700633355`). Each item has `dataType` (`part` / `muscle` / `equipment`), `dataValue` (the code, as a string) and `dataCode` (an i18n key resolved by `https://static.coros.com/locale/coros-traininghub-v2/en-US.prod.js`). `muscleRelevance` (secondary muscles) reuses the `muscle` codes. The API itself sends and returns the codes as integers.

| part | label |
|---|---|
| 0 | Whole Body |
| 1 | Shoulders |
| 2 | Chest |
| 3 | Back |
| 4 | Core |
| 5 | Legs/Hips |
| 6 | Arms |

| muscle / muscleRelevance | label |
|---|---|
| 1 | Deltoids |
| 2 | Chest |
| 3 | Biceps |
| 4 | Triceps |
| 5 | Forearms |
| 6 | Abs |
| 7 | Quadriceps |
| 8 | Posterior Thigh |
| 9 | Adductor |
| 10 | Abductor |
| 11 | Trapezius |
| 12 | Latissimus Dorsi |
| 13 | Erector Spinae |
| 14 | Glutes |
| 15 | Calves |

| equipment | label |
|---|---|
| 1 | Bodyweight |
| 2 | Dumbbells |
| 3 | Barbells |
| 4 | Barbell Plates |
| 5 | Ropes |
| 6 | Gym Equipment |
| 7 | Exercise Ball |
| 8 | Bosu Ball |
| 9 | Bands |
| 10 | Medicine Ball |
| 11 | Kettlebell |
| 12 | Hangboard |
| 13 | Indoor Rower |
| 16 | Others |

These match the codes on built-in exercises (e.g. Arnold press `part [1]`, `muscle [1]`, `muscleRelevance [1,4]`; dumbbell bench press variants `equipment [2]`; barbell military press `equipment [3]`; machines `equipment [6]`).

## Using custom exercises in workouts

Strength workouts (`/training/program/add`, inline `/training/schedule/update`) reference an exercise by `originId` = its `id`. For a custom exercise, `name` is its display name and `overview` is its description (`""` if it has none); built-ins use the T-code and `sid_strength_*` key. Each workout exercise also carries the exercise's `access` (`1` for custom, `0` for built-in): a workout with a custom exercise showed wrongly in the app when it was missing.