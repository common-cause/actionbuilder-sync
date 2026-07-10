## Automated Assessment Rules

The nightly sync automatically sets assessment levels for entities in ActionBuilder based on their engagement activity across our digital platforms. Assessments are **upgrade-only** — the automation will never downgrade someone.

### Level 1

An entity qualifies for Level 1 if they meet **any one** of the following:

- **Any Mobilize event attendance** (past 6 months or all-time)
- **Any NewMode submission**
- **Any ScaleToWin phone bank call**
- **20+ Action Network actions in the past 6 months**
- **1MC Host tag** (completed host training)

### Level 2

An entity qualifies for Level 2 if they meet **any one** of the following:

- **2+ ScaleToWin phone bank calls**
- **2+ virtual Mobilize events in the past 6 months**
- **Any in-person Common Cause Mobilize event** (all-time)
- **Hosted a 1MC event** (≥1 event report in Airtable)

### Level 3

An entity qualifies for Level 3 if they meet the following:

- **1MC Leader tag** (completed leader training)

### Write Policy

The automation respects organizer judgment:

- **Will set:** Entities with no assessment, assessment of 0, or a Level 1–2 previously set by the automation (API user, `created_by_id = 3`)
- **Will not touch:** Any assessment set by a human organizer — even if the data suggests a higher level
- **Never downgrades:** Only writes when the recommended level is higher than the current level
