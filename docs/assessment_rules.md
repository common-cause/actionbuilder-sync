## Automated Assessment Rules

The nightly sync automatically sets assessment levels for entities in ActionBuilder based on their engagement activity across our digital platforms. Assessments are **upgrade-only** — the automation will never downgrade someone.

### Level 1

An entity qualifies for Level 1 if they meet **any one** of the following:

- **Any Mobilize event attendance** (past 6 months or all-time)
- **Any NewMode submission**
- **Any ScaleToWin phone bank call**
- **20+ Action Network actions in the past 6 months**

### Level 2

An entity qualifies for Level 2 if they meet **any one** of the following:

- **2+ ScaleToWin phone bank calls**
- **2+ virtual Mobilize events in the past 6 months**
- **Any in-person Common Cause Mobilize event** (all-time)

### Write Policy

The automation respects organizer judgment:

- **Will set:** Entities with no assessment, assessment of 0, or Level 1 set by the automation
- **Will not touch:** Any assessment set by a human organizer — even if the data suggests a higher level
- **Never downgrades:** Only writes when the recommended level is higher than the current level
