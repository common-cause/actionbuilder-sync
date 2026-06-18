-- external_ptv_source_codes: the canonical set of "other group" PTV source codes
-- (lowercased), used to detect coalition-partner provenance in both EP archive
-- records (external_ep_emails) and Mobilize signups (referrer__utm_source).
--
-- Single source of truth so the external/internal call is identical everywhere
-- (master_load_qualifiers, ep_external_removal, mobilize_external_removal).
--
-- Robust to two data-quality issues in the hand-maintained ep_archive.source_codes:
--   1. CASE COLLISIONS — the same code can appear under multiple casings with
--      conflicting flags (e.g. CCAZ external='Y' vs ccaz internal). Because all
--      matching is case-insensitive, the 'Y' would otherwise win and we'd poach-
--      exclude our own people. Rule: a code is external only if NO casing of it is
--      flagged internal (internal wins on conflict — never exclude when the data
--      disagrees with itself).
--   2. KNOWN-OURS OVERRIDES — Common Cause's own codes mistakenly flagged external.
--      CCAZ / CCAZR = Common Cause Arizona; they are ours and were never meant to be
--      external (source-table glitch found 2026-06-18; the upstream rows could not be
--      corrected from this project — no write access to ep_archive). Remove from the
--      override list once ep_archive.source_codes is fixed at the source.

WITH code_flags AS (
  SELECT
    LOWER(source_code) AS source_code,
    COUNTIF(external IS DISTINCT FROM 'Y') AS internal_rows  -- any non-'Y' casing
  FROM ep_archive.source_codes
  WHERE source_code IS NOT NULL
  GROUP BY LOWER(source_code)
)
SELECT source_code
FROM code_flags
WHERE internal_rows = 0                       -- external only if no casing is internal
  AND source_code NOT IN ('ccaz', 'ccazr')    -- known-ours override (Common Cause Arizona)
