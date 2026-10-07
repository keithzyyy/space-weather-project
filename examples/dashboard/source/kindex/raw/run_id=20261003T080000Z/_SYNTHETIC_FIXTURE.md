# Synthetic K-index conflict fixture

This directory is not an original BoM API response.

It is a deliberately constructed presentation fixture paired with the modified
demonstration run `20260307T050056Z`.

For `Australian region` at `2025-01-01 00:00:00`:

- the modified demonstration run reports K-index `6`;
- this later synthetic run reports K-index `8`.

The fixture demonstrates that the audit table preserves both reports, while
canonicalization selects the value from the latest run and sets `flag=true`
because the reported values disagree.

The conflicting timestamp is outside the example dataset-assessment coverage
interval, so it does not make that request ineligible.
