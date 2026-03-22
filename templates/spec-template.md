# Feature: what do I want to build?

## Recall these things if necessary

## 1. High-level approach


## 2. Expected behavior & Invariants


## 3. Important edge cases
- 

## 4. Failure modes

## 5. Key modules/classes/function signatures
Below is an example:
```
**Module:** `src/ingestion/loader.py`

* `fetch_raw_data(source_url: str, retry_limit: int = 3) -> pd.DataFrame`
    * *Behavior:* Pulls CSV from the remote endpoint; implements exponential backoff.
* `validate_schema(df: pd.DataFrame) -> bool`
    * *Behavior:* Checks for the 5 mandatory columns defined in Invariant 1.2.

**Module:** `src/ingestion/cleaner.py`

* `class DataStreamProcessor:`
    * `__init__(self, config: Dict[str, Any])`
    * `process(self, raw_df: pd.DataFrame) -> pd.DataFrame`
        * *Behavior:* Orchestrates the two-step squashing approach.
```

## 6. ⚠️ Important remark on unit tests
Unit tests **must be derived from the spec** of each function:
1. expected behavior
2. invariants / schema contracts
3. important edge cases
4. failure modes

Assertions **should validate those contracts directly**, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.

## 7. Finally, any remarks?
