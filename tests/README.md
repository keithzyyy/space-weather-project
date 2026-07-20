# Example-based testing guide

1. Prefer named dictionaries over long positional tuples to encode scenarios.
    ```python
    # Prefer: requested and expected boundaries are explicit
    cases = (
        {
            "scenario": "clip requested start",
            "requested_start": start - one_day,
            "requested_end": start + one_day,
            "expected_start": start,
            "expected_end": start + one_day,
        },
    )

    # Avoid: positional meaning must be memorized
    cases = (
        ("left", start - one_day, start + one_day, start, start + one_day),
    )
    ```
2. Commenting etiquette examples:
    - Comments for testing specific mechanisms beyond ordinary Python nor the Arrange, Act and Assert framework. This includes things like but not limited to: `patch()` and what real collaborator it replaces, whether a patch is created once per method or once per subtest, `reset_mock()`, `return_value` vs `side_effect`, iteratble `side_effect` behavior, `subTest()`, `assertRaises() as raised` and `raised.exception`, `call_args, call_args_list, call_count`, `inspect.signature(), bind()`, `iter(...)/next(...)` when used to simulate sequential returns, Test-only event ledgers such as `lifecycle_events`, `TemporaryDirectory`, including who owns cleanup, Test callback functions such as `capture_chunk_write`, Why a mock is intentionally left without a return value
    ```python
    # Create these mocks once for the whole table-driven test. Each subtest
    # resets their call history so an earlier case cannot affect a later case.
    with (
        patch("src.ingest.omni.fetch_hapi_info") as mock_fetch_info,
        patch("src.ingest.omni._run_id_utc") as mock_run_id,
    ):
        for case in validation_cases:
            with self.subTest(scenario=case["scenario"]):
                # Clear recorded calls without replacing the configured mock object.
                mock_fetch_info.reset_mock()
                mock_run_id.reset_mock()

    # An iterable side_effect returns the next payload for each fetch call,
    # matching the source function's repeated chunk requests.
    mock_fetch.side_effect = expected_payloads
    ```
    - No need to comment ordinary operations. For this example, no need to comment `# Check that the values are equal`
    ```python
    self.assertEqual(actual_run_dir, expected_run_dir)
    ```

