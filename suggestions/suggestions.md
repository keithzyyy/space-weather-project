- First I Map out artifacts:
    1. AGENTS.md: I believe this is an AI facing document (machine instruction) — automatically read by AI. I have several approahces:
        1. multiple AGENTS.md across multiple hierarchies of directories
            1. AGENTS.md at the root directory: it specifies the “project-level habit” that needs to be adhered at all times
            2. AGENTS.md at the `tests/` directory: all things the AI needs to know when generating test code
            3. AGENTS.md at the `src/` directory. But since I aim to develop code semi-manually (see my workflow below for more information), i guess this md file can function as:
                1. a code reviewer ensuring that it aligns with the spec
                2. persistent conventions or contracts (initially I reckon these can go into the spec but when the feature building has finished we can condense the contract in some way and move to AGENTS.md) e.g. 
                    1. BoM ingestion datetime strings must follow the strict UTC format configured for the API, currently `YYYY-MM-DD HH:mm:ss`.
                    2. Use chunked BoM API requests for historical ingestion, with configurable `chunk_days` and `sleep_s`.
                    3. …
        2. The problem is that when a contract in `src/` got into `src/agents.md` , and somehow a test is dependent on this contract then `tests/agent.md` cannot access `src/agents.md`. for this reason, an alternative solution is to set one and only one `AGENTS.md` at the root directory. Of crouse, with one big if that it should be as concise as possible so that it does not bloat the contexxt window.
    2. ADR: document crucial decisions/the whys
    3. `specs/spec*.md`: What do I want to build?
- To know when to use what artifact, I map out my workflow first. So when creating a feature (could be ingestion, preprocessing, and later model serving):
    1. create a new branch
    2. create a ipynb notebook under notebooks/
    3. create a spec under specs/ (adhereing to the spec template under `templates/spec-template.md`)
    4. in the specs draft the high level idea of the thing you want.
    5. Scan previous specs and ADR for any potential conflicts or existing conventions to watch out for (the former can be sent as a prompt, the latter however i tend to do it manually to consult my ADR in Notion)
    6. manually (or semi-manually — me and AI) develop code in the notebook and make an attempt to modularize it to functions
    7. refine the spec
    8. repeat step 4-7 until the spec is filled completely, including tests (test parameters of expected input and output, any setUp & tearDown fixtures necessary, what to validate)
    9. construct unit tests for the function signatures in the spec WITHOUT implementation. We wanna validate the ‘boundary’ of the implementation — validate the contract of the functions. 
    10. Only after 9 is done move code from notebook to `src/`
    11. Design an entrypoint/interface in some way under `entrypoint/` with logging
- Some remarks and /or potential suggestions to the above workflow
    1. separating english rules for failure mode and edge cases from Python function definition: Every time you change a failure mode, you have to remember to update both the markdown text and the code block parameters. Suggestion to change the spec template as follows: basically explicitly docuemnt  faillure modes and edge cases at docstrings
        - proposed template refinement under `templates/spec-template.md`
            
            ```markdown
            # Feature: what do I want to build?
            
            ## 1. Recall these things if necessary
            ```
            - Add any project context, assumptions, upstream dependencies, or domain notes that are important before thinking about the feature.
            - If another spec strongly constrains this one, link it here.
            ```
            
            ## 2. High-level approach (!! be as concise as possible)
            ```
            Describe the overall design at a high level.
            
            Suggested prompts:
            - What is the feature supposed to achieve?
            - What are the main stages or transformations?
            - What part of the system owns this behavior?
            - Is this feature static, incremental, rebuild-only, online, batch, etc.?
            
            Keep this section conceptual. Avoid locking yourself into low-level implementation too early.
            ```
            
            ## 3. Expected behavior & invariants (!! encouraged to use pseudocode but keep it conceptual)
            ```
            List the contracts that should always hold if the feature is working correctly.
            
            Suggested prompts:
            - What should the feature do on normal inputs?
            - What outputs, schemas, or side effects are expected?
            - What invariants must remain true before and after execution?
            - What must never be modified, removed, duplicated, or corrupted?
            - If tables/files are produced, what is their expected schema and meaning?
            
            This section should define correctness, not implementation detail.
            ```
            
            ## 4. Key modules/classes/function signatures
            (!! (including edge cases & failure modes)
            ```
            List only the main interfaces and their intended behavior.
            
            Guidelines:
            - Include signatures, arguments, return values, and a brief behavior summary.
            - Do not over-specify internal implementation unless necessary.
            - Keep this section flexible enough to allow iteration during notebook exploration.
            ```
            Below is an example:
            
            ```python
            # src/ingestion/loader.py
            def fetch_raw_data(source_url: str, retry_limit: int = 3) -> pd.DataFrame:
            		"""
            		Pulls CSV from the remote endpoint; implements exponential backoff.
            		EDGE CASES:
            		- ...
            		- ...
            		- ...
            		
            		FAILURE MODES:
            		- ...
            		- ...
            		- ...
            		"""
            
            def validate_schema(df: pd.DataFrame) -> bool:
            		"""
            		Checks for the 5 mandatory columns defined in Invariant 1.2.
            		EDGE CASES:
            		- ...
            		- ...
            		- ...
            		
            		FAILURE MODES:
            		- ...
            		- ...
            		- ...
            		"""
            ```
            
            ```python
            # src/ingestion/cleaner.py
            class ComponentInterface:
                """
                High-level abstraction tracking the primary component execution.
                """
                def __init__(self, config: dict):
                    pass
            
                def execute_transform(self, input_data: Any) -> Any:
                    """
                    Brief summary of primary happy-path behavior.
                    
                    EDGE CASES:
                    - [Edge Case 1]: Describe expected input variation and graceful handling.
                    - [Edge Case 2]: Describe data boundary corrections (clipping, filling).
                    
                    FAILURE MODES & EXCEPTIONS:
                    - [ExceptionClass]: Raised when [Specific Violation Condition occurs].
                    - [ExceptionClass]: Raised when invariant data conditions fail.
                    """
                    pass
            
            ```
            
            ## 5. Unit tests 
            ```
            Unit tests must be derived from the spec of each function:
            1. expected behavior
            2. invariants / schema contracts
            3. important edge cases
            4. failure modes
            
            Assertions should validate those contracts directly, not incidental ordering, formatting, or hardcoded fixture details unless those are explicitly part of the contract.
            
            Suggested test design prompts:
            
            What is the smallest happy-path example?
            What edge case should still succeed gracefully?
            What failure mode should raise or stop execution?
            What invariant must be protected no matter what?
            ```
            
            ## 7. Finally, any remarks?
            ```
            Use this section for short notes that do not fit elsewhere, for example:
            
            reasons for choosing one design over another
            temporary compromises
            assumptions expected to be revisited later
            open questions that should be resolved before boilerplate code is generated
            ```
            ```
            
    2. Although I advocate for manual coding, I tend to take shortcut to impelmenet at `src/` and then tests because manually constructing tests is too long, hence why i ended up using ai to generate tests. A secondary problem is that it takes equally long effort for me to READ the test code. 
        - Proposed solution to address all problems: I propose 80-90% automation to generate test code, with the following guard rails to ensure that I do not take too long to review the test code (remaining 10-20%)
            - set a structure/template for tests?
            - test function naming conventions e.g. `test_[func_name]_[scenario_or_happy_path]` or `test_[func_name]_[edge_case_or_failure_mode]`
            - More detailed test specification in `spec*.md` such as
                
                ```markdown
                ## 6. ⚠️ Test Specifications & Automation Blueprint
                
                ### 6.1 Fixtures & Test Data Setups
                * **Fixture Name:** `mock_[data_description]`
                    * **Scope:** (e.g., function, module, session)
                    * **Structure:** Describe columns, data types, or shape.
                    * **Mock Code Block:**
                      ```python
                      # Paste your raw dictionary, list, or minimal dataframe setup here
                      ```
                
                ### 6.2 Test Matrix (Happy Paths & Edge Cases)
                * **Test Case:** `test_[func_name]_[scenario_or_happy_path]`
                    * **Inputs:** (e.g., `mock_df`, `threshold=0.5`)
                    * **Expected Output:** (e.g., DataFrame with 5 rows, or raises `ValueError`)
                    * **Assertions:** 
                        * Assert shape is exactly `(5, 3)`
                        * Assert column `'x'` contains zero missing values
                        * Assert the mean of column `'y'` equals `10.0`
                
                * **Test Case:** `test_[func_name]_[edge_case_or_failure_mode]`
                    * **Inputs:** (e.g., `mock_df_empty`)
                    * **Expected Output:** Escalates and raises a specific exception
                    * **Assertions:** 
                        * Assert `pytest.raises(ValueError, match="Input dataframe cannot be empty")`
                
                ### 6.3 Mocks & Patches
                * **Target to Patch:** (e.g., `src.ingestion.loader.requests.get`)
                * **Mock Behavior:** (e.g., Return a response mock with status code `200` and text payload `{"status": "ok"}`)
                
                ```
                
                - This way, in this template (or any other template, depending what you think), I can also manually develop the test spec so that I won’t be fully surprised with the generated code.
            - An additioinal `AGENTS.md` file under `tests/` to ensure you the AI write readable, clean and idiomatic test code (⚠️ the following is JUST an example from google AI, because for this project we used `unittest` the python built-in test library). Again, so that I won’t be fully surprised with how the generated test code turns out.
                
                ```markdown
                # Test Automation Guardrails
                
                You are responsible for generating clean, highly scannable, and robust test suites based directly on the feature specification document provided by the user.
                
                ## 1. Architectural Rules
                - **Framework:** Always use `pytest`. Never use the built-in `unittest` class structure unless explicitly requested.
                - **File Naming:** Test files must match the source file exactly, prefixed with `test_`. (e.g., `src/ingestion/cleaner.py` -> `tests/ingestion/test_cleaner.py`).
                - **Isolation:** Tests must be perfectly reproducible. Never read or write to actual local directories or external databases; use `pytest`'s `tmp_path` fixture or mock objects.
                
                ## 2. Test Code Readability & Scannability
                - **The AAA Pattern:** Every test function must be clearly separated into three distinct visual phases using simple comments:
                  ```python
                  def test_calculate_metrics_happy_path(mock_telemetry_df):
                      # Arrange
                      df = mock_telemetry_df
                      
                      # Act
                      result = calculate_metrics(df)
                      
                      # Assert
                      assert result.shape[0] == 10
                  ```
                - **Docstrings:** Keep test functions self-documenting. A brief, single-sentence docstring explaining the boundary contract being tested is mandatory.
                - **No Hardcoded Magic Numbers:** Use descriptive variable names for expected outputs so the test's intent is immediately readable without squinting at array indices.
                
                ## 3. Fixture Management
                - Define shared, re-usable fixtures inside `tests/conftest.py`.
                - Keep feature-specific or small mock data frames localized directly inside the test file itself to preserve readability.
                - Use explicit type hints on fixtures where possible.
                ```
                
            - Probably i’d use `/plan` mode for generating test code (since they are automated so that I have a chance to review to my own understanding based on the shared spec)
    3. I think step 8 (feedback loop from step 4-7) can be kept for now. I still wanna have manual control over the implementation (let’s say 20-80% of it) so that I can minimize cognitive debt as much as possible. step 4-7 will be sort of like a back and forth between us, although I am wondering if there are codex features that can help this process other than me chatting with you (or am I overthinking this because this is exactly the way)
    4. As mentioned, I had the AGENTS.md concern above.