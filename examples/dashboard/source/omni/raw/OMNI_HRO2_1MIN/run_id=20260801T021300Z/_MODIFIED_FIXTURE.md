# Modified OMNI demonstration fixture

This directory retains the HAPI schema, timestamps, parameter definitions,
units, source-fill positions, row presence, chunk boundaries, and ingestion
metadata of an OMNI_HRO2_1MIN response. Its non-fill numeric observations have
been reassigned for public presentation.

For each non-time parameter, represented numeric values were deterministically
reassigned only within that parameter. Values never move between parameters.
Nulls and documented source-fill placeholders remain in their original cells,
so the audit, canonical, and coverage states remain unchanged. The exact
reassignment method is intentionally omitted.

The resulting timestamp-value combinations do not represent historical NASA
OMNI measurements and must not be used for scientific analysis or operational
forecasting. NASA SPDF and the OMNI_HRO2_1MIN dataset remain the sources of the
schema, parameter definitions, units, cadence, and source-fill conventions
demonstrated here.
