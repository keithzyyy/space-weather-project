# What variables should we use to predict K index/Kp index?

## What is Kp index again?

An index that measures *geomagnetic* activity in the Earth's atmosphere, which is also used to forecast geomagnetic storms. Geomagnetic activities give birth to Aurora Borealis in the night skies [[3]](#source-3). The values of the Kp range from 0 (very quiet) to 9 (very disturbed). The Kp index can affect communications, navigation systems, satellite health, power grids, and space travel [[1]](#source-1).

Think of Earth as a magnet surrounded by a magnetic "shield." The Sun constantly blows a stream of charged particles at us (the solar wind) [[4]](#source-4). 
- Low Kp (0 to 3): The solar wind is gentle. The shield handles it easily, and auroras stay only in the far polar regions.
- High Kp (5 to 9): A massive burst of particles from the Sun hits Earth’s magnetic field, causing it to "wobble" and shake. This pushes the auroras much closer to mid-latitudes, making them visible to more people

## Potential predictors: 🔍 Solar wind and interplanetary magnetic field

From [[1]](#source-1)
> There have been many studies that show the **correlations between Kp and various parameters of the solar wind and interplanetarymagnetic field (IMF)**.

### Definition
**Solar wind, IMF**
- The *solar wind* is the stream of charged particles flowing outward from the Sun.
  - In ML feature terms, the "plasma" part of the solar wind is usually represented by variables such as solar wind speed, proton density, proton temperature, and dynamic pressure.

- The *interplanetary magnetic field* (IMF) is the magnetic field carried along by that solar-wind plasma.
  - In ML feature terms, the magnetic-field part is usually represented by the total field magnitude `B` and the vector components `Bx`, `By`, and `Bz`.

- BoM's solar-wind explainer describes `Bx`, `By`, and `Bz` as components of the *solar wind magnetic field* [[5]](#source-5). Space-weather papers often call the same quantities *IMF components*. These are compatible descriptions: the IMF is the magnetic field embedded in the solar-wind stream.

- `Bx` lies roughly along the Sun-Earth line, while `By` and `Bz` define the plane used for the solar-wind clock angle [[5]](#source-5). For geomagnetic activity, `Bz` is especially important because sustained southward `Bz` couples more efficiently with Earth's magnetic field.


### Proposed features
**Clean feature grouping**
- Solar wind plasma features: `Vsw`, proton density, proton temperature, dynamic pressure.
- Solar wind magnetic-field / IMF features: `B`, `Bx`, `By`, `Bz`, clock angle, southward `Bz` summaries.
- Engineered lag-window features: min, max, and average summaries of the above over windows before the forecast time, for example `Bz_min_last_1h`, `B_avg_last_3h`, or `Vsw_max_last_1h`.

## Proposed data source for ingestion
### Recommended dataset
The recommended external predictor dataset is:

```text
OMNI_HRO2_1MIN
```

This is a 1-minute OMNI high-resolution dataset exposed by NASA/SPDF through CDAWeb. It contains combined solar wind plasma moments and interplanetary magnetic field data, time-shifted to the nose of Earth's bow shock [[6]](#source-6).

The data-engineering rationale:
- **High enough resolution for lag-window features**: K-index is 3-hourly, but predictors like Bz_min_last_1h, Vsw_max_last_3h, or coverage ratios need finer input than hourly data.
- **Can be downsampled later**: 1-minute data can become hourly or 3-hour summaries, but hourly OMNI cannot recover short-lived spikes.
- Already near-Earth/time-shifted: the dataset is processed into a reference frame that is more directly relevant to geomagnetic response than raw spacecraft time.
- **Contains the core predictor families**: IMF variables plus plasma variables in one dataset.
- **Accessible through a stable API path**: CDAWeb HAPI gives /info for metadata/fill values and /data for time-bounded retrieval.

### What OMNI, CDAWeb, and HAPI mean here
**OMNI** is the NASA/SPDF solar-wind and interplanetary magnetic-field dataset family.
- In this project, OMNI is the exact external data source we want to use as upstream solar-wind predictors for K-index modelling.
- **`OMNI_HRO2_1MIN`** is the specific OMNI product we want for the kindex exogenous predictors' ingestion. The dataset ID matters because it is required to retrieve data via the `/info` and `/data` endpoints (see CDAWeb HAPI description below).

**CDAWeb** is NASA/SPDF's Coordinated Data Analysis Web system.
- It is the web data-access service used to browse and retrieve space-physics datasets, including OMNI products.

**CDAWeb HAPI** is the concrete API surface this project would call: CDAWeb exposes OMNI datasets through HAPI-shaped endpoints (CDAWeb HAPI documentation here [[8]](#source-8)).
- NOTE: **HAPI** is the generic time-series data access protocol. It defines endpoint patterns such as `/catalog`, `/info`, and `/data`, but it is not an OMNI dataset by itself (generic HAPI documentation on endpoints here [[9]](#source-9)).

### Why this dataset fits K-index modelling
For historical model training, `OMNI_HRO2_1MIN` is useful because it provides upstream solar wind and IMF variables at 1-minute cadence. These measurements are global near-Earth drivers, not station-specific ground observations, so they can later be converted into lag-window features before each K-index target time.

The selected dataset is also convenient because CDAWeb's dataset-specific HAPI metadata can describe the available parameters, units, types, and fill values for the exact dataset ID used by the pipeline.

### How the data is accessed
The access path is:

```text
CDAWeb HAPI server -> OMNI_HRO2_1MIN dataset -> selected parameters -> bounded time range
```

For ingestion, use bounded time windows such as weekly or monthly chunks. Store the raw response before feature engineering. Missing/fill values should be discovered from `/info` and converted before aggregation.

### Important HAPI endpoints
For this project, the important CDAWeb HAPI endpoints are:

| Endpoint | Role |
|---|---|
| `/catalog` | Discover datasets exposed by the CDAWeb HAPI server. |
| `/info?id=OMNI_HRO2_1MIN` | Retrieve dataset-specific metadata, including parameter names, descriptions, units, types, and fill values. |
| `/data` | Retrieve bounded time ranges for a dataset ID and parameter list. |

The `/info` endpoint is the operational metadata source for ingestion. The `/data` endpoint is the retrieval endpoint.

### Useful parameters for v1 ingestion
The table below is a practical v1 subset of OMNI parameters for solar-wind and IMF predictors, not the complete OMNI data dictionary.

| Conceptual variable | HAPI parameter | Suggested local column name | Notes |
|---|---|---|---|
| Total magnetic field magnitude | `F` | `B_nT` | This is the `B` feature used in many Kp studies |
| Sun-Earth magnetic-field component | `BX_GSE` | `Bx_GSE_nT` | There is commonly no separate `BX_GSM` because the x-axis is shared for this purpose |
| East-west magnetic-field component | `BY_GSM` | `By_GSM_nT` | GSM coordinate frame |
| North-south magnetic-field component | `BZ_GSM` | `Bz_GSM_nT` | Key geoeffective component |
| Solar wind speed | `flow_speed` | `Vsw_km_s` | Useful core plasma predictor |
| Proton density | `proton_density` | `proton_density_n_cm3` | Useful core plasma predictor |
| Dynamic pressure | `Pressure` | `pressure_nPa` | Can also be derived from speed and density |

### Example request
This example shows a `/data` request for one day of selected OMNI parameters. It is a retrieval example; use `/info?id=OMNI_HRO2_1MIN` as the source of truth for parameter metadata and fill values.

```text
https://cdaweb.gsfc.nasa.gov/hapi/data?id=OMNI_HRO2_1MIN&parameters=F,BX_GSE,BY_GSM,BZ_GSM,flow_speed,proton_density,Pressure&time.min=2021-11-21T00:00:00Z&time.max=2021-11-22T00:00:00Z&format=csv
```

### Source roles
- [CDAWeb OMNI dataset notes](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) [[6]](#source-6): Main source for the selected dataset ID, cadence, CDAWeb context, and parameter names.
- [OMNI data documentation](https://omniweb.gsfc.nasa.gov/html/ow_data.html) [[7]](#source-7): Optional deeper background on the broader OMNI data family, provenance, and processing.
- [CDAWeb HAPI documentation](https://cdaweb.gsfc.nasa.gov/hapi) [[8]](#source-8): CDAWeb's HAPI entry point for dataset discovery, metadata, and data retrieval endpoints.
  - CDAWeb HAPI `/info` endpoint: Dataset-specific operational metadata source for ingestion.
- [HAPI 2.0.0 documentation](https://github.com/hapi-server/data-specification/blob/master/hapi-2.0.0/HAPI-data-access-spec-2.0.0.pdf) [[9]](#source-9): the generic specification for the HAPI endpoints (e.g. `/info, /catalog, /capabilities, /data`) used in the CDAWEb HAPI.

```text
https://cdaweb.gsfc.nasa.gov/hapi/info?id=OMNI_HRO2_1MIN
```

The next section explains how `/info` should be used to retrieve fill values.

### Missing values/fill values: how to identify them
- fetch those fill values dynamically from /info instead of hardcoding them (e.g. `99.99`):
```python
import requests

def fetch_hapi_fill_values(dataset_id: str, parameter_names: list[str]) -> dict[str, float | int | str | None]:
    url = "https://cdaweb.gsfc.nasa.gov/hapi/info"
    response = requests.get(url, params={"id": dataset_id}, timeout=60)
    response.raise_for_status()

    info = response.json()
    fills = {}

    for parameter in info["parameters"]:
        name = parameter["name"]
        if name in parameter_names:
            fills[name] = parameter.get("fill")

    return fills

fills = fetch_hapi_fill_values(
    "OMNI_HRO2_1MIN",
    ["F", "BX_GSE", "BY_GSM", "BZ_GSM", "flow_speed", "proton_density", "Pressure"],
)
```

### Suggested invariant to handle missing values
- Fill values must be read from the HAPI /info metadata for the selected dataset and parameters, then converted to missing values before feature aggregation. That keeps the pipeline robust if we later switch from OMNI_HRO2_1MIN to another OMNI product.


## How can it be joined to K-index data?
**How can it be joined to K-index data, and why so**

The OMNI solar-wind / IMF data is **global upstream driver data**, not station-specific ground data.
- It is measured in near-Earth interplanetary space, not at Alice Springs, Canberra, Hobart, Learmonth, or any other ASWFC station.
- Therefore it should be joined to BoM K-index observations by **time only**, then station metadata should be joined separately by station.

Conceptual ML row:

```text
target row:
  station = Hobart
  valid_time = 2024-05-11T03:00:00Z
  target = Hobart K-index for the 03:00-06:00 UTC interval

predictors:
  OMNI Bz minimum before 03:00 UTC
  OMNI B average before 03:00 UTC
  OMNI solar wind speed maximum before 03:00 UTC
  previous completed Hobart K values
  Hobart latitude/longitude
  local-time and seasonal features
```

The same OMNI features for a given timestamp can be attached to every station's K-index row at that timestamp because they represent the same global solar-wind input to Earth's magnetosphere. The station-specific part comes from the response variable (`K` at that station), lagged station K values, and station metadata such as latitude, longitude, magnetic latitude if available, and local time.

### Leakage control
For leakage control, the solar-wind feature windows should end at or before the forecast issue time. If a BoM K-index `valid_time` marks the start of a 3-hour interval, then a safe first modelling rule is:

```text
For target K at valid_time T, use OMNI data only from times < T.
```

Examples:

| Target K interval | Allowed OMNI feature windows |
|---|---|
| `03:00-06:00 UTC` | `02:00-03:00`, `00:00-03:00`, `21:00-00:00` |
| `06:00-09:00 UTC` | `05:00-06:00`, `03:00-06:00`, `00:00-03:00` |

This keeps the model in a real forecasting posture. It prevents the model from seeing solar-wind or ground-response information that would only be available after the target K interval has started.


## Remarks

[[1]](#source-1) Need to properly consider the ML goal: classification (treating positive cases of those with Kp > 5) or regression (but deal with the supposed claim of Kp > 5 being hard to predict)
> Magnetically active times, e.g., Kp > 5, are notoriously difficult to predict, precisely the times when such predictions are crucial to the space weather users.


## Sources
<span id="source-1">[1]</span>: [Kp forecast models by Wing et al. (2005)](/papers/Kp%20forecast%20models%20paper.pdf)

<span id="source-2">[2]</span>: [RMIT University’s practical space weather prediction laboratory by Carter et al. (2022)](/papers/RMIT%20university%20space%20weather%20lab%20paper.pdf)

<span id="source-3">[3]</span>: [What is the Kp-index?](https://theaurorazone.com/nuts-about-kp/)


<span id="source-4">[4]</span>: [Kp index explained](https://auroraforecast.me/guides/kp-index-explained)

<span id="source-5">[5]</span>: [BoM solar wind explanation](https://www.sws.bom.gov.au/Solar/1/4)

<span id="source-6">[6]</span>: [CDAWeb OMNI datasets (link points to the `OMNI_HRO2_1MIN` dataset specifically)](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN)

<span id="source-7">[7]</span>: [OMNI data documentation](https://omniweb.gsfc.nasa.gov/html/ow_data.html) 

<span id="source-8">[8]</span>: [CDAWeb HAPI documentation](https://cdaweb.gsfc.nasa.gov/hapi)

<span id="source-9">[9]</span>: [HAPI 2.0.0 documentation (which is what the CDAWeb HAPI server uses)](https://github.com/hapi-server/data-specification/blob/master/hapi-2.0.0/HAPI-data-access-spec-2.0.0.pdf)
