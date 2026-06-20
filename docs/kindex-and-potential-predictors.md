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

### Proposed data sources for ingestion
**Potential data sources for ingestion**

For historical model training, the most practical source is NASA SPDF's OMNI high-resolution dataset, accessed through the CDAWeb HAPI endpoint.
- HAPI is the access protocol; OMNI is the actual dataset. 
- The CDAWeb HAPI server exposes REST-like endpoints including `/catalog`, `/info`, and `/data`, and its `/data` endpoint streams time-bounded data for a chosen dataset ID and parameter list [[6]](#source-6).
- The HAPI data-access specification is maintained separately by the HAPI project [[8]](#source-8).

The recommended dataset is:

```text
OMNI_HRO2_1MIN
```

This dataset is described as combined solar wind plasma moments and interplanetary magnetic field data, time-shifted to the nose of Earth's bow shock, at 1-minute cadence [[7]](#source-7). That is convenient for K-index modelling because the upstream solar-wind measurements have already been processed into a near-Earth reference frame.

Useful HAPI parameters:

| Conceptual variable | HAPI parameter | Suggested local column name | Notes |
|---|---|---|---|
| Total magnetic field magnitude | `F` | `B_nT` | This is the `B` feature used in many Kp studies |
| Sun-Earth magnetic-field component | `BX_GSE` | `Bx_GSE_nT` | There is commonly no separate `BX_GSM` because the x-axis is shared for this purpose |
| East-west magnetic-field component | `BY_GSM` | `By_GSM_nT` | GSM coordinate frame |
| North-south magnetic-field component | `BZ_GSM` | `Bz_GSM_nT` | Key geoeffective component |
| Solar wind speed | `flow_speed` | `Vsw_km_s` | Useful core plasma predictor |
| Proton density | `proton_density` | `proton_density_n_cm3` | Useful core plasma predictor |
| Dynamic pressure | `Pressure` | `pressure_nPa` | Can also be derived from speed and density |

Example HAPI request:

```text
https://cdaweb.gsfc.nasa.gov/hapi/data?id=OMNI_HRO2_1MIN&parameters=F,BX_GSE,BY_GSM,BZ_GSM,flow_speed,proton_density,Pressure&time.min=2021-11-21T00:00:00Z&time.max=2021-11-22T00:00:00Z&format=csv
```

For a production-style ingestion design, fetch the OMNI data in bounded chunks, for example weekly or monthly, and store the raw response before feature engineering. Replace OMNI fill values with missing values before aggregating, and keep coverage features such as `Bz_coverage_last_1h` so the model can distinguish genuine calm conditions from missing upstream data.

### How can it be joined to K-index data?
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