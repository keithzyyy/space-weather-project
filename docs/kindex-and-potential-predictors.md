# What variables should we use to predict K index/Kp index?

## What is Kp index again?

An index that measures *geomagnetic* activity in the Earth's atmosphere, which is also used to forecast geomagnetic storms. Geomagnetic activities give birth to Aurora Borealis in the night skies [[3]](#source-3). The values of the Kp range from 0 (very quiet) to 9 (very disturbed). The Kp index can affect communications, navigation systems, satellite health, power grids, and space travel [[1]](#source-1).

Think of Earth as a magnet surrounded by a magnetic "shield." The Sun constantly blows a stream of charged particles at us (the solar wind) [[4]](#source-4). 
- Low Kp (0 to 3): The solar wind is gentle. The shield handles it easily, and auroras stay only in the far polar regions.
- High Kp (5 to 9): A massive burst of particles from the Sun hits Earth’s magnetic field, causing it to "wobble" and shake. This pushes the auroras much closer to mid-latitudes, making them visible to more people

## Potential predictors

### 🔍 Solar wind and interplanetary magnetic field

From [[1]](#source-1)
> There have been many studies that show the **correlations between Kp and various parameters of the solar wind and interplanetarymagnetic field (IMF)**.

**NOTE**
- The *solar wind* is the flowing plasma from the Sun,
- and the *interplanetary magnetic field* is the **magnetic field carried along** with that plasma.
- So `Bx, By, and Bz`, the 3 components for which the solar wind magnetic field is measured [[5]](#source-5), are often called IMF components, but it's also typically called the solar wind magnetic field components because they are measured in the solar-wind stream.


## Remarks

[[1]](#source-1) Need to properly consider the ML goal: classification (treating positive cases of those with Kp > 5) or regression (but deal with the supposed claim of Kp > 5 being hard to predict)
> Magnetically active times, e.g., Kp > 5, are notoriously difficult to predict, precisely the times when such predictions are crucial to the space weather users.


## Sources
<span id="source-1">[1]</span>: [Kp forecast models by Wing et al. (2005)](/papers/Kp%20forecast%20models%20paper.pdf)

<span id="source-2">[2]</span>: [RMIT University’s practical space weather prediction laboratory by Carter et al. (2022)](/papers/RMIT%20university%20space%20weather%20lab%20paper.pdf)

<span id="source-3">[3]</span>: [What is the Kp-index?](https://theaurorazone.com/nuts-about-kp/)


<span id="source-4">[4]</span>: [Kp index explained](https://auroraforecast.me/guides/kp-index-explained)

<span id="source-5">[5]</span>: [BoM solar wind explanation](https://www.sws.bom.gov.au/Solar/1/4)