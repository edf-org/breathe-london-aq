Author: Lauren Padilla\
Project: London GSV

## Objective
Evaluation of temporal bias given expected concentrations defined from total mobile measurement vs. expected concentrations
defined as mobile enhancement over stationary background + stationary background.

Regional background is defined as the 1st percentile concentration among AQMesh sites in a moving 1 hr window (i.e. the 44th lowest measurement out of 4440 values (74 sites * 60 1 min samples after April, 4 15 min samples otherwise)). 

## Data sources
- street-view-air-quality-london BQ datasets
- Breathe London datasets
- LAQN 

## Scripts
**Step:** 0  
**Purpose:** To calculate percent bias, summarize by number of drives, and plot percent bias as a function of N drives. 

> *Scripted Query:* 0  
*Purpose:* Create preliminary table of background conentrations from stationary data 
*Output:* BQ table drivebias0_background

> *Scripted Query:* 1  
*Purpose:* Create preliminary table of drive period medians, background conentrations, and expected values over all drives from segments with N >= 100 drives 
*Output:* BQ table drivebias1_selectpasses

> *Scripted Query:* 2  
*Purpose:* Create table of random subsamples where number of drives (n < N) in (5,10,15,..100). 500 trials per segment per n.  
*Output:* BQ table drivebias2_subsamples

> *Scripted Query:* 3  
*Purpose:* Calculate % bias for each subsample and both methods (total and enhancement) then create summary statistics of % bias over subsamples at each n. Percent bias calc defined as (expected value over n drives in subsample - expected value over all N drives)/(expected value over all N drives)*100%. Expected value using total method: median of n mobile measurements. Expected value using enhancement method: median of n mobile enhancements + median of N stationary background values. 
*Output:* BQ table drivebias3_summarize

**Path:** ..\temporal_bias\bias\drivebias0_totalvsenhancement.py  
**Requirements:** Google cloud API python client and bigquery with pandas, ggplot, pandas, central_tendency processing and corresponding output tables  
**Output:** ..\charts\bias.png and n_segments.png

## Manual


## Notes
