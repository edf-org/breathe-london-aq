The uncertainty analysis uses 2 scripts:
Script 1:
https://github.com/edf-org/london-aq/blob/dev/4_central_tendency/central2_ulezeval_sampleuncertainty.py

The first script uses SQL to conduct the random sub-sampling (500 sub-samples for every road segment). The script requires that the road segments have at least 75 passes to be included in this analysis. Subsampling is conducted for N = 5 to 35 in increments of 5. All of the bias estimates are pooled into one distribution for each N and each percentile in that distribution is found, so the result is a file with 7 rows (for the 7 different N) of 99 bias values (for the 99 percentiles). 

Note the loop for every pollutant and time periods before and after ULEZ, which wouldn't be applicable in every case.

Script 2:
https://github.com/edf-org/london-aq/blob/dev/4_central_tendency/central3_exceeds_WHO_confidence.py

The second script uses a SQL query to calculate the medians and number of passes (post ULEZ implementation in this case) for each segment. Then given the median, number of passes, and a threshold (WHO annual NO2 in this case), the script uses python to interpolate the distributions that were output by the first script to obtain the probability that each segment's median would exceed the threshold. 
