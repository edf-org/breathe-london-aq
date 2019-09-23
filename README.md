# breathe-london-aq

This repository includes software to post-process and analyze highly-localized air quality data from large, mobile and stationary monitoring networks in London, UK.
  
The system architecture includes Google Cloud Storage and BigQuery. Spatial processing relies on BigQuery geography datatypes and related SQL functions.

Key processing stages, in order of operation are:  
- Quality control/data cleaning and flagging (QAQC)
- Assignment of measurements to spatial location (geolocation)
- Calculation of central statistics (central_tendency)
- Visualization (dashboards)
- Uncertainty evaluation (temporal_bias)

Air quality data associated with this repository are publicly available from Air Quality Data Commons (aqdatacommons.org) and Breathe London (breathelondon.org).

This project is licensed under the terms of the GNU General Public License version 3.  

For more information, contact info@aqdatacommons.org and hello@breathelondon.org.