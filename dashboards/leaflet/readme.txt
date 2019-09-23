2019 August 31
Contact lpadilla@edf.org for assistance.
This dashboard is intended for use by EDF and EDF Partners. It is designed to help us see coverage and general levels of PRELIMINARY data.

It is a light-weight, interactive London air quality data visualization tool that runs locally in your browser, accessing prepared geojson files in 
Google Cloud Storage bucket: https://console.cloud.google.com/storage/browser/london-viz?project=street-view-air-quality-london.

You must have a Google account with view permission on this bucket to be able to use the tool, and you must be logged into this Google account in your browser.

Some caveats about the data:
-It is Preliminary with a capital P. Not all of the QAQC logic is in place and analysis methods are not finalized. 
-Lauren reserves the right to change everything and any time.
-It’s showing ~30 m road segments, which is very fine resolution so can be slow to load. For panning and zooming, Lauren recommends turning the roads off, navigating to the area of interest, and then turning the roads back on. 
-Mobile data is through Aug 31 2019.
-AQ Mesh points are through Jun 30, 1-hr-average data and “best” scaling factors. In the chart, the lines are the pod data and the dots are the unique drive passes from mobile data, color-coded by unique segment.
-The mobile data map displays the median of the drive pass distribution. If there was more than one drive pass in a 1-hour window, only the median of those makes it into the distribution. We can change the length of this window or remove it altogether. 
-The mobile map colors and break points are approximate and not tied to any hazard levels or formalized uncertainty levels at this time
-Minimum detection limits have not been applied to mobile data.
-The road segment discretization is close to final but Lauren may revise it in some locations where the algorithm broke down (like for circular roads). 

