//This program is an air quality monitoring data post-processing and analysis routine
//prepared by Environmental Defense Fund.

//For details on how to use this program refer to the doc/ folder in each root
//subfolder.

//This program is free software: you can redistribute it and/or modify
//it under the terms of the GNU General Public License as published by
//the Free Software Foundation, either version 3 of the License, or
//(at your option) any later version.   This program is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//GNU General Public License for more details at root level in LICENSE.txt
//or see http://www.gnu.org/licenses/.


//////////////////////////////////////////////////////////////
//general
var mymap = L.map('mapid').setView([51.505, -0.09], 13);
L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'pk.eyJ1IjoibHBhZGlsbGEiLCJhIjoiY2p4ZHMycjUxMDFvMzN6b2ZpZmppaXJrZyJ9.VGh2amxTORviMfn-LH4K0A'
}).addTo(mymap);
var popup = L.popup();

//popup returning lat/lon
function onMapClick(e) {
    popup
        .setLatLng(e.latlng)
        .setContent(e.latlng.toString())
        .openOn(mymap);
}
mymap.on('click', onMapClick);

//add data with style and popups
function addDataToLayer(data, mylayer) {
        mylayer.addData(data);
}

//////////////////////////////////////////////////////////////
//polygon functions
function polygonStyle(feature) {
        return {
                stroke: true,
                color: '#33cccc',
                weight: 3,
                opacity: 1,
                fillOpacity: .25,
                fillColor: '#33cccc'
        };
}

function polygonPopup(feature, layer) {
            var popupText = feature.properties.name;
            layer.bindPopup(popupText); 
}

var polygonLayer = L.geoJSON("",{onEachFeature: polygonPopup, style: polygonStyle});
function polyCallback(json) {
        console.log(json);
        addDataToLayer(json,polygonLayer);
}

//////////////////////////////////////////////////////////////
//road segment functions
//#571E0D
function build5Color(grades,colors) {
        return function (d) {
        return d > grades[5] ? colors[5] :
               d > grades[4]  ? colors[4] :
               d > grades[3]  ? colors[3] :
               d > grades[2]  ? colors[2] :
               d > grades[1]   ? colors[1]:
                         colors[0];
        }
}

function build7Color(grades,colors) {
        return function (d) {
        return d > grades[6] ? colors[6] :
               d > grades[5] ? colors[5] :
               d > grades[4]  ? colors[4] :
               d > grades[3]  ? colors[3] :
               d > grades[2]  ? colors[2] :
               d > grades[1]   ? colors[1]:
                         colors[0];
        }
}
var pmGrades = [0,8,15,25,35,45];
var bcGrades = [0,1600,2200,2800,3400,4000];
var o3Grades = [0,25,50,75,100,125];
//var no2Grades = [0,40,80,120,160,200]; 
var no2Grades = [0,10,20,40,80,120,200]; //BL grades
var noGrades = [0,.2,.4,.8,1.0,1.2];
var co2Grades = [0,420,440,460,480,500];
var covGrades = [0,5,10,15,20,25];
var biasGrades = [0, 10, 20, 30, 40, 50];
var colorArr = ['#229954','#F1C40F','#EBA984','#E36C7C','#A22942','#801A19'];
var biascolorArr = ['#00429d', '#5960a2', '#8681a5', '#aea3a8', '#d4c7a9', '#faeda9'];
var color7Arr = ['#ffffb2', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#b10026'];
var countColorArr = ['#000066','#660099','#993366','#cc6633','#cc9900','#99cc00'];
var pmColor = build5Color(pmGrades,colorArr);
var bcColor = build5Color(bcGrades,colorArr);
var o3Color = build5Color(o3Grades,colorArr);
var no2Color = build7Color(no2Grades,color7Arr);
var noColor = build5Color(noGrades,colorArr);
var co2Color = build5Color(co2Grades,colorArr);
var coverageColor = build5Color(covGrades,countColorArr);
var biasColor = build5Color(biasGrades,biascolorArr);
function buildDrvctWeight(grades) {
        return function (d) {
        return d > grades[5] ? 6 :
               d > grades[4] ? 5 :
               d > grades[3]  ? 4 :
               d > grades[2]  ? 3 :
               d > grades[1]   ? 2 :
                         1;
        }
}
var drvctWeight = buildDrvctWeight(covGrades);

function roadStyle(feature) {
        return {
                weight: drvctWeight(parseInt(feature.properties.no2_drvct)),
                opacity: 1,
                color: no2Color(parseFloat(feature.properties.no2_med)),
        };
}

var legend = L.control({position: 'bottomright'});
var speciesselect = document.getElementById("speciesid");        
legend.onAdd = buildLegend('NO2 (ug/m3)',no2Grades,no2Color)
legend.addTo(mymap);
function symbRoads(lyr,leg){
        if (speciesselect.value=="pm2_5") { 
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.pm2_5_drvct)),opacity:1,color:pmColor(parseFloat(feature.properties.pm2_5_med))};});
                leg.onAdd = buildLegend('PM 2.5 (ug/m3)',pmGrades,pmColor);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="bc") {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.bc_drvct)),opacity:1,color:bcColor(parseFloat(feature.properties.bc_med))};});
                leg.onAdd = buildLegend('BC (ng/m3)',bcGrades,bcColor);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="o3") {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.o3_drvct)),opacity:1,color:o3Color(parseFloat(feature.properties.o3_med))};});
                leg.onAdd = buildLegend('O3 (ppb)',o3Grades,o3Color);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="co2") {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.co2_drvct)),opacity:1,color:co2Color(parseFloat(feature.properties.co2_med))};});
                leg.onAdd = buildLegend('CO2 (umol/mol)',co2Grades,co2Color);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="no2") {
                mymap.removeControl(leg);
                lyr.setStyle(roadStyle);
                leg.onAdd = buildLegend('NO2 (ug/m3)',no2Grades,no2Color);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="no") {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.no_drvct)),opacity:1,color:noColor(parseFloat(feature.properties.no_med))};});
                leg.onAdd = buildLegend('NO (ppm)',noGrades,noColor);
                leg.addTo(mymap);
        }
        else if (speciesselect.value=="pm2_5_drvct") {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:drvctWeight(parseInt(feature.properties.pm2_5_drvct)),opacity:1,color:coverageColor(parseFloat(feature.properties.pm2_5_drvct))};});
                leg.onAdd = buildLegend('Count of passes',covGrades,coverageColor);
                leg.addTo(mymap);
        }
        else {
                mymap.removeControl(leg);
                lyr.setStyle(function (feature) {return {weight:1,opacity:1,color:'#000381'};});
                console.log(speciesselect.value);
        }
}

var lwlegend = L.control({position: 'topright'});
lwlegend.onAdd = lineweightLegend('Drive count<br>(line weight)',covGrades)
lwlegend.addTo(mymap);
function lineweightLegend(title,grades) {
        return function (map) {

    var div = L.DomUtil.create('div', 'info legend weight'),
        labels = [];
    div.style.background="#FBFCFC";
    div.innerHTML = title+'<br>'
    // loop through grades and generate a label with a line thickness for each interval
    for (var i = 0; i < grades.length; i++) {
        div.innerHTML +=
            '<i style="width:18px;float:left;margin-right:8px;opacity:0.7;background:#808080;height:' + (i+1).toFixed(0) + 'px;"></i> ' +
            grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
    }
    //console.log(div.style);
    console.log(div.id);
    return div;
    };
};

function buildLegend(title,grades,colorfunc) {
        return function (map) {

    var div = L.DomUtil.create('div', 'info legend'),
        labels = [];
    div.style.background="#FBFCFC";
    div.innerHTML = title+'<br>'
    // loop through our density intervals and generate a label with a colored square for each interval
    for (var i = 0; i < grades.length; i++) {
        div.innerHTML +=
            '<i style="width:18px;height:18px;float:left;margin-right:8px;opacity:0.7;background:' + colorfunc(grades[i] + .01) + '"></i> ' +
            grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '<br>' : '+');
    console.log(i);
    console.log(grades[i]+.01);
    console.log(colorfunc(grades[i]+.01));
    }
    //console.log(div.style);
    return div;
    };
};

TESTER = document.getElementById('tsplotid');

function roadPopup(feature, layer) {
            var popupText = "Segment ID: " + feature.properties.segid
                + "<br>Function: " + feature.properties.function+"<br>"
                + "<br>PM2.5 drive count: " + parseInt(feature.properties.pm2_5_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.pm2_5_med).toFixed(1)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.pm2_5_std).toFixed(1)+"<br>"
                + "<br>BC drive count: " + parseInt(feature.properties.bc_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.bc_med).toFixed(1)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.bc_std).toFixed(1)+"<br>"
                + "<br>NO2 drive count: " + parseInt(feature.properties.no2_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.no2_med).toFixed(1)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.no2_std).toFixed(1)+"<br>"
                + "<br>NO drive count: " + parseInt(feature.properties.no_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.no_med).toFixed(2)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.no_std).toFixed(2)+"<br>"
                + "<br>CO2 drive count: " + parseInt(feature.properties.co2_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.co2_med).toFixed(1)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.co2_std).toFixed(1)+"<br>"
                + "<br>O3 drive count: " + parseInt(feature.properties.o3_drvct).toFixed(0)
                + "<br>Median: " + parseFloat(feature.properties.o3_med).toFixed(1)
                + "<br>Std. Dev.: " + parseFloat(feature.properties.o3_std).toFixed(1);
            layer.bindPopup(popupText);
            layer.on({click: 
                        function (e) {
                    	console.log(feature.properties.no2time_arr[0]);
                    	console.log(feature.properties.no2time_arr[feature.properties.no2time_arr.length-1]);
                        Plotly.newPlot( TESTER, [
                        /*{
                    	x: feature.properties.pm2_5time_arr,
                    	y: feature.properties.pm2_5mean_arr,
                        name: 'PM2.5',
                        type: 'scatter',
                        mode: 'markers',
                        marker: {color: '#9c1499', size: 5},
                        showlegend: true,
                        xaxis:'x',
                        yaxis:'y2'},
                        */
                        {
                    	x: [feature.properties.no2time_arr[0],feature.properties.no2time_arr[feature.properties.no2time_arr.length-1]],
                    	y: [parseFloat(feature.properties.no2_med), parseFloat(feature.properties.no2_med)],
                        name: 'Median NO2',
                        type: 'scatter',
                        mode: 'lines',
                        line: {color: '#8e9cf5', width: 5, dash:'solid'},
                        showlegend: true,
                        xaxis: 'x',
                        yaxis:'y'},
                        {
                    	x: feature.properties.no2time_arr,
                    	y: feature.properties.no2ugm3_arr,
                        name: 'NO2',
                        type: 'scatter',
                        mode: 'markers',
                        marker: {color: '#425af5', size: 5},
                        showlegend: true,
                        xaxis: 'x',
                        yaxis:'y'}
                        ], 
                        {title: {text:'Drive pass means<br>Segment '+feature.properties.segid.slice(-8),y:0.95}, 
                                margin: { t: 40, r: 20},
                                legend: {x: 1.15},
                                yaxis: {title: {text: 'NO2 concentration (ug/m3)'}}
                                //, yaxis2: {title: {text: 'PM2.5 concentration ug/m3)'}, side:'right',overlaying:'y'}
                        });
}});
}

function roadFilter(count,species){
        return function(feature,layer) {
                var prop;
                if (species=="no2") {
                        prop = feature.properties.no2_drvct;
                }
                else if (species=="co2") {
                        prop = feature.properties.co2_drvct;
                }
                else if (species=="pm2_5") {
                        prop = feature.properties.pm2_5_drvct;
                }
                else if (species=="bc") {
                        prop = feature.properties.bc_drvct;
                }
                else if (species=="o3") {
                        prop = feature.properties.o3_drvct;
                }
                else if (species=="no") {
                        prop = feature.properties.no_drvct;
                }
                else { prop = "10000";}
                return parseFloat(prop) >= parseFloat(count);
        }
}

var drivecountslider = document.getElementById("drivecountrange");
var drivecountoutslider = document.getElementById("drivecountsliderval");
var slider_count_val = 10;
drivecountslider.value = slider_count_val;
drivecountoutslider.innerHTML = slider_count_val;
//build empty layer with default drive count filter of 1
var roadLayer = L.geoJSON("",{onEachFeature: roadPopup, style: roadStyle, filter: roadFilter(slider_count_val,speciesselect.value)});
var roaddata;
var roadcheckbox = document.getElementById("roadcheck");

function updateRoads() {
        slider_count_val = drivecountslider.value;
        drivecountoutslider.innerHTML = slider_count_val;
        //rebuild the layer every time the slider moves
        roadLayer.remove();
        roadLayer = L.geoJSON(roaddata,{onEachFeature: roadPopup, filter: roadFilter(slider_count_val,speciesselect.value)});
        //set the style and legend again
        symbRoads(roadLayer,legend)
        if (roadcheckbox.checked == true) {roadLayer.addTo(mymap)};
}

//change symbology based on slider change
drivecountslider.oninput = updateRoads;
//change symbology based on selected species change
document.getElementById("speciesid").onchange = updateRoads;

function jsonCallback(json) {
        console.log(json);
        roaddata = json;
        addDataToLayer(json,roadLayer);
}


//////////////////////////////////////////////////////////////
//Example placeholder code for others wanting to learn
//////////////////////////
//Katie, your code below!
//////////////////////////
//functions for additional background layers
//Placeholder popup function
function pointPopup(feature, layer) {
            //Katie, edit the fields you want to show up in the popup
            var popupText = 'Field 1: '+feature.properties.field1+"<br><br>"+
                            'Field 2: '+feature.properties.field2;
            layer.bindPopup(popupText);
}

//Placeholder style function
function pointStyle(feature) {
        return {
                //Katie, edit these properties 
                //They can be a function of the feature's attributes, 
                //e.g. feature.properties.number_of_students/100 or whatever
                stroke: true,
                radius: 5,
                color: '#FFA500',
                weight: 3,
                opacity: 1,
                fillOpacity: .25,
                fillColor: '#FFA500'
        };
}
//Katie, you could even use a custom icon for your points following this tutorial:
//https://leafletjs.com/examples/custom-icons/

//Placeholder layer
//Shouldn't need any edits to this call unless you change the name of the functions above
var pointLayer = L.geoJSON("",{pointToLayer: function (feature,latlng) 
                                                {return L.circleMarker(latlng)}, 
                               onEachFeature: pointPopup, 
                               style: pointStyle});
//Placeholder callback
//No changes needed here either unless you use a different callback name than 'pointCallback' with ogr2ogr
function pointCallback(json) {
        console.log(json);
        addDataToLayer(json,pointLayer);
}
//Placeholder ajax call
//Katie, replace url with path to your file, it should be in the same bucket, different filename
//$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/placeholder.json"});
//then remember to add it to the map! we can add a checkbox for it later
//pointLayer.addTo(mymap)
////////////////////////////////////
//End of Katie's code edits
////////////////////////////////////

//////////////////////////////////////////////////////////////
//AQMesh-mobile data comparisons layers
function aqmeshPopup(feature, layer) {
            //Katie, edit the fields you want to show up in the popup
            var popupText = 'Pod location ID: '+feature.properties.pod_id_location+
                            '<br>Location name: '+feature.properties.location_name;
            var plotdata = [{
                    	x: feature.properties.aqmesh_scaled_no2_ugm3.time,
                    	y: feature.properties.aqmesh_scaled_no2_ugm3.value,
                        name: feature.properties.aqmesh_scaled_no2_ugm3.name,
                        type: 'scatter',
                        mode: 'lines',
                        line: {color: '#425af5', width: 2, shape:'linear',smoothing:0,dash:'solid'},
                        showlegend: true,
                        xaxis:'x',
                        yaxis:'y'}];
            for (var i = 0; i < feature.properties.gsv_no2_ugm3.length; i++) {
                    var seg = feature.properties.gsv_no2_ugm3[i];
                    plotdata.push(
                        {
                    	x: seg.time,
                    	y: seg.value,
                        name: seg.name.slice(-8),
                        type: 'scatter',
                        mode: 'markers',
                        //marker: {color: '#229954', size: 5},
                        showlegend: true,
                        xaxis: 'x',
                        yaxis:'y'}
                    );}
            layer.bindPopup(popupText);
            layer.on({click: 
                        function (e) {
                    	Plotly.newPlot( TESTER, plotdata,
                        {title: {text: feature.properties.location_name+' ('+feature.properties.aqmesh_scaled_no2_ugm3.name+')',y:0.95}, 
                        yaxis: {title: {text: 'NO2 concentration (ug/m3)'}},
                        margin: { t: 40 }} );
                    }});
}

//style function
function aqmeshStyle(feature) {
        return {
                //They can be a function of the feature's attributes, 
                //e.g. feature.properties.number_of_students/100 or whatever
                stroke: true,
                radius: 5,
                color: '#425af5',
                weight: 3,
                opacity: 1,
                fillOpacity: .25,
                fillColor: '#435af5'
        };
}

//layer
//Shouldn't need any edits to this call unless you change the name of the functions above
var aqmeshLayer = L.geoJSON("",{pointToLayer: function (feature,latlng) 
                                                {return L.circleMarker(latlng)}, 
                               onEachFeature: aqmeshPopup, 
                               style: aqmeshStyle});
//callback
function aqmeshCallback(json) {
        console.log(json);
        addDataToLayer(json,aqmeshLayer);
}

//////////////////////////////////////////////////////////////
//Hotspots functions
function hotspotPopup(feature, layer) {
            var popupText = 'Segment-median total NO2 (ppb): '+parseFloat(feature.properties.no2_total_med).toFixed(1)+"<br>"+
                            'City-median total NO2 (ppb): '+parseFloat(feature.properties.city_total_med).toFixed(1)+"<br>"+
                            'Total ratio (segment-median LB/city-median UB): '+parseFloat(feature.properties.ratio_total_10).toFixed(1)+", "+
                            parseFloat(feature.properties.ratio_total_20).toFixed(1)+", "+
                            parseFloat(feature.properties.ratio_total_30).toFixed(1)+" (10,20,30% uncert.)<br>"+
                            '<br>City-median UB relative NO2 (ppb): '+parseFloat(feature.properties.MMub_departure_10).toFixed(1)+", "+
                            parseFloat(feature.properties.MMub_departure_20).toFixed(1)+", "+
                            parseFloat(feature.properties.MMub_departure_30).toFixed(1)+"<br>"+
                            'Segment-% elevated relative N02: '+(parseFloat(feature.properties.elevated_pct_departure_10)*100).toFixed(0)+", "+
                            (parseFloat(feature.properties.elevated_pct_departure_20)*100).toFixed(0)+", "+
                            (parseFloat(feature.properties.elevated_pct_departure_30)*100).toFixed(0)+"<br>"+
                            'Relative ratio (segment-median LB/city-median UB): '+parseFloat(feature.properties.ratio_departure_10).toFixed(1)+", "+
                            parseFloat(feature.properties.ratio_departure_20).toFixed(1)+", "+
                            parseFloat(feature.properties.ratio_departure_30).toFixed(1);
            layer.bindPopup(popupText);
}

//style function
function hotspotStyle(feature) {
        return {
                stroke: true,
                color: '#36454f',
                weight: 12,
                opacity: .5,
        };
}

function hotspotFilter(uncert,baseline){
        return function(feature,layer) {
                var ratio;
                var u_pct = uncert+"0"; //convert 1,2,3 to 10,20,30
                if (baseline == "a") {ratio = feature.properties["ratio_total_"+u_pct]}
                else if (baseline == "b") {ratio = feature.properties["ratio_departure_"+u_pct]}
                else {ratio = 200}
                return (parseFloat(ratio) > 1); //elevated status
        }
}

var slider = document.getElementById("uncertaintyrange");
var outslider = document.getElementById("sliderval");
var slider_uncert_val = 1;
slider.value = slider_uncert_val;
outslider.innerHTML = parseInt(slider_uncert_val)*10;
var hotselect = document.getElementById("hot_ab_id");        
//build empty layer with default hotspot filter of X%
var hotspotLayer = L.geoJSON("",{onEachFeature: hotspotPopup, style: hotspotStyle, filter: hotspotFilter(slider_uncert_val,hotselect.value)});
var hotspotdata;
var hotcheckbox = document.getElementById("hotspotcheck");

function updateHotspots() {
        slider_uncert_val = slider.value;
        outslider.innerHTML = parseInt(slider_uncert_val)*10;
        //rebuild the layer every time the slider moves
        hotspotLayer.remove();
        hotspotLayer = L.geoJSON(hotspotdata,{onEachFeature: hotspotPopup, style: hotspotStyle, filter: hotspotFilter(slider_uncert_val,hotselect.value)});
        if (hotcheckbox.checked == true) {hotspotLayer.addTo(mymap)};
}

slider.oninput = updateHotspots;
hotselect.onchange = updateHotspots;

//callback
function hotspotCallback(json) {
        console.log(json);
        hotspotdata = json;
        addDataToLayer(json,hotspotLayer);
}

//////////////////////////////////////////////////////////////
//Temporal bias functions
function tempbiasPopup(feature, layer) {
            var popupText = 'N drives: '+feature.properties.n_passes+"<br>"+
                            'Bias (%): '+parseFloat(feature.properties.bias_pct).toFixed(1)+"<br>"+
                            'Med NO2 (ppb): '+parseFloat(feature.properties.med_a).toFixed(1)+"<br>"+
                            'Subset median NO2 (ppb): '+parseFloat(feature.properties.med_subset).toFixed(1);
            layer.bindPopup(popupText);
}

//style function
function tempbiasStyle(feature) {
        return {
                stroke: true,
                //color: '#36454f',
                color: biasColor(Math.abs(parseFloat(feature.properties.bias_pct))),
                //weight: 2,
                weight: drvctWeight(parseInt(feature.properties.n_passes)),
                opacity: 1
                //opacity: 1,
                //color: 'ff005e'
        };
}

//legend functions
var biaslegend = L.control({position: 'topright'});
biaslegend.onAdd = buildLegend('Abs Bias (%)',biasGrades,biasColor)

//build empty layer
var tempbiasLayer = L.geoJSON("",{onEachFeature: tempbiasPopup, style: tempbiasStyle});
var tempbiasdata;
var tempbiascheckbox = document.getElementById("tempbiascheck");

//callback
function biasCallback(json) {
        console.log(json);
        tempbiasdata = json;
        addDataToLayer(json,tempbiasLayer);
}
//////////////////////////////////////////////////////////////
//Point source layers and functions
function partaPopup(feature, layer) {
            var popupText = 'Operator: '+feature.properties.Operator_Name+"<br>"+
                            'Sector: '+feature.properties.Sector+"<br>"+
                            'Name: '+feature.properties.Site_Name+"<br>"+
                            'Primary fuel: '+feature.properties.Primary1_fuel;
            layer.bindPopup(popupText);
}

//style function
function partaStyle(feature) {
        return {
                stroke: false,
                radius: 4,
                color: '#FFA500',
                weight: 3,
                opacity: 1,
                fillOpacity: 1,
                fillColor: '#000080'
        };
}

//layer
var partaLayer = L.geoJSON("",{pointToLayer: function (feature,latlng) 
                                                {return L.circleMarker(latlng)}, 
                               onEachFeature: partaPopup, 
                               style: partaStyle});
//callback
function partaCallback(json) {
        console.log(json);
        addDataToLayer(json,partaLayer);
}
//partaLayer.addTo(mymap)

function partbPopup(feature, layer) {
            var popupText = 'Operator: '+feature.properties.Operator+"<br>"+
                            'Process Type: '+feature.properties.Process_Type;
            layer.bindPopup(popupText);
}

//style function
function partbStyle(feature) {
        return {
                stroke: false,
                radius: 4,
                color: '#FFA500',
                weight: 3,
                opacity: 1,
                fillOpacity: 1,
                fillColor: '#000080'
        };
}

//layer
var partbLayer = L.geoJSON("",{pointToLayer: function (feature,latlng) 
                                                {return L.circleMarker(latlng)}, 
                               onEachFeature: partbPopup, 
                               style: partbStyle});
//callback
function partbCallback(json) {
        console.log(json);
        addDataToLayer(json,partbLayer);
}
//partbLayer.addTo(mymap)

//////////////////////////////////////////////////////////////
//Additional analysis functions
SURFACEPLOT = document.getElementById('surfaceplotid');
IQRPLOT = document.getElementById('iqrplotid');
//var z_data = {"z_1pct":[[8,9,10],[9,10,11],[12,13,14]]};
var surface_data = {};
var iqr_data;
var heatscale = [[0.0,'#000080'],[0.08,'#000080'],[0.13,'#7fe1e3'],[.16,'#1cc95c'],[.19,'#f0e354'],[0.22,'#f0be54'],[0.25,'#F0a754'],[0.28,'#EBA984'],[0.31,'#E36C7C'],[0.34,'#A22942'],[1,'#801A19']];
var surface_layout = {
                        title: 'Background NO2 (ppb)',autosize: false,
                        height: 600, width: 600,
                        margin:{l:40,r:40,t:40,b:40},
                            yaxis: {title: 'pod'},
                            xaxis: {title:'hour'},
                            zaxis: {title:'NO2'}

};
var iqr_layout = {
                        title: 'Background NO2 IQR',autosize: false,
                        height: 300, width: 600,
                        margin:{l:40,r:40,t:40,b:40},
                            yaxis: {title: 'NO2 (ppb)'},
                            xaxis: {title:'hour'}
};

function updateSurface() {
        var z_pct = document.getElementById('percentileid').value;
        if (z_pct == 'z_5pct') {
            Plotly.newPlot(SURFACEPLOT, surface_data[z_pct], surface_layout);
            Plotly.newPlot(IQRPLOT, iqr_data, iqr_layout);
        }
        else {
            Plotly.newPlot(SURFACEPLOT, surface_data[z_pct], surface_layout);
            Plotly.purge(IQRPLOT);
        }
}

function surfaceCallback(json) {
        console.log(json);
        //console.log(json.z_1pct);
        //z_data = JSON.parse(json);
        //z_data = json;
        surface_data[json.z_pct]=[{z: json.z_arr, type:'heatmap', colorscale: heatscale}]; //alternatively use 'surface'
}

function iqrCallback(json) {
        console.log(json);
        iqr_data = [{x: json.hourint,
                    	y: json.iqr25,
                        name: '25th',
                        type: 'scatter',
                        mode: 'lines',
                        line: {color: '#17becf'},
                        showlegend: true},
                        {x: json.hourint,
                    	y: json.iqr75,
                        name: '75th',
                        type: 'scatter',
                        mode: 'lines',
                        line: {color: '#7f7f7f'},
                        showlegend: true}];
}

$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/background0_1pct_19Aug15.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/background0_5pct_19Aug15.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/background0_10pct_19Aug15.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/background0_50pct_19Aug15.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/background1_iqr_19Aug16.json"})
//change chart based on percentile change
document.getElementById("percentileid").onchange = updateSurface;
//////////////////////////////////////////////////////////////
//Add/remove jsonp map layers

function showPolygons() {
        var checkbox = document.getElementById("polygoncheck");
        if (checkbox.checked == true) {polygonLayer.addTo(mymap)}
        else {polygonLayer.remove();}
}

function showPtsrcs() {
        var checkbox = document.getElementById("ptsrccheck");
        if (checkbox.checked == true) {
                partaLayer.addTo(mymap);
                partbLayer.addTo(mymap);
        }
        else {
                partaLayer.remove();
                partbLayer.remove();
        }
}

function showRoads() {
        if (roadcheckbox.checked == true) {roadLayer.addTo(mymap)}
        else {roadLayer.remove();}
}

function showAQMesh() {
        var checkbox = document.getElementById("aqmeshcheck");
        if (checkbox.checked == true) {aqmeshLayer.addTo(mymap)}
        else {aqmeshLayer.remove();}
}

function showHotspots() {
        if (hotcheckbox.checked == true) {hotspotLayer.addTo(mymap)}
        else {hotspotLayer.remove();}
}

function showTempbias() {
        if (tempbiascheckbox.checked == true) {
                tempbiasLayer.addTo(mymap);
                biaslegend.addTo(mymap);
        }
        else {tempbiasLayer.remove();
              biaslegend.remove();
        }
}

//////////////////////////////////////////////////////////////
//Fetch data
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/drivepolygons.json"});
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/central0_drivesummarystats_19Sep11.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/statmob3_aqmesh_gsv_19Sep11.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/hotspot0_19Aug07.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/laei2013_parta.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/laei2013_partb.json"})
$.ajax({ dataType: "jsonp", url: "https://storage.cloud.google.com/london-viz/pctbias_2019Sep18.json"})
//These will load faster if they are local but remember to turn them back to the bucket version before sharing
//Also may result in CORS issues with new browser rules
//$.ajax({ dataType: "jsonp", url: "statmob3_aqmesh_gsv_19Jul12.json"});
//$.ajax({ dataType: "jsonp", url: "drivepolygons.json"});
//$.ajax({ dataType: "jsonp", url: "central0_drivesummarystats_19Jul23.json"})
//$.ajax({ dataType: "jsonp", url: "hotspot0_19Jul25.json"})
document.getElementById("roadcheck").checked = true
roadLayer.addTo(mymap)
document.getElementById("aqmeshcheck").checked = false
//aqmeshLayer.addTo(mymap)
document.getElementById("hotspotcheck").checked = false
//hotspotLayer.addTo(mymap)
document.getElementById("tempbiascheck").checked = false
//tempbiasLayer.addTo(mymap)
//biaslegend.addTo(mymap)

