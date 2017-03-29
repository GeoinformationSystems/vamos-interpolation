#!/bin/bash

create_csv_vrt(){
    echo $1
    echo "<OGRVRTDataSource>
    <OGRVRTLayer name=\"$1\">
        <SrcDataSource>$1.csv</SrcDataSource>
        <GeometryType>wkbPoint</GeometryType>
        <LayerSRS>WGS84</LayerSRS>
        <GeometryField encoding=\"PointFromColumns\" x=\"lon\" y=\"lat\"/>
    </OGRVRTLayer>
    </OGRVRTDataSource>" > value_points.vrt

    cat value_points.vrt
}

# There is some potential for otimization here. The current logic creates individual rasters for each variable an then perform a
# nearest neighbour interpolation.
# It might be quicker to raster and interpolate the common ID field once and then substitute (join, reclassify, lookup, ...) each
# id value the the variable's value. Theoretically, this should be a lot faster but has not been tested. So speed improvements are
# speculative right now. It is also unknown whether GRASS tools can cope with reclassification tables with more than 65536 values.
# There is some evidence that implementation in GIS libraries restrict the number of values in such lookup tables to a maximum number
# and (silently?) drop additional values. This is something that must be tested *before* switching to a id->value substitution.
process_points() {

    #### convert CSV to shape and reproject ####
    ogr2ogr -f "ESRI Shapefile" -overwrite value_points_utm33n.shp value_points.vrt -t_srs EPSG:25833

    # rasterize id field with 2x2 sqm pixel size, NoData=-1
    gdal_rasterize -a value -a_nodata -1 -ot Float64 -co COMPRESS=DEFLATE -co TILED=YES -co PREDICTOR=3 -tr 2 2 value_points_utm33n.shp value_points_raster.tif
    #gdaladdo --config COMPRESS_OVERVIEW DEFLATE -r NEAREST value_points_raster.tif 4 8 16 32 64
    #gdalinfo -stats value_points_raster.tif

    #### run grass r.grow.distance ####
    echo "Running GRASS (r.grow.distance)"
    workdir="$(mktemp -d)"
    echo "Work $workdir"
    location="$(pwgen 8 1)"
    gisdb="$workdir/grassdata"
    #echo "$gisdb"
    mkdir -p $gisdb
    mapset='PERMANENT'
    locationpath="$gisdb/$location"
    #echo "$location"

    basedir=$(pwd)

    export GISBASE="/usr/lib/grass70"
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$GISBASE/lib"
    export PYTHONPATH="${PYTHONPATH}:$GISBASE/etc/python/"
    export GIS_LOCK=$$

    export GISRC="$workdir/.grassrc"
    echo "$GISRC"
    echo "GISDBASE: $gisdb" > "$GISRC"
    echo "LOCATION_NAME: $location" >> "$GISRC"
    echo "MAPSET: $mapset" >> "$GISRC"
    echo "GRASS_GUI: text" >> "$GISRC"
    export PATH="$GISBASE/bin:$PATH"

    #grass70 -c "$basedir/value_points_raster.tif" -e ${locationpath}
    grass70 -c "$basedir/cellmask.tif" -e ${locationpath}
    r.external input="$basedir/value_points_raster.tif" band=1 output=sourcepoints --overwrite
    r.grow.distance input=sourcepoints metric=euclidean distance=grow_distance value=grow_value --overwrite
    g.remove -f type=raster name=grow_distance,sourcepoints  # remove unused/old data
    r.external input="$basedir/cellmask.tif" band=1 output=cellmask --overwrite
    r.mapcalc "masked_grow_value = cellmask + grow_value"
    g.remove -f type=raster name=grow_value  # remove old data

    # ORIGINAL r.out.gdal command
    #r.out.gdal --overwrite -c createopt="TILED=YES,COMPRESS=DEFLATE" input=masked_grow_value output="$basedir/value_raster.tif"

    # r.out.gdal is buggy in recent grass releases and may omit some elements of the SRS definition
    # see https://trac.osgeo.org/grass/ticket/3048
    #
    # BEGIN workaround
    r.out.gdal --overwrite -c createopt="TILED=YES,COMPRESS=DEFLATE" input=masked_grow_value output="$basedir/value_raster_grass.tif"
    gdal_translate -a_srs EPSG:25833 -co COMPRESS=DEFLATE -co TILED=YES value_raster_grass.tif value_raster.tif
    rm value_raster_grass.tif
    # END workaround
    
    gdaladdo --config COMPRESS_OVERVIEW DEFLATE -r NEAREST value_raster.tif 4 8 16 32 64
    gdalinfo -stats value_raster.tif

    rm -r ${workdir}
    rm value_points_raster.tif

}




# date=2017-01-14

date=$1

# approx. 2 min on Thinkpad T430
if python vamos.py -d ${date}; then
    # Each iteration takes approx. 5..6 min on Thinkpad T430
    for variable in 'gesamt' 'reifen' 'bremsen' 'strasse' 'zw'
    do
        echo "Processing $variable"
        create_csv_vrt ${variable}
        process_points
        mv value_raster.tif "$variable.$date.tif"
    done
else
    echo
    echo "IO or argument error"
    echo "Script usage: ./workflow.sh <YYYY-MM-DD>"
    exit 1
fi




