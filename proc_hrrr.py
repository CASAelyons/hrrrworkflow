#!/usr/bin/env python

import sys
import os
import pwd
#import time
import logging
import requests
import json, geojson, time, socket, subprocess, pytz, certifi, urllib3
from pathlib import Path
from Pegasus.api import *
from datetime import datetime
from argparse import ArgumentParser


class hrrrWindspeedWorkflow(object):
    def __init__(self, configfile, featurename, comparison_str, threshold, inputfile):
        
        self.configfile = configfile
        self.featurename = '"' + featurename + '"'
        self.comparison_str = '"' + comparison_str + '"'
        self.threshold = threshold
        self.inputfile = inputfile

    def generate_jobs(self):
        
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_hrrr_wf-%s" % ts)

        sc = SiteCatalog()
        
        shared_scratch = Directory(Directory.SHARED_SCRATCH, path="/nfs/shared/hrrr/scratch")\
                .add_file_servers(FileServer("file:///nfs/shared/hrrr/scratch", Operation.ALL))

        #container_location = Directory(Directory.SHARED_STORAGE, path="/nfs/shared/ldm")\
        #        .add_file_servers(FileServer("file:///nfs/shared/ldm", Operation.ALL))

        local_storage = Directory(Directory.LOCAL_STORAGE, "/home/ldm/hrrrworkflow/output")\
                .add_file_servers(FileServer("file:///home/ldm/hrrrworkflow/output", Operation.ALL))
        
        local = Site("local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")

        #local.add_directories(shared_scratch,local_storage, container_location)
        local.add_directories(shared_scratch,local_storage)

        #exec_site = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
        exec_site = Site("condorpool")
        exec_site.add_directories(shared_scratch)\
                .add_pegasus_profile(clusters_size=32)\
                .add_pegasus_profile(cores=4)\
                .add_pegasus_profile(data_configuration="nonsharedfs")\
                .add_pegasus_profile(memory=2048)\
                .add_pegasus_profile(style="condor")\
                .add_condor_profile(universe="vanilla")\
                .add_pegasus_profile(auxillary_local="true")\
                .add_profiles(Namespace.PEGASUS)

        #exec_site.add_directories(shared_scratch, container_location)

        sc.add_sites(local, exec_site)

        hrrrconfigfile = File("d3_hrrr_windspeed.cfg")
        #hrrrconfigfile = File(self.configfile)
        inputfile = File("latest_hrrr_80mWinds.netcdf")
        #inputfile = File(self.inputfile)

        rc = ReplicaCatalog()\
             .add_replica("condorpool", hrrrconfigfile, "/nfs/shared/hrrr/d3_hrrr_windspeed.cfg")\
             .add_replica("condorpool", inputfile, "/nfs/shared/hrrr/latest_hrrr_80mWinds.netcdf")

        d3hrrr_container = Container(
            name="d3hrrr_container",
            container_type=Container.SINGULARITY,
            image="file:///nfs/shared/ldm/d3_hrrr_windspeed_singularity.img",
            image_site="condorpool",
            bypass_staging=False,
            mounts=["/nfs/shared:/nfs/shared"]
        )
        
        d3hrrr_transformation = Transformation(
            name="d3hrrr",
            site="condorpool",
            pfn="/opt/d3_hrrr_windspeed/d3_hrrr_windspeed",
            bypass_staging=False,
            container=d3hrrr_container
        )
        
        tc = TransformationCatalog()\
            .add_containers(d3hrrr_container)\
            .add_transformations(d3hrrr_transformation)
            
        props = Properties()
        props.write()

        d3_job = Job(d3hrrr_transformation)\
            .add_args("-c", hrrrconfigfile, "-n", self.featurename, "-e", self.comparison_str, "-t", self.threshold, inputfile)\
            .add_inputs(hrrrconfigfile, inputfile)

        wf.add_jobs(d3_job)
        wf.add_site_catalog(sc)
        wf.add_replica_catalog(rc)
        wf.add_transformation_catalog(tc)
        
        try:
            wf.plan(submit=True)
            wf.wait()
            wf.analyze()
            wf.statistics()
        except PegasusClientError as e:
            print(e.output)

    def generate_workflow(self):
        # Generate dax
        self.generate_jobs()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = ArgumentParser(description="HRRR Workflow")
    parser.add_argument("-c", "--configfile", metavar="CONFIG_FILE", type=str, help="Path to config file", required=True)
    parser.add_argument("-l", "--flights", metavar="FLIGHT_QUERY_URL", type=str, help="URL to query flights", required=True)
    parser.add_argument("-i", "--inputfile", metavar="INPUT_FILE", type=str, help="Path to input netcdf file", required=True)

    args = parser.parse_args()
    configfile = args.configfile
    flights = args.flights
    inputfile = args.inputfile

    try: 
        #response = requests.get(flights, auth=CASA_AUTH, verify=True)
        response = requests.get(flights, verify=True)
        if response.status_code == 200:
            liveEvents = geojson.loads(response.content)
        else:
            print('Unable to query the CityWarn events page. Returned Status is: ' + response.status_code)
            print('Exiting')
            exit
    except requests.exceptions.HTTPError as errh:
        print ("HTTP error querying the live events page: ", errh)
        print('Exiting')
        exit
    except requests.exceptions.ConnectionError as errc:
        print ("Connection error querying the live events page: ", errc)
        print('Exiting')
        exit
    except requests.exceptions.Timeout as errt:
        print ("Timeout error querying the live events page: ", errt)
        print('Exiting')
        exit
    except requests.exceptions.RequestException as errr:
        print ("Request error querying the live events page: ", errr)
        print('Exiting')
        exit

    features = liveEvents.get('features')

    if features is None:
        print('No flights found.  Exiting')
        exit

    for feature in features:
        
        featProperties = feature.get('properties')
        if featProperties is None:
            print("No feature properties defined.  Skipping this feature")
            continue
        featName = featProperties.get('eventName')
        if featName is None:
            print("No name associated with this feature.")
            featName = "UnknownEvent"

        featStart = featProperties.get('startTime')
        if featStart is None:
            print("No startTime associated with this feature. Skipping this feature")
            continue
        
        #massagedLocationTimestamp = featStart.replace("Z", "+00:00")
        #locationDatetime = datetime.fromisoformat(massagedLocationTimestamp)
        #locationUnixsecs = locationDatetime.timestamp()
        #startdt = datetime.strptime(featStart, "%Y-%m-%dT%H:%M:%S%z");

        featEnd = featProperties.get('endTime')
        if featEnd is None:
            print("No endTime associated with this feature. Skipping this feature")
            continue

        #enddt = datetime.strptime(featEnd, "%Y-%m-%dT%H:%M:%S%z");

        #flightTimeCouplet = (startdt.timestamp(), enddt.timestamp())

        featGeometry = feature.get('geometry')
        if featGeometry is None:
            print("No feature geometry exists.  Skipping this feature")
            continue

        featGeometryType = featGeometry.get('type')
        if featGeometryType is None:
            print("No feature geometry type listed.  Skipping this feature")
            continue

        products = featProperties.get('products')
        if products is None:
            print("No feature products listed.  Skipping this feature")
            continue

        for product in products:
            hazardType = product.get('hazard')
            if hazardType is None:
                print("Unknown hazard.  Skipping this product")
                continue
            print(hazardType)
            parameters = product.get('parameters')
            if parameters is None:
                print("No feature parameters listed.  Skipping this product")
                continue
            
            for parameter in parameters:
                #print(parameter)
                valueField = parameter.get('valueField')
                
                comparison = parameter.get('comparison')
                if comparison is not None:
                    if comparison == '>':
                        comparison_str = 'gt'
                    elif comparison == '<':
                        comparison_str = 'lt'
                    elif comparison == '>=':
                        comparison_str = 'gte'
                    elif comparison == '<=':
                        comparison_str = 'lte'
                    elif comparison == '=':
                        comparison_str = 'eq'
                    else:
                        print("unknown comparison.  Assuming gt");
                        comparison_str = 'gt'

                threshold_units = parameter.get('thresholdUnits')
                threshold = parameter.get('threshold')

                if threshold_units == 'mph':
                    threshold = threshold * 0.868976
                elif threshold_units == 'mps':
                    threshold = threshold * 1.934

                distance_units = parameter.get('distanceUnits')
                distance = parameter.get('distance')
                
                if distance_units == 'miles':
                    dxmeters = distance * 1609.34
                elif distance_units == 'kilometers':
                    dxmeters = distance * 1000
                elif distance_units == 'feet':
                    dxmeters = distance * .3048
                elif distance_units == 'meters':
                    dxmeters = distance
                else:
                    print("unknown distance units.  skipping this parameter")
                    continue
                    
                if hazardType == "WINDS_80M":
                    #print("comparison: " + comparison)
                    print("alert on 80M winds " + comparison_str + " " + str(threshold) + " " + threshold_units + " within " + str(distance) + " " + distance_units + " from " + featName)
                    #d3cmd = "/home/elyons/bin/d3_hrrr -c /home/elyons/d3_hrrr/options.cfg -n \"" + featName + "\" -p \"WindSpeed\" -H \"" + hazardType + "\" -e " + comparison_str + " -t " + str(threshold) + " " + windsFile
                    workflow = hrrrWindspeedWorkflow(configfile, featName, comparison_str, threshold, inputfile)
                    workflow.generate_workflow()

