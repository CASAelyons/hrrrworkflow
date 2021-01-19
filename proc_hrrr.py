#!/usr/bin/env python

import sys
import os
import pwd
#import time
import logging
#import requests
#import json, geojson, time, socket, subprocess, pytz, certifi, urllib3
from pathlib import Path
from Pegasus.api import *
#from datetime import datetime
from argparse import ArgumentParser


class hrrrWorkflow(object):
    def __init__(self, featurename, flights, alertURL, usrname, password):

        self.featurename = '"' + featurename + '"'
        self.flights = '"' + flights + '"'
        self.alertURL = '"' + alertURL + '"'
        self.usrname = usrname
        self.password = password

    def generate_dax(self):
        
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_hrrr_wf-%s" % ts)
        #dax.metadata("name", "CASA Routing")

        # Call the flight routing algorithm
        usrpass = self.usrname + ":" + self.password
        d3_job = Job("d3_hrrr")
        d3_job.addArguments("-n", self.featurename)
        d3_job.addArguments("-l", self.flights)
        d3_job.addArguments("-t", self.alertURL) 
        d3_job.addArguments("-u", usrpass)
        wf.addJob(d3_job)

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()

if __name__ == '__main__':
    parser = ArgumentParser(description="HRRR Workflow")
    parser.add_argument("-l", "--flights", metavar="FLIGHT_QUERY_URL", type=str, help="URL to query flights", required=True)
    parser.add_argument("-t", "--alertURL", metavar="ALERT_URL", type=str, help="URL to post output", required=True)
    parser.add_argument("-u", "--usrname", metavar="USR", type=str, help="username for query authentication", required=True)
    parser.add_argument("-p", "--password", metavar="PASS", type=str, help="password for query authentication", required=True)

    args = parser.parse_args()

    flights = args.flights
    alertURL = args.alertURL
    usrname = args.usrname
    password = args.password
    
    CASA_AUTH = (usrname,password) 

    try: 
        response = requests.get(flights, auth=CASA_AUTH, verify=True)
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
        print('No features found.  Exiting')
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

        startdt = datetime.strptime(featStart, "%Y-%m-%dT%H:%M:%S%z");

        featEnd = featProperties.get('endTime')
        if featEnd is None:
            print("No endTime associated with this feature. Skipping this feature")
            continue

        enddt = datetime.strptime(featEnd, "%Y-%m-%dT%H:%M:%S%z");

        flightTimeCouplet = (startdt.timestamp(), enddt.timestamp())

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
                    print("alert on 80M winds " + comparison + " " + str(threshold) + " " + threshold_units + " within " + str(distance) + " " + distance_units + " from " + featName
                    d3cmd = "/home/elyons/bin/d3_hrrr -c /home/elyons/d3_hrrr/options.cfg -n \"" + featName + "\" -p \"WindSpeed\" -H \"" + hazardType + "\" -e " + comparison_str + " -t " + str(threshold) + " " + windsFile
                    workflow = hrrrWorkflow(featname,flights,alertURL,usrname,password)
                    workflow.generate_workflow()
