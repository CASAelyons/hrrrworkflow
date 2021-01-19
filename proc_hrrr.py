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
    def __init__(self, outdir, featurename, flights, obstacles, outputFlightURL, usrname, password, bufferkm):
        self.outdir = outdir
        self.featurename = '"' + featurename + '"'
        self.flights = '"' + flights + '"'
        self.obstacles = '"' + obstacles + '"'
        self.outputFlightURL = '"' + outputFlightURL + '"'
        self.usrname = usrname
        self.password = password
        self.bufferkm = bufferkm

    def generate_dax(self):
        
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        wf = Workflow("casa_hrrr_wf-%s" % ts)
        #dax.metadata("name", "CASA Routing")

        # Call the flight routing algorithm
        #usrpass = self.usrname + ":" + self.password
        d3_job = Job("d3_hrrr")
        d3_job.addArguments("-n", self.featurename)
        #d3_job.addArguments("-l", self.flights)
        #d3_job.addArguments("-o", self.obstacles) 
        #d3_job.addArguments("-t", self.outputFlightURL) 
        #d3_job.addArguments("-u", usrpass)
        #d3_job.addArguments("-b", bufferkm)
        wf.addJob(rout(d3_job)

        # Write the DAX file
        #daxfile = os.path.join(self.outdir, dax.name+".dax")
        #dax.writeXMLFile(daxfile)
        #pegasus_call = os.system("./plan.sh " + daxfile)
        #print pegasus_call

    def generate_workflow(self):
        # Generate dax
        self.generate_dax()

if __name__ == '__main__':
    parser = ArgumentParser(description="Flight Path Routing Workflow")
    parser.add_argument("-l", "--flights", metavar="FLIGHT_QUERY_URL", type=str, help="URL to query flights", required=True)
    parser.add_argument("-o", "--obstacles", metavar="OBSTACLE_QUERY_URL", type=str, help="URL to query obstacles", required=True)
    parser.add_argument("-t", "--outputFlightURL", metavar="OUTPUT_FLIGHT_URL", type=str, help="URL to post output", required=True)
    parser.add_argument("-u", "--usrname", metavar="USR", type=str, help="username for query authentication", required=True)
    parser.add_argument("-p", "--password", metavar="PASS", type=str, help="password for query authentication", required=True)
    parser.add_argument("-b", "--bufferkm", metavar="BUFF", type=str, help="buffer zone around obstacles in km", required=True)
    parser.add_argument("-d", "--outdir", metavar="OUTPUT_LOCATION", type=str, help="DAX Directory", required=True)
    args = parser.parse_args()
    outdir = os.path.abspath(args.outdir)

    if not os.path.isdir(args.outdir):
        os.makedirs(outdir)

    flights = args.flights
    obstacles = args.obstacles
    outputFlightURL = args.outputFlightURL
    usrname = args.usrname
    password = args.password
    bufferkm = args.bufferkm
    
    CASA_AUTH = (usrname,password) 
    response = requests.get(flights, auth=CASA_AUTH, verify=True)
    if response.status_code == 200:
        liveEvents = geojson.loads(response.content)
    
    for feature in liveEvents['features']:
        #all flights will be linestrings and vice versa for now... eventually read a flight property
        if feature['geometry']['type'] == 'LineString':
            featurename = feature['properties']['eventName']
            print featurename
            workflow = hrrrWorkflow(outdir,featurename,flights,obstacles,outputFlightURL,usrname,password,bufferkm)
            workflow.generate_workflow()
