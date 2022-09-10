#!/bin/bash

HOSTNAME=`hostname --fqdn`
echo "Starting Chord on $HOSTNAME..."
python3 -m chord.http.server $HOSTNAME 5000 8
