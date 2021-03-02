#!/usr/bin/env bash

cd ../helm-charts/activemq-artemis/activemq-artemis || exit

helm delete artemis

oc delete pvc data-artemis-activemq-artemis-master-0 

helm install artemis .

cd ../../../broker/ || exit

rm -rf mybroker

artemis create mybroker --user licit --password licit --allow-anonymous 

./mybroker/bin/artemis.cmd run
