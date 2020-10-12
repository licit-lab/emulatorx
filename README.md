# Artemis Broker

## Creation
To create a broker, execute the following command
`artemis create mybroker`

## Execution
`mybroker\bin artemis run`

# Data Model

## Description

Each _link_ record has the following fields:
- *id*, long, link id
- *length*, int, length in meters
- *ffs*, int, free-flow speeed in km/h
- *frc*, int, functional road class
- *netclass*, int, network importance, from 0 to 6
- *fow*, int, form of way
- *routenumber*, String, road number
- *areaname*, String, area name
- *name*, String, road name
- *geom*, String[], lat-long points sequence

Each _record_ has the following fields:
- *id*, int, progressive
- *linkid*, long, link id
- *coverage*, float, link coverage, from 0 to 1
- *timestamp*, dd/mm/yyyy hh:mm:ss, average speed computing timestamp 
- *speed*, float, average speed in km/h

*Initialization* instantiates AreaNode and for each of them, it assigns the respective links.
