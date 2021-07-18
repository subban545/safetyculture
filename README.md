# SafetyCulture Data Engineering Challenge

## Overview
This repository is for the SafetyCulture Data Engineering challenge.

Docker is used to spin up a number of services for the data pipeline.

The purpose of each service is as follows;

| Service  | Description |
| ------------- | ------------- |
| postgres  | The postgres database used for this data pipeline |
| postgres-load  | A service that creates database tables, inserts raw example data into the postgres database, then runs the ETLs to populate the dimension tables. This runs once when the services are started, then stops.  |

## Requirements

* docker
* docker-compose

## General architecture

The following diagram shows the architecture of this data stream.

![architecture diagram](https://github.com/subban545/safetyculture/master/images/current_pipeline.jpg "Data architecture")


Both user metadata and events are pushed directly into the database.

In this example, the postgres-load service will populate the staging tables, simulating data landing in the database from a foreign system, then it will run the ETL procedure to populate the sc.sc_user and sc.sc_user_event tables.

The postgres database can be connected to and explored locally via port 5432.

```
PGPASSWORD=postgres psql -h localhost -U postgres postgres
```

## Running

Run the following to pull the repository, pull build the docker images, and start the services.

```
git clone https://github.com/subban545/safetyculture
cd safetyculture
docker-compose build
docker-compose up -d
```

## Database structure

### Dimensions

The SafetyCulture software is used as the master system for this data warehouse. The user table in software is based on users of the SafetyCulture application ecosystem. ETLs attend to associate users from other systems (e.g. CRM) to a SafetyCulture user.

All dimensional user data, lands in the staging schema in the database.

An ETL process integrates user data from various sources into a single table with all information on the customer, sc.sc_user.

As a user can move between companies but retain their SafetyCulture account, history must be kept so that user behaviour is associated with the correct organisation. As a result, historical changes for the customer are tracked in sc.sc_user_hist.

All the user event details can be queried from sc.sc_user_event table.

### Tables

| Schema  | Table | Description |
| ------------- | ------------- | ------------- |
| staging  | sc_user_document | Raw SafetyCulture user data in a NoSQL / JSON nested structure |
| staging  | crm_customer | Raw output of crm customer data |
| sc  | sc_user | User dimension containing information user/customer information extracted from multiple sources |
| sc  | sc_user_hist | Table that tracks historical changes to sc.sc_user |
| sc  | sc_user_event | Table containing user event data |


## Scalability & future changes

The architecture used was designed to load data from the files stored in disk. A production solution would likely have the following changes.

![architecture diagram](https://github.com/subban545/safetyculture/master/images/production_pipeline.jpg "Data architecture")