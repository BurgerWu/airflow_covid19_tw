# airflow_covid19_tw

## Motivation
Covid -19 has changed people's life since 2020. For Taiwan, aka my home town, 2021 was especially an year of huge impact by Covid-19. In this project, we would like to collect data from Taiwanese government so that we can present the data in our another applcation and also inspect how the disease impacted on Taiwan. In order to do so, we utilize Apache Airflow which is a well-known software for workflow management as well as MySql, a popular relational databasew management system. We will define dags that has operators inside in to scratch raw data from remote and write into our local database.

## Introduction
This repository contains dags folder and plugins required to complete the project task. You can also move relevant files within the folder to your own airflow folder, and it should also work just fine. Within dags folder, there are dag py files that defines the tasks of initializing database, update and data check. Within operators folder, there are two main sub folder called helpers and operators. Helpers contains helper function that our operator can call. Operators folder contains custom operators that does jobs not available(or intentionally make it new operator) with default operators.
