# Traffic Monitoring and Alert

## Introduction

[![Build Status](https://travis-ci.org/osya/Traffic_Monitoring_and_Alert.svg?branch=master)](https://travis-ci.org/osya/Traffic_Monitoring_and_Alert)

This app intended for some traffic monitoring in Denovlab

Used technologies:

- Python
- PostgreSQL & MySQL

## Installation

1. Clone repository of Monitoring app
2. Then run the following commands to bootstrap your environment.

```bash
cd <path_where_you_cloned_app>
virtualenv env
source env/bin/activate
pip install mysqlclient
cd monitoring
nohup python main.py
```

## Usage

## Tests