#!/bin/bash

SCHEMA=$1;shift

echo "$*" |cqlsh -k $SCHEMA tr-cassandra2
