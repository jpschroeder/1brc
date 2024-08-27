#!/usr/bin/env bash

set -e
time ./calculate.py ../measurements.txt > answer.txt
diff answer.txt ../answer.txt
