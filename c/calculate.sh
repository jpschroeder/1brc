#!/usr/bin/env bash

set -e

gcc -o calculate main.c
time ./calculate ../measurements.txt > answer.txt
diff answer.txt ../answer.txt
