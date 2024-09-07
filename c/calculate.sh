#!/usr/bin/env bash

set -e

gcc -Wall -Wextra -O3 -o calculate calculate.c
# ./calculate ../measurements.txt
time ./calculate ../measurements.txt > answer.txt
diff answer.txt ../answer.txt
