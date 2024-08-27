#!/usr/bin/env bash

set -e

go build -o calculate .
rm -f cpu.prof
rm -f mem.prof
rm -f answer.txt
time ./calculate ../measurements.txt > answer.txt
diff answer.txt ../answer.txt

# go tool pprof -http localhost:3000 cpu.prof
# go tool pprof -http localhost:3000 mem.prof
