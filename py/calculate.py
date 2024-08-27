#!/usr/bin/env python3.12

from collections import defaultdict
import fileinput
import sys


def main():
    stations = defaultdict(
        lambda: {"count": 0, "sum": 0.0, "max": float("-inf"), "min": float("inf")}
    )

    i = 0
    for line in fileinput.input(encoding="utf-8"):
        splt = line.strip().split(";")
        station = splt[0]
        measurement = float(splt[1])

        stats = stations[station]
        stats["count"] += 1
        stats["sum"] += measurement
        stats["min"] = min(stats["min"], measurement)
        stats["max"] = max(stats["max"], measurement)
        if i % 1000000 == 0:
            print(i, end="\r", file=sys.stderr)
        i += 1

    print(file=sys.stderr)

    output = []
    for station in sorted(stations.keys()):
        stats = stations[station]
        minimum = stats["min"]
        mean = stats["sum"] / stats["count"]
        maximum = stats["max"]
        output.append(f"{station}={minimum:.1f}/{mean:.1f}/{maximum}")

    print("{" + ", ".join(output) + "}")


if __name__ == "__main__":
    main()
