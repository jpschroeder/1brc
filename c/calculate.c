#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "uthash.h"

typedef struct {
    char station[50];
    double min;
    double max;
    double total;
    int count;
    UT_hash_handle hh;
} StationData;

// Comparison function to sort stations alphabetically
int compare_stations(StationData *a, StationData *b) {
    return strcmp(a->station, b->station);
}

int main(int argc, char *argv[]) {
    const char *filename = "measurements.txt";

    if (argc == 2) {
        filename = argv[1];
    }

    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Unable to open file");
        return 1;
    }

    StationData *stations = NULL;  // Hash table

    char line[100];
    while (fgets(line, sizeof(line), file)) {
        char station[50];
        double temperature;

        // Parse the station name and temperature from the line
        sscanf(line, "%[^;];%lf", station, &temperature);

        // Find or add the station in the hash table
        StationData *entry;
        HASH_FIND_STR(stations, station, entry);
        if (entry == NULL) {
            entry = (StationData *)malloc(sizeof(StationData));
            strcpy(entry->station, station);
            entry->min = temperature;
            entry->max = temperature;
            entry->total = temperature;
            entry->count = 1;
            HASH_ADD_STR(stations, station, entry);
        } else {
            // Update the min, max, and total temperatures
            if (temperature < entry->min) {
                entry->min = temperature;
            }
            if (temperature > entry->max) {
                entry->max = temperature;
            }
            entry->total += temperature;
            entry->count += 1;
        }
    }
    fclose(file);

    // Sort the stations alphabetically
    HASH_SORT(stations, compare_stations);

    // Print the results in the required format
    printf("{");
    StationData *entry, *tmp;
    int first = 1;
    HASH_ITER(hh, stations, entry, tmp) {
        double mean = entry->total / entry->count;
        if (!first) {
            printf(", ");
        }
        printf("%s=%.1f/%.1f/%.1f", entry->station, entry->min, mean, entry->max);
        first = 0;
    }
    printf("}\n");

    // Free the memory allocated for the hash table
    HASH_ITER(hh, stations, entry, tmp) {
        HASH_DEL(stations, entry);
        free(entry);
    }

    return 0;
}

