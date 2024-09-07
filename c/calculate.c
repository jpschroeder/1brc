#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

// Statistics for a particular station
typedef struct {
    char *city;
    size_t city_len;
    uint32_t hash;
    int count;
    int sum;
    int max;
    int min;
} Station;

// Add a single measurement to this station
void station_add_measurement(Station *st, int mnum) {
    st->count++;
    st->sum += mnum;
    if (mnum < st->min) {
        st->min = mnum;
    }
    if (mnum > st->max) {
        st->max = mnum;
    }
}

void station_add_station(Station *st, Station *station) {
    st->count += station->count;
    st->sum += station->sum;
    if (station->min < st->min) {
        st->min = station->min;
    }
    if (station->max > st->max) {
        st->max = station->max;
    }
}

int city_compare(char *ci, size_t ci_len, char *cj, size_t cj_len) {
    size_t min_len = ci_len;
    if (cj_len < min_len) {
        min_len = cj_len;
    }

    int cmp = memcmp(ci, cj, min_len);

    if (cmp == 0) {
        return ci_len - cj_len;
    } else {
        return cmp;
    }
}

int station_compare(const void *i, const void *j) {
    Station *si = *(Station**)i;
    Station *sj = *(Station**)j;

    // > 0 means i bigger than j
    // < 0 means j bigger than i
    // sort nulls to the end means that nulls need to be bigger
    if (si == NULL && sj == NULL) {
        return 0;
    }
    if (si == NULL) {
        return INT_MAX;
    }
    if (sj == NULL) {
        return INT_MIN;
    }

    return city_compare(si->city, si->city_len, sj->city, sj->city_len);
}

const size_t HashTableInitialSize = 1 << 18; // Must be a power of 2

// A collection of station statistics
typedef struct {
    Station **table; // Hash table of stations
    size_t length;
    uint32_t count;
} Stations;

void stations_init(Stations *s) {
    s->count = 0;
    s->length = HashTableInitialSize;
    s->table = (Station**)calloc(s->length, sizeof(Station*));
}

void stations_zero(Stations *s) {
    for (size_t i = 0; i < s->length; i++) {
        Station *st = s->table[i];
        if (st == NULL) {
            continue;
        }
        free(st);
        s->table[i] = NULL;
    }
}
    
void stations_free(Stations *s) {
    free(s->table);
}

// Sort stations by city
// Warning, this destroys the hashtable
void stations_sort(Stations *s) {
    qsort(s->table, s->length, sizeof(Station*), station_compare);
}

// Get a station given a hash and key value
// If the station does not exist, return nil and index where it would go
uint32_t stations_get(Stations *s, uint32_t hash, char *city, size_t city_len, Station **station) {
    // Because size is a power of 2, we can just mask off the higher bits
    // This is faster than using modulus
    uint32_t index = hash & (s->length-1);

    // Forward probe until you find a matching station or an empty entry
    Station *entry = s->table[index];
    while (entry != NULL && city_compare(city, city_len, entry->city, entry->city_len) != 0) {
        index++;
        if (index >= s->length) {
            index = 0; // Loop around to beginning
        }
        entry = s->table[index];
    }
    *station = entry;
    return index;
}

// Update a station value - this value should already exist
void stations_update(Stations *s, Station *value) {
    Station *entry;
    uint32_t index = stations_get(s, value->hash, value->city, value->city_len, &entry);
    s->table[index] = value;
}

// Add a new station to the set - this station should not already exist
// The index should be returned from stations_get()
// Warning: You may need to free the station
void stations_append(Stations *s, uint32_t index, Station *st) {
    s->table[index] = st;
    s->count++;

    // Make sure that the hash table is twice as big as the number of entries
    if (s->count > s->length / 2) {
        size_t oldlength = s->length;
        Station **oldtable = s->table; 
        s->length *= 2;
        s->table = (Station**)calloc(s->length, sizeof(Station*));
        for (size_t i = 0; i < oldlength; i++) {
            Station *station = oldtable[i];
            if (station == NULL) {
                continue;
            }
            stations_update(s, station);
        }
    }
}

// Combine the statistics of several sets of stations
void stations_combine(Stations *s, Stations *others) {
    for (size_t i = 0; i < others->length; i++) {
        Station *other = others->table[i];
        if (other == NULL) {
            continue;
        }
        Station *existing;
        uint32_t index = stations_get(s, other->hash, other->city, other->city_len, &existing);
        if (existing == NULL) {
            stations_append(s, index, other);
        } else {
            station_add_station(existing, other);
        }
    }
}

typedef struct {
    char *chunk;
    size_t chunk_len;
} Chunk;

// Divide data into a set of chunks
// Ensure that the chunks are on newline boundaries
size_t chunk_data(char *data, size_t data_len, Chunk *chunks, size_t numchunks) {
    size_t chunksize = data_len / numchunks;
    size_t start = 0;
    size_t end = start + chunksize - 1;
    size_t index = 0;
    while (start < data_len) {
        if (end >= data_len) {
            end = data_len - 1;
        }
        // Look forward for the next newline
        while (end < data_len && data[end] != '\n') {
            end++;
        }
        chunks[index].chunk = data + start;
        chunks[index].chunk_len = end - start;
        index++;

        start = end + 1;
        end = start + chunksize + 1;
    }
    return index;
}


const uint32_t offset32 = 2166136261;
const uint32_t prime32  = 16777619;

// Parse the next line of the input
// This should be allocation free
// This expects well formatted input for performance reasons
size_t parse_line(char *data, size_t pos, char **city_r, size_t *city_len_r, uint32_t *hash_r, int *number_r) {
    size_t start = pos;

    // Parse the city name
    //   Advance to the position of the semicolon
    //   Calculate the FNV hash at the same time
    uint32_t hash = offset32;
    char value = data[pos];
    while (value != ';') {
        hash = (hash ^ value) * prime32;
        pos++;
        value = data[pos];
    }

    *city_r = data + start;
    *city_len_r = pos - start;
    *hash_r = hash;

    // Skip over the semicolon
    pos++;

    // Look for the negative sign
    int sign = 1;
    if (data[pos] == '-') {
        sign = -1;
        pos++;
    }

    // Parse the temperature whole number part
    //   Advance to the position of the dot
    int whole = 0;
    value = data[pos];
    while (value != '.') {
        whole *= 10;
        whole += value - '0';
        pos++;
        value = data[pos];
    }

    // Skip over the dot
    pos++;

    // Add in the decimal value and the sign
    //   Use the fact that there is only one decimal precision to represent as an int
    //   This allows for integer arithmetic instead of floating points
    *number_r = sign * ((whole * 10) + data[pos]-'0');

    // Skip over the decimal value and the newline
    pos += 2;

    return pos;
}

void parse_chunk(char *data, size_t data_len, Stations *stations) {
    char *city;
    size_t city_len;
    uint32_t hash;
    int mnum;
    size_t pos = 0;
    while (pos < data_len) {
        pos = parse_line(data, pos, &city, &city_len, &hash, &mnum);

        Station *st = NULL;
        uint32_t index = stations_get(stations, hash, city, city_len, &st);
        if (st == NULL) {
            st = (Station*)malloc(sizeof(Station));
            st->city = city;
            st->city_len = city_len;
            st->hash = hash;
            st->count = 1;
            st->sum = mnum;
            st->max = mnum;
            st->min = mnum;
            stations_append(stations, index, st);
        } else {
            station_add_measurement(st, mnum);
        }
    }
}

void format_output(Stations *totals) {
    stations_sort(totals);
    printf("{");
    fflush(stdout);
    for (size_t i = 0; i < totals->count; i++) {
        Station *st = totals->table[i];
        size_t wrote = write(1, st->city, st->city_len);
        if (wrote != st->city_len) {
            perror("error writing stdout");
            exit(1);
        }
        float minimum = (float)(st->min) / 10.0;
        float mean = (float)(st->sum) / (float)(st->count) / 10.0;
        float maximum = (float)(st->max) / 10.0;
        printf("=%.1f/%.1f/%.1f", minimum, mean, maximum);
        if (i < totals->count - 1) {
            printf(", ");
        }
        fflush(stdout);
    }
    printf("}\n");
}

int main(int argc, char *argv[]) {
    const char *filename = "measurements.txt";

    if (argc == 2) {
        filename = argv[1];
    }

    int file = open(filename, O_RDONLY);
    if (file < 0) {
        perror("error opening file");
        return 1;
    }

    struct stat fileinfo;
    int err = fstat(file, &fileinfo);
    if (err < 0) {
        perror("error getting file stats");
        return 1;
    }

    size_t filesize = fileinfo.st_size;

    char *data = mmap(NULL, filesize, PROT_READ, MAP_SHARED, file, 0);
    if (data == MAP_FAILED) {
        perror("error mmaping file");
        return 1;
    }

    Stations stations;
    stations_init(&stations);
    parse_chunk(data, filesize, &stations);

    format_output(&stations);

    stations_zero(&stations);
    stations_free(&stations);

    close(file);
    munmap(data, filesize);

    return 0;
}

