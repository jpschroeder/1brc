package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"
)

// Statistics for a particular station
type Station struct {
	city  []byte
	hash  uint32 // hash of the city
	count int
	sum   int
	max   int
	min   int
}

// Add a single measurement to this station
func (st *Station) AddMeasurement(mnum int) {
	st.count++
	st.sum += mnum
	if mnum < st.min {
		st.min = mnum
	}
	if mnum > st.max {
		st.max = mnum
	}
}

// Add the statistics to this station
func (st *Station) AddStation(station *Station) {
	st.count += station.count
	st.sum += station.sum
	if station.max > st.max {
		st.max = station.max
	}
	if station.min < st.min {
		st.min = station.min
	}
}

const HashTableInitialSize = 1 << 18 // Must be a power of 2

// A collection of station statistics
type Stations struct {
	table []*Station // Hash table of stations
	list  []*Station // Unique list of stations
}

func NewStations() *Stations {
	return &Stations{
		table: make([]*Station, HashTableInitialSize),
		list:  make([]*Station, 0),
	}
}

// The number of unique stations
func (s *Stations) Count() int {
	return len(s.list)
}

// Sort unique stations by city
func (s *Stations) Sort() {
	sort.Slice(s.list, func(i, j int) bool {
		return bytes.Compare(s.list[i].city, s.list[j].city) < 0
	})
}

// All unique stations
func (s *Stations) All() []*Station {
	return s.list
}

// var HashTableCollisions int = 0
// var HashTableExpansions int = 0

// Get a station given a hash and key value
// If the station does not exist, return nil and index where it would go
func (s *Stations) Get(hash uint32, city []byte) (int, *Station) {
	// Because size is a power of 2, we can just mask off the higher bits
	// This is faster than using modulus
	index := int(hash & uint32(len(s.table)-1))
	//index := int(hash % uint32(len(ht.table)))

	// Forward probe until you find a matching station or an empty entry
	entry := s.table[index]
	for entry != nil && !bytes.Equal(city, entry.city) {
		// HashTableCollisions++
		index++
		if index >= len(s.table) {
			index = 0 // Loop around to beginning
		}
		entry = s.table[index]
	}
	return index, entry
}

// Update a station value - this value should already exist
func (s *Stations) Update(value *Station) {
	index, _ := s.Get(value.hash, value.city)
	s.table[index] = value
}

// Add a new station to the set - this station should not already exist
// The index should be returned from Get()
func (s *Stations) Append(index int, st *Station) {
	s.table[index] = st
	s.list = append(s.list, st)

	// Make sure that the hash table is twice as big as the number of entries
	if len(s.list) > len(s.table)/2 {
		// HashTableExpansions++
		s.table = make([]*Station, len(s.table)*2)
		for _, station := range s.list {
			s.Update(station)
		}
	}
}

// Combine the statistics of several sets of stations
func (s *Stations) Combine(others *Stations) {
	for _, other := range others.All() {
		index, existing := s.Get(other.hash, other.city)
		if existing == nil {
			s.Append(index, other)
		} else {
			existing.AddStation(other)
		}
	}
}

// Divide data into a set of chunks
// Ensure that the chunks are on newline boundaries
func chunkData(data []byte, numchunks int) [][]byte {
	chunksize := len(data) / numchunks

	chunks := make([][]byte, 0)
	start := 0
	end := start + chunksize - 1
	for start < len(data) {
		if end >= len(data) {
			end = len(data) - 1
		}
		// Look forward for the next newline
		for end < len(data) && data[end] != '\n' {
			end++
		}
		chunk := data[start:end]
		chunks = append(chunks, chunk)

		start = end + 1
		end = start + chunksize - 1
	}
	return chunks
}

// Constants for FNV Hashing
// https://github.com/golang/go/blob/master/src/hash/fnv/fnv.go
const (
	offset32 = 2166136261
	prime32  = 16777619
)

// Parse the next line of the input
// This should be allocation free
// This expects well formatted input for performance reasons
func parseLine(data []byte, pos int) ([]byte, uint32, int, int) {
	start := pos

	// Parse the city name
	//   Advance to the position of the semicolon
	//   Calculate the FNV hash at the same time
	var hash uint32 = offset32
	value := data[pos]
	for value != ';' {
		hash = (hash ^ uint32(value)) * prime32
		pos++
		value = data[pos]
	}

	city := data[start:pos]

	// Skip over the semicolon
	pos++

	// Look for the negative sign
	sign := 1
	if data[pos] == '-' {
		sign = -1
		pos++
	}

	// Parse the temperature whole number part
	//   Advance to the position of the dot
	var whole byte = 0
	value = data[pos]
	for value != '.' {
		whole *= 10
		whole += value - '0'
		pos++
		value = data[pos]
	}

	// Skip over the dot
	pos++

	// Add in the decimal value and the sign
	//   Use the fact that there is only one decimal precision to represent as an int
	//   This allows for integer arithmetic instead of floating points
	number := sign * ((int(whole) * 10) + int(data[pos]-'0'))

	// Skip over the decimal value and the newline
	pos += 2

	return city, hash, number, pos
}

// Parse a chunk of data and return all the statistics on a result channel
func parseChunk(data []byte, result chan<- *Stations) {
	stations := NewStations()
	var city []byte
	var hash uint32
	var mnum int
	var pos int = 0
	for pos < len(data) {
		city, hash, mnum, pos = parseLine(data, pos)

		index, st := stations.Get(hash, city)
		if st == nil {
			st = &Station{city: city, hash: hash, count: 1, sum: mnum, max: mnum, min: mnum}
			stations.Append(index, st)
		} else {
			st.AddMeasurement(mnum)
		}
	}
	result <- stations
}

// Format the output as expected by the description
func formatOutput(totals *Stations) string {
	totals.Sort()
	output := make([]string, totals.Count())
	for i, st := range totals.All() {
		key := st.city
		minimum := float32(st.min) / 10.0
		mean := float32(st.sum) / float32(st.count) / 10.0
		maximum := float32(st.max) / 10.0
		output[i] = fmt.Sprintf("%s=%.1f/%.1f/%.1f", key, minimum, mean, maximum)
	}
	return fmt.Sprintf("{%s}", strings.Join(output, ", "))
}

func run() error {
	debug.SetGCPercent(-1) // disable garbage collection
	//debug.SetMemoryLimit(math.MaxInt64)

	// Enable cpu profiling
	// cpuf, _ := os.Create("cpu.prof")
	// defer cpuf.Close()
	// pprof.StartCPUProfile(cpuf)
	// defer pprof.StopCPUProfile()

	// Open and mmap the file
	file, err := os.Open(os.Args[1])
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file stats: %w", err)
	}

	filesize := fileinfo.Size()

	data, err := syscall.Mmap(int(file.Fd()), 0, int(filesize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error mmaping file: %w", err)
	}
	defer syscall.Munmap(data)

	// Chunk the data
	numchunks := runtime.NumCPU()
	// numchunks = 1
	chunks := chunkData(data, numchunks)

	// Parse the chunks as separate goroutines
	results := make(chan *Stations)
	for _, chunk := range chunks {
		go parseChunk(chunk, results)
	}

	// Combine the results
	totals := NewStations()
	for range chunks {
		totals.Combine(<-results)
	}

	// Take a memory snapshot
	// memf, _ := os.Create("mem.prof")
	// defer memf.Close()
	// pprof.WriteHeapProfile(memf)

	// fmt.Fprintln(os.Stderr, "Collisions:", HashTableCollisions, "Expansions:", HashTableExpansions)

	// Show the output
	output := formatOutput(totals)
	fmt.Println(output)

	return nil
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}
