package main

import (
	"fmt"
	"testing"
)

func TestChunkFile(t *testing.T) {
	var tests = []struct {
		input     string
		numchunks int
		output    []string
	}{
		{"asdf\nqwer\n", 2, []string{"asdf", "qwer"}},
		{"asdf\nqwer", 2, []string{"asdf", "qwer"}},
		{"asdf\nqwer\nhjkl", 2, []string{"asdf\nqwer", "hjkl"}},
		{"asdf\nqwer\nhjkl\n", 2, []string{"asdf\nqwer", "hjkl"}},
	}

	for _, tt := range tests {
		testname := fmt.Sprintf("TestChunkFile:%s", tt.input)
		t.Run(testname, func(t *testing.T) {
			output := chunkData([]byte(tt.input), tt.numchunks)
			if len(output) != len(tt.output) {
				t.Errorf("len mismatch %d", len(output))
				for _, o := range output {
					fmt.Println(o)
				}
				return
			}
			for i := 0; i < len(output); i++ {
				if string(output[i]) != tt.output[i] {
					t.Errorf("output mismatch %d %s %v", i, output[i], output[i])
				}
			}
		})
	}
}

func TestParseLine(t *testing.T) {
	var tests = []struct {
		input, city string
		num         int
		left        string
	}{
		{"Baltimore;-12.3\n", "Baltimore", -123, ""},
		{"Baltimore;12.3\n", "Baltimore", 123, ""},
		{"Baltimore;2.3\n", "Baltimore", 23, ""},
		{"Baltimore;-2.3\n", "Baltimore", -23, ""},
		{"Baltimore;-12.3\nblah", "Baltimore", -123, "blah"},
	}

	for _, tt := range tests {
		testname := fmt.Sprintf("TestParseLine:%s", tt.input)
		t.Run(testname, func(t *testing.T) {
			city, _, num, pos := parseLine([]byte(tt.input), 0)
			left := tt.input[pos:]
			if string(city) != tt.city {
				t.Errorf("city mismatch %s", city)
			}
			if num != tt.num {
				t.Errorf("num mismatch %d", num)
			}
			if string(left) != tt.left {
				t.Errorf("left mismatch %d", num)
			}
		})
	}
}
