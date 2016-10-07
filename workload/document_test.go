package workload

import (
	"testing"
)

func TestDocSizeGeneratorInvalidDistribution(t *testing.T) {

	tests := []struct {
		dist DocSizeDistribution
	}{
		{
			dist: DocSizeDistribution{},
		},
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    10,
					MinSize: 128,
					MaxSize: 1024,
				},
			},
		},
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    101,
					MinSize: 128,
					MaxSize: 1024,
				},
			},
		},
	}

	for _, test := range tests {
		_, err := NewDocSizeGenerator(test.dist)
		if err == nil {
			t.Errorf("Exptected error for invalid distribution, got nil")
		}
	}
}

func TestDocSizeGeneratorUniform(t *testing.T) {

	tests := []struct {
		dist DocSizeDistribution
		size int
	}{
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    100,
					MinSize: 64,
					MaxSize: 64,
				},
			},
			size: 64,
		},
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    100,
					MinSize: 130048,
					MaxSize: 130048,
				},
			},
			size: 130048,
		},
	}

	for _, test := range tests {

		dsg, _ := NewDocSizeGenerator(test.dist)
		// try each one 100 times
		for count := 0; count < 100; count++ {
			size := dsg.NextDocSize()
			if size != test.size {
				t.Errorf("Expected size %d, got %d, on count %d", test.size, size, count)
			}
		}
	}

}

func TestDocSizeGeneratorVaried(t *testing.T) {

	tests := []struct {
		dist    DocSizeDistribution
		minSize int
		maxSize int
	}{
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    50,
					MinSize: 64,
					MaxSize: 64,
				},
				&DocSizeDistributionElement{
					Prob:    50,
					MinSize: 128,
					MaxSize: 128,
				},
			},
			minSize: 64,
			maxSize: 128,
		},
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    45,
					MinSize: 64,
					MaxSize: 64,
				},
				&DocSizeDistributionElement{
					Prob:    45,
					MinSize: 128,
					MaxSize: 128,
				},
				&DocSizeDistributionElement{
					Prob:    10,
					MinSize: 130048,
					MaxSize: 130048,
				},
			},
			minSize: 64,
			maxSize: 130048,
		},
	}

	for _, test := range tests {

		dsg, _ := NewDocSizeGenerator(test.dist)
		// try each one 100 times
		for count := 0; count < 100; count++ {
			size := dsg.NextDocSize()
			if size < test.minSize {
				t.Errorf("Expected size greater than or equal to %d, got %d, on count %d", test.minSize, size, count)
			}
			if size > test.maxSize {
				t.Errorf("Expected size less than or equal to %d, got %d, on count %d", test.maxSize, size, count)
			}
		}
	}

}

func TestDocSizeGeneratorMultiple(t *testing.T) {

	tests := []struct {
		dist       DocSizeDistribution
		validSizes []int
	}{
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    50,
					MinSize: 64,
					MaxSize: 64,
				},
				&DocSizeDistributionElement{
					Prob:    50,
					MinSize: 128,
					MaxSize: 128,
				},
			},
			validSizes: []int{64, 128},
		},
		{
			dist: DocSizeDistribution{
				&DocSizeDistributionElement{
					Prob:    45,
					MinSize: 64,
					MaxSize: 64,
				},
				&DocSizeDistributionElement{
					Prob:    45,
					MinSize: 128,
					MaxSize: 128,
				},
				&DocSizeDistributionElement{
					Prob:    10,
					MinSize: 130048,
					MaxSize: 130048,
				},
			},
			validSizes: []int{64, 128, 130048},
		},
	}

	for _, test := range tests {

		dsg, _ := NewDocSizeGenerator(test.dist)
		// try each one 100 times
	OUTER:
		for count := 0; count < 100; count++ {
			size := dsg.NextDocSize()
			for _, vs := range test.validSizes {
				if size == vs {
					continue OUTER
				}
			}
			t.Errorf("Expected size in %v, got %d, on count %d", test.validSizes, size, count)
		}
	}

}

func TestDocSizeGeneratorDistribution(t *testing.T) {

	dist := DocSizeDistribution{
		&DocSizeDistributionElement{
			Prob:    67,
			MinSize: 64,
			MaxSize: 64,
		},
		&DocSizeDistributionElement{
			Prob:    33,
			MinSize: 128,
			MaxSize: 128,
		},
	}

	dsg, _ := NewDocSizeGenerator(dist)

	dismap := make(map[int]int)
	for count := 0; count < 100; count++ {
		size := dsg.NextDocSize()
		existing, ok := dismap[size]
		if ok {
			dismap[size] = existing + 1
		} else {
			dismap[size] = 1
		}
	}
	if dismap[64] < dismap[128] {
		t.Errorf("expected more 64byte sizes (got %d) than 128byte (got %d)", dismap[64], dismap[128])
	}
}

func TestDocSizeGeneratorActuallyDistributed(t *testing.T) {

	dist := DocSizeDistribution{
		&DocSizeDistributionElement{
			Prob:    100,
			MinSize: 64,
			MaxSize: 96,
		},
	}

	dsg, _ := NewDocSizeGenerator(dist)

	dismap := make(map[int]int)
	for count := 0; count < 1000; count++ {
		size := dsg.NextDocSize()
		existing, ok := dismap[size]
		if ok {
			dismap[size] = existing + 1
		} else {
			dismap[size] = 1
		}
	}
	for i := 64; i < 96; i++ {
		if dismap[i] == 0 {
			t.Errorf("expected some values at size %d, got 0", i)
		}
	}
}
