CXX      := g++
PKG_CONFIG ?= pkg-config
CXXFLAGS := -std=c++20 -O3 -Wall -Wextra -g
TARGETS  := shm_test perf_test parquet_reader_test
SRC_shm_test  := shm_test.cpp
SRC_perf_test := perf_test.cpp
SRC_parquet_reader_test := parquet_reader_test.cpp

ARROW_PARQUET_CFLAGS := $(shell $(PKG_CONFIG) --cflags arrow parquet)
ARROW_PARQUET_LIBS   := $(shell $(PKG_CONFIG) --libs arrow parquet)

.PHONY: all clean

all: $(TARGETS)

shm_test: $(SRC_shm_test)
	$(CXX) $(CXXFLAGS) $< -o $@

perf_test: $(SRC_perf_test)
	$(CXX) $(CXXFLAGS) $< -o $@


parquet_reader_test: $(SRC_parquet_reader_test)
	$(CXX) $(CXXFLAGS) $(ARROW_PARQUET_CFLAGS) $< -o $@ $(ARROW_PARQUET_LIBS)

clean:
	rm -f $(TARGETS)
