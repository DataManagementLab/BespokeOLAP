#pragma once

#include <string>

struct ParquetTables;

ParquetTables* load(std::string);

struct LoaderApi {
    ParquetTables* (*load)(std::string);
};
