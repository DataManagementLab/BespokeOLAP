#pragma once

struct ParquetTables;
struct Database;

Database* build(ParquetTables*);


struct BuilderApi {
    Database* (*build)(ParquetTables*);
};
