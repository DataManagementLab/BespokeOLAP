#include "loader_impl.hpp"

#include "loader_utils.hpp"

#include <stdio.h>
#include <unistd.h>


ParquetTables* load(std::string path) {
    auto tables = new ParquetTables{};

    // start: table-reads
    // Generated for TPC-H
    tables->customer = ReadParquetTable(path + "customer.parquet");
    tables->orders = ReadParquetTable(path + "orders.parquet");
    tables->lineitem = ReadParquetTable(path + "lineitem.parquet");
    tables->part = ReadParquetTable(path + "part.parquet");
    tables->partsupp = ReadParquetTable(path + "partsupp.parquet");
    tables->supplier = ReadParquetTable(path + "supplier.parquet");
    tables->nation = ReadParquetTable(path + "nation.parquet");
    tables->region = ReadParquetTable(path + "region.parquet");
    // end: table-reads

    return tables;
}
