#pragma once

#include <arrow/table.h>
#include <memory>

struct ParquetTables {
    using ArrowTable = std::shared_ptr<arrow::Table>;

    // start: table-defs
    // Generated for TPC-H
    ArrowTable customer;
    ArrowTable orders;
    ArrowTable lineitem;
    ArrowTable part;
    ArrowTable partsupp;
    ArrowTable supplier;
    ArrowTable nation;
    ArrowTable region;
    // end: table-defs
};


ParquetTables* load(std::string);
