#include "query_api.hpp"

#include "utils/plugin_base.hpp"


static const QueryApi QUERY = {
    .query = &query,
};

extern "C" __attribute__((visibility("default")))
const void*
plugin_query() {
    return &QUERY;
}
