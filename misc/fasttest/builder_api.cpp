#include "builder_api.hpp"

#include "utils/plugin_base.hpp"

static const BuilderApi BUILDER = {
    .build = &build,
};

extern "C" __attribute__((visibility("default")))
const void*
plugin_query() {
    return &BUILDER;
}
