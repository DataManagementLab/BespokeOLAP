#pragma once


struct Database;


void query(Database*);

struct QueryApi {
    void (*query)(Database*);
};
