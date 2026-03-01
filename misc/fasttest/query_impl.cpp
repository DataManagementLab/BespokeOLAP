#include "query_impl.hpp"

#include <iostream>
#include <sstream>
#include <string>

#include "args_parser.hpp"


void query(Database*) {
    std::vector<QueryRequest> requests;
    std::string line;
    while (std::getline(std::cin, line)) {
        // empty line signals end of input - stop query input loop (terminate execution)
        if (line.empty()) {
            break;
        }
        std::istringstream iss(line);
        std::string query_id =  "0";
        iss >> query_id;
        if (!iss) {
            continue;
        }
        requests.push_back(QueryRequest{query_id, line});
    }

    // TODO: implement the query execution logic on the Database - use arg parser example code below
    for (auto& x : requests) {
        std::cout << "Parsed: Q" << x.id << " args=" << x.line << "\n";
    }
}

//<<example parser call code>>