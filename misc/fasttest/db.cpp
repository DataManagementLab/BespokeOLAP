#include "builder_api.hpp"
#include "loader_api.hpp"
#include "query_api.hpp"
#include "utils/pipeline.hpp"

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <stdexcept>

struct State {
    std::string parquet_path;
    ParquetTables* parquet_tables;
    Database* database;
};

static State state;

static auto build_pipeline() {
    return make_pipeline(
        stage<RunPolicy::OnChange>("./build/libloader.so", [](Plugin& plugin) {
            auto api = plugin.get<LoaderApi>();
            std::cerr << "loader start\n";
            state.parquet_tables = api.load(state.parquet_path);
            std::cerr << "loader done\n";
            return 0;
        }),
        stage<RunPolicy::OnChange>("./build/libbuilder.so", [](Plugin& plugin, int) {
            auto api = plugin.get<BuilderApi>();
            std::cerr << "builder start\n";
            const auto t0 = std::chrono::steady_clock::now();
            state.database = api.build(state.parquet_tables);
            std::cerr << "builder done\n";
            const auto t1 = std::chrono::steady_clock::now();
            const float ms =
                std::chrono::duration<float, std::milli>(t1 - t0).count();
            std::cerr << "Ingest ms: " << ms << "\n";
            return 0;
        }),
        stage<RunPolicy::Always>("./build/libquery.so", [](Plugin& plugin, int) {
            auto api = plugin.get<QueryApi>();
            std::cerr << "query start\n";
            api.query(state.database);
            std::cerr << "query done\n";
            return 0;
        }));
}

static void run_child(int read_fd, int done_fd) {
    auto pipeline = build_pipeline();
    pipeline.run(read_fd, done_fd, false);
}

static int getenv_fd(const char* name) {
    const char* v = std::getenv(name);
    if (!v) {
        throw std::runtime_error(std::string(name) + " not supplied");
    }
    return std::atoi(v);
}


static void run_parent(PipelineControl& control) {
    int in_fd = getenv_fd("P2C_FD");  // read from parent
    int out_fd = getenv_fd("C2P_FD"); // write to parent

    std::ifstream in("/proc/self/fd/" + std::to_string(in_fd));
    if (!in.is_open()) {
        throw std::runtime_error("open P2C_FD failed");
    }
    std::ofstream out("/proc/self/fd/" + std::to_string(out_fd));
    if (!out.is_open()) {
        throw std::runtime_error("open C2P_FD failed");
    }

    std::string cmd;
    while (std::getline(in, cmd)) {
        std::cout << "got: " << cmd << "\n";

        if (cmd == "stop") {
            break;
        }
        if (cmd != "run") {
            throw std::runtime_error("invalid command");
        }

        control.send_run();
        DoneToken token = control.read_done();
        std::cerr << "exit_code: " << token.exit_code << " signal: " << token.term_signal
                  << "\n";
        out << "exit_code: " << token.exit_code << " signal: " << token.term_signal
            << "\n";
        out.flush();
    }

    control.send_terminate();
}


int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <PARQUET_DIR\n";
        return 1;
    }
    std::string base_parquet = argv[1];
    state.parquet_path = base_parquet;

    signal(SIGPIPE, SIG_IGN);
    int p2c[2];
    int done_pipe[2];
    if (pipe(p2c) == -1) {
        perror("pipe");
        return 1;
    }
    if (pipe(done_pipe) == -1) {
        perror("pipe");
        close(p2c[0]);
        close(p2c[1]);
        return 1;
    }

    pid_t pid = fork();
    if (pid == 0) {
        close(p2c[1]);
        close(done_pipe[0]);
        run_child(p2c[0], done_pipe[1]);
        _exit(0);
    }
    if (pid < 0) {
        perror("fork");
        close(p2c[0]);
        close(p2c[1]);
        close(done_pipe[0]);
        close(done_pipe[1]);
        return 1;
    }

    close(p2c[0]);
    close(done_pipe[1]);
    PipelineControl control(p2c[1], done_pipe[0], true);
    run_parent(control);
    waitpid(pid, nullptr, 0);
    return 0;
}
