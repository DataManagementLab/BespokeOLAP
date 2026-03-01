#pragma once

#include "plugin.hpp"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <poll.h>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <sys/signalfd.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <type_traits>
#include <unistd.h>

namespace ipc {

struct io_error : std::runtime_error {
    using std::runtime_error::runtime_error;
};

template <class T>
void read_exact(int fd, T& out) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(std::is_standard_layout_v<T>, "T must be standard layout");

    std::byte* p = reinterpret_cast<std::byte*>(&out);
    size_t off = 0;

    while (off < sizeof(T)) {
        ssize_t r = ::read(fd, p + off, sizeof(T) - off);
        if (r > 0) {
            off += static_cast<size_t>(r);
        } else if (r == 0) {
            throw io_error("read_exact: unexpected EOF");
        } else if (errno != EINTR) {
            throw io_error(std::string("read_exact: ") + std::strerror(errno));
        }
    }
}

template <class T>
void write_exact(int fd, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(std::is_standard_layout_v<T>, "T must be standard layout");

    const std::byte* p = reinterpret_cast<const std::byte*>(&value);
    size_t off = 0;

    while (off < sizeof(T)) {
        ssize_t w = ::write(fd, p + off, sizeof(T) - off);
        if (w > 0) {
            off += static_cast<size_t>(w);
        } else if (w == 0) {
            throw io_error("write_exact: wrote 0 bytes");
        } else if (errno != EINTR) {
            throw io_error(std::string("write_exact: ") + std::strerror(errno));
        }
    }
}

} // namespace ipc

enum class Action {
    RUN,
    TERMINATE,
};

struct DoneToken {
    int exit_code = 0;
    int term_signal = 0;
};

enum class RunPolicy {
    OnChange,
    Always,
};


namespace detail {

struct Message {
    Action action;
};

struct ChildHandle {
    pid_t pid = -1;
    int write_fd = -1;
    int pid_fd = -1;
};

template <class T>
struct compute_traits;

template <class C, class R, class Arg0>
struct compute_traits<R (C::*)(Arg0) const> {
    using input_type = void;
};

template <class C, class R, class Arg0, class Arg1>
struct compute_traits<R (C::*)(Arg0, Arg1) const> {
    using input_type = Arg1;
};

template <class Compute>
using compute_input_t = typename compute_traits<decltype(&Compute::operator())>::input_type;

struct NoInput {};

template <class Compute, class Input>
struct compute_result_type {
    using type = std::invoke_result_t<Compute, Plugin&, Input>;
};

template <class Compute>
struct compute_result_type<Compute, NoInput> {
    using type = std::invoke_result_t<Compute, Plugin&>;
};

template <class Compute, class Input>
using compute_result_t = typename compute_result_type<Compute, Input>::type;

template <class Compute>
static auto compute_result(Compute& compute, Plugin& plugin, const NoInput&) {
    return compute(plugin);
}

template <class Compute, class Input>
static auto compute_result(Compute& compute, Plugin& plugin, const Input& input) {
    return compute(plugin, input);
}

template <RunPolicy P, class Input, class Compute>
struct StageDef {
    using input_type = Input;
    static constexpr RunPolicy policy = P;
    const char* so_path;
    Compute compute;
};

template <RunPolicy P, class Compute>
static StageDef<P, detail::compute_input_t<Compute>, Compute> make_stage(
    const char* so_path,
    Compute compute) {
    return StageDef<P, detail::compute_input_t<Compute>, Compute>{so_path, compute};
}

static void stop_child(ChildHandle& child) {
    if (child.pid > 0) {
        Message msg{.action = Action::TERMINATE};
        try {
            ipc::write_exact(child.write_fd, msg);
        } catch (const std::exception&) {
        }
        close(child.write_fd);
        waitpid(child.pid, nullptr, 0);
    }
    if (child.pid_fd >= 0) {
        close(child.pid_fd);
        child.pid_fd = -1;
    }
    child.pid = -1;
    child.write_fd = -1;
}

struct StatusInfo {
    int exit_code = 0;
    int term_signal = 0;
};

static StatusInfo status_to_info(int status) {
    StatusInfo info{};
    if (WIFSIGNALED(status)) {
        info.term_signal = WTERMSIG(status);
        return info;
    }
    if (WIFEXITED(status)) {
        info.exit_code = WEXITSTATUS(status);
        return info;
    }
    info.exit_code = -1;
    return info;
}

static void write_done(int done_fd, StatusInfo info) {
    if (done_fd < 0)
        return;
    DoneToken token{info.exit_code, info.term_signal};
    ipc::write_exact(done_fd, token);
}

static bool reap_dead_child(ChildHandle& child, int done_fd) {
    if (child.pid <= 0)
        return false;
    int status = 0;
    pid_t r = waitpid(child.pid, &status, WNOHANG);
    if (r <= 0)
        return false;
    write_done(done_fd, status_to_info(status));
    stop_child(child);
    return true;
}

static bool notify_child_run(ChildHandle& child, int done_fd) {
    if (child.pid <= 0)
        return false;
    Message run_msg{.action = Action::RUN};
    try {
        ipc::write_exact(child.write_fd, run_msg);
        return true;
    } catch (const std::exception&) {
        int status = 0;
        waitpid(child.pid, &status, 0);
        write_done(done_fd, status_to_info(status));
        stop_child(child);
        return false;
    }
}

static int setup_sigchld_fd() {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    if (sigprocmask(SIG_BLOCK, &mask, nullptr) != 0)
        return -1;
    return signalfd(-1, &mask, SFD_CLOEXEC);
}

static int open_pidfd(pid_t pid) {
#ifdef SYS_pidfd_open
    int fd = static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
    if (fd >= 0)
        return fd;
#endif
    return -1;
}

} // namespace detail

class PipelineControl {
public:
    PipelineControl(int write_fd, int done_fd, bool own_fds = false)
        : write_fd_(write_fd), done_fd_(done_fd), own_fds_(own_fds) {}

    PipelineControl(const PipelineControl&) = delete;
    PipelineControl& operator=(const PipelineControl&) = delete;

    PipelineControl(PipelineControl&& other) noexcept
        : write_fd_(other.write_fd_), done_fd_(other.done_fd_), own_fds_(other.own_fds_) {
        other.write_fd_ = -1;
        other.done_fd_ = -1;
        other.own_fds_ = false;
    }

    PipelineControl& operator=(PipelineControl&& other) noexcept {
        if (this != &other) {
            close();
            write_fd_ = other.write_fd_;
            done_fd_ = other.done_fd_;
            own_fds_ = other.own_fds_;
            other.write_fd_ = -1;
            other.done_fd_ = -1;
            other.own_fds_ = false;
        }
        return *this;
    }

    ~PipelineControl() { close(); }

    void send_run() const {
        detail::Message msg{.action = Action::RUN};
        ipc::write_exact(write_fd_, msg);
    }

    void send_terminate() const {
        detail::Message msg{.action = Action::TERMINATE};
        ipc::write_exact(write_fd_, msg);
    }

    DoneToken read_done() const {
        DoneToken token{};
        ipc::read_exact(done_fd_, token);
        return token;
    }

    void close() noexcept {
        if (!own_fds_)
            return;
        if (write_fd_ >= 0) {
            ::close(write_fd_);
            write_fd_ = -1;
        }
        if (done_fd_ >= 0) {
            ::close(done_fd_);
            done_fd_ = -1;
        }
        own_fds_ = false;
    }

private:
    int write_fd_ = -1;
    int done_fd_ = -1;
    bool own_fds_ = false;
};

template <RunPolicy P, class Compute>
static auto stage(const char* so_path, Compute compute) {
    return detail::make_stage<P>(so_path, compute);
}

template <RunPolicy P, bool Done, class Input, class Compute, class NextStart>
static void stage_loop_impl(
    int read_fd,
    int done_fd,
    const char* so_path,
    Input input,
    Compute compute,
    NextStart next_start,
    bool start_now) {
    Plugin plugin(so_path);
    using Output = detail::compute_result_t<Compute, Input>;
    if constexpr (Done) {
        static_assert(std::is_convertible_v<Output, int>,
                      "DoneToken requires last stage output convertible to int");
    }
    Output result{};
    bool has_run = false;
    detail::ChildHandle child;
    bool child_active = false;
    int sigfd = -1;
    if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
        sigfd = detail::setup_sigchld_fd();
        if (sigfd < 0) {
            throw std::runtime_error("setup_sigchld_fd failed");
        }
    }

    auto do_run = [&]() {
        bool reload = plugin.needs_reload();
        bool should_run = reload || P == RunPolicy::Always || !has_run;
        if (reload) {
            if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
                if (child_active) {
                    detail::stop_child(child);
                    child_active = false;
                }
            }
            plugin.reload();
        }
        if (should_run) {
            result = detail::compute_result(compute, plugin, input);
            has_run = true;
            if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
                child = next_start(result, read_fd);
                child_active = true;
            }
        } else if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
            if (has_run && !child_active) {
                child = next_start(result, read_fd);
                child_active = true;
            }
        }
        if constexpr (Done) {
            detail::write_done(done_fd, detail::StatusInfo{static_cast<int>(result), 0});
        }
        if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
            if (child_active)
                detail::notify_child_run(child, done_fd);
        }
    };

    if (start_now) {
        do_run();
    }

    if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
        while (true) {
            struct pollfd fds[3];
            int nfds = 0;
            fds[nfds].fd = read_fd;
            fds[nfds].events = POLLIN;
            nfds++;
            if (child_active) {
                if (child.pid_fd >= 0) {
                    fds[nfds].fd = child.pid_fd;
                    fds[nfds].events = POLLIN;
                    nfds++;
                } else {
                    fds[nfds].fd = sigfd;
                    fds[nfds].events = POLLIN;
                    nfds++;
                }
            }
            int poll_rc = poll(fds, nfds, -1);
            if (poll_rc < 0) {
                if (errno == EINTR)
                    continue;
                throw ipc::io_error(std::string("poll: ") + std::strerror(errno));
            }
            for (int i = 0; i < nfds; ++i) {
                if (!(fds[i].revents & POLLIN))
                    continue;
                if (fds[i].fd == read_fd) {
                    detail::Message msg;
                    ipc::read_exact(read_fd, msg);
                    switch (msg.action) {
                        case Action::RUN: {
                            if (child_active && detail::reap_dead_child(child, done_fd)) {
                                child_active = false;
                            }
                            do_run();
                            if (child_active && detail::reap_dead_child(child, done_fd)) {
                                child_active = false;
                            }
                            break;
                        }
                        case Action::TERMINATE:
                            if (child_active)
                                detail::stop_child(child);
                            std::cerr << so_path << " child terminates\n";
                            _exit(0);
                        default:
                            throw std::runtime_error("unknown action");
                    }
                    continue;
                }
                if (child_active && fds[i].fd == sigfd) {
                    struct signalfd_siginfo info{};
                    ipc::read_exact(sigfd, info);
                    if (detail::reap_dead_child(child, done_fd)) {
                        child_active = false;
                    }
                    continue;
                }
                if (child_active && fds[i].fd == child.pid_fd) {
                    if (detail::reap_dead_child(child, done_fd)) {
                        child_active = false;
                    }
                    continue;
                }
            }
        }
    } else {
        while (true) {
            detail::Message msg;
            ipc::read_exact(read_fd, msg);
            switch (msg.action) {
                case Action::RUN: {
                    if constexpr (!std::is_same_v<NextStart, std::nullptr_t>) {
                        if (child_active && detail::reap_dead_child(child, done_fd)) {
                            child_active = false;
                        }
                    }
                    do_run();
                    if (child_active && detail::reap_dead_child(child, done_fd)) {
                        child_active = false;
                    }
                    break;
                }
                case Action::TERMINATE:
                    std::cerr << so_path << " child terminates\n";
                    _exit(0);
                default:
                    throw std::runtime_error("unknown action");
            }
        }
    }
}

template <RunPolicy P, class Input, class Compute, class NextStart>
static detail::ChildHandle start_stage_child(
    const char* so_path,
    Input input,
    Compute compute,
    NextStart next_start,
    int done_fd,
    int close_fd) {
    int pipe_fd[2];
    if (pipe(pipe_fd) == -1) {
        perror("pipe");
        return {};
    }
    pid_t pid = fork();
    if (pid == 0) {
        close(pipe_fd[1]);
        if (close_fd >= 0)
            close(close_fd);
        stage_loop_impl<P, false>(
            pipe_fd[0],
            done_fd,
            so_path,
            input,
            compute,
            next_start,
            false);
        _exit(0);
    }
    if (pid < 0) {
        perror("fork");
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        return {};
    }
    close(pipe_fd[0]);
    detail::ChildHandle child{pid, pipe_fd[1], -1};
    child.pid_fd = detail::open_pidfd(pid);
    return child;
}

template <class Stage, class Next>
struct Pipeline {
    Stage stage_def;
    Next next;

    template <bool PropagateDone>
    auto make_next_start(int done_fd) {
        if constexpr (std::is_same_v<Next, std::nullptr_t>) {
            return nullptr;
        } else if constexpr (PropagateDone) {
            return [this, done_fd](auto output, int parent_fd) {
                return next.start(output, parent_fd, done_fd);
            };
        } else {
            return [this](auto output, int parent_fd) {
                (void)parent_fd;
                return next.start(output, parent_fd);
            };
        }
    }

    template <bool EmitDone, bool PropagateDone, bool StartNow, class Input>
    void run_impl(int read_fd, int done_fd, Input input) {
        auto next_start = make_next_start<PropagateDone>(done_fd);
        stage_loop_impl<Stage::policy, EmitDone>(
            read_fd,
            done_fd,
            stage_def.so_path,
            input,
            stage_def.compute,
            next_start,
            StartNow);
    }

    template <bool EmitDone, bool PropagateDone, class Input>
    detail::ChildHandle start_child_impl(Input input, int done_fd, int close_fd) {
        if constexpr (std::is_same_v<Next, std::nullptr_t>) {
            int pipe_fd[2];
            if (pipe(pipe_fd) == -1) {
                perror("pipe");
                return {};
            }
            pid_t pid = fork();
            if (pid == 0) {
                close(pipe_fd[1]);
                if (close_fd >= 0)
                    close(close_fd);
                stage_loop_impl<Stage::policy, EmitDone>(
                    pipe_fd[0],
                    done_fd,
                    stage_def.so_path,
                    input,
                    stage_def.compute,
                    nullptr,
                    false);
                _exit(0);
            }
            if (pid < 0) {
                perror("fork");
                close(pipe_fd[0]);
                close(pipe_fd[1]);
                return {};
            }
            close(pipe_fd[0]);
            detail::ChildHandle child{pid, pipe_fd[1], -1};
            child.pid_fd = detail::open_pidfd(pid);
            return child;
        } else {
            auto next_start = make_next_start<PropagateDone>(done_fd);
            return start_stage_child<Stage::policy>(
                stage_def.so_path,
                input,
                stage_def.compute,
                next_start,
                done_fd,
                close_fd);
        }
    }

    template <class Input>
    void run(int read_fd, Input input, int done_fd = -1, bool start_now = false) {
        constexpr bool emit_done = std::is_same_v<Next, std::nullptr_t>;
        const bool propagate_done = done_fd >= 0;
        if (propagate_done) {
            if (start_now)
                run_impl<emit_done, true, true>(read_fd, done_fd, input);
            else
                run_impl<emit_done, true, false>(read_fd, done_fd, input);
        } else {
            if (start_now)
                run_impl<false, false, true>(read_fd, -1, input);
            else
                run_impl<false, false, false>(read_fd, -1, input);
        }
    }

    void run(int read_fd, int done_fd = -1, bool start_now = false) {
        static_assert(std::is_same_v<typename Stage::input_type, void>,
                      "first stage requires input");
        run(read_fd, detail::NoInput{}, done_fd, start_now);
    }

    template <class Input>
    auto start(Input input, int close_fd = -1, int done_fd = -1) {
        constexpr bool emit_done = std::is_same_v<Next, std::nullptr_t>;
        const bool propagate_done = done_fd >= 0;
        if (propagate_done)
            return start_child_impl<emit_done, true>(input, done_fd, close_fd);
        return start_child_impl<false, false>(input, -1, close_fd);
    }
};

template <class Node>
struct all_always : std::false_type {};

template <>
struct all_always<std::nullptr_t> : std::true_type {};

template <class Stage, class Next>
struct all_always<Pipeline<Stage, Next>>
    : std::bool_constant<Stage::policy == RunPolicy::Always && all_always<Next>::value> {};

static std::nullptr_t make_pipeline() { return nullptr; }

template <class Stage, class... Rest>
static auto make_pipeline(Stage stage_def, Rest... rest) {
    auto next = make_pipeline(rest...);
    using PipelineT = Pipeline<Stage, decltype(next)>;
    static_assert(Stage::policy != RunPolicy::Always || all_always<decltype(next)>::value,
                  "RunPolicy::Always requires all downstream stages to be RunPolicy::Always");
    return PipelineT{stage_def, next};
}
