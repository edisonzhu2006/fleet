#pragma once
// Shutdown signal handling shared by fleetmaster and fleetlet.
//
// WHY THIS EXISTS. Both binaries must stop cleanly on Ctrl-C or SIGTERM
// (fleetlet has to kill its tasks' process groups on the way out, or they
// outlive it). A classic signal handler may only call async-signal-safe
// functions, so it could not call server.shutdown(). Instead the signals
// are blocked in every thread (the mask is inherited by threads created
// afterwards, including gRPC's) and the main thread waits for them with
// sigwait(), which returns the signal as an ordinary function result.
// After that, any code is safe to run.

#include <csignal>

namespace fleet {

inline sigset_t shutdown_signals() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  return set;
}

// Call first thing in main(), before any thread is created.
inline void block_shutdown_signals() {
  sigset_t set = shutdown_signals();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

// Blocks until SIGINT or SIGTERM arrives; returns which.
inline int wait_for_shutdown_signal() {
  sigset_t set = shutdown_signals();
  int sig = 0;
  sigwait(&set, &sig);
  return sig;
}

// A fleetmaster replica (m4b) also takes SIGUSR1 and SIGUSR2: isolate it
// from the other replicas, and lift that, for the partition test. Same
// scheme: blocked everywhere, received by sigwait in main.
inline sigset_t control_signals() {
  sigset_t set = shutdown_signals();
  sigaddset(&set, SIGUSR1);
  sigaddset(&set, SIGUSR2);
  return set;
}

inline void block_control_signals() {
  sigset_t set = control_signals();
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

inline int wait_for_control_signal() {
  sigset_t set = control_signals();
  int sig = 0;
  sigwait(&set, &sig);
  return sig;
}

}  // namespace fleet
