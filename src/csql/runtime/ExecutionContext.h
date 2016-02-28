/**
 * This file is part of the "libfnord" project
 *   Copyright (c) 2015 Paul Asmuth
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/stdtypes.h>
#include <stx/autoref.h>
#include <stx/thread/taskscheduler.h>
#include <csql/svalue.h>

using namespace stx;

namespace csql {

struct ExecutionStatus {
  ExecutionStatus();

  std::atomic<size_t> num_subtasks_total;
  std::atomic<size_t> num_subtasks_completed;
  std::atomic<size_t> num_rows_scanned;

  String toString() const;
  double progress() const;
};


class ExecutionContext : public RefCounted {
public:

  ExecutionContext(
      TaskScheduler* sched,
      size_t max_concurrent_tasks = 32);

  void onStatusChange(Function<void (const ExecutionStatus& status)> fn);

  void cancel();
  bool isCancelled() const;
  void onCancel(Function<void ()> fn);

  void incrNumSubtasksTotal(size_t n);
  void incrNumSubtasksCompleted(size_t n);
  void incrNumRowsScanned(size_t n);

  void runAsync(Function<void ()> fn);

  Option<String> cacheDir() const;
  void setCacheDir(const String& cachedir);

protected:

  void statusChanged();

  TaskScheduler* sched_;
  size_t max_concurrent_tasks_;
  Option<String> cachedir_;

  ExecutionStatus status_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  Function<void (const ExecutionStatus& status)> on_status_change_;
  Function<void ()> on_cancel_;
  bool cancelled_;
  size_t running_tasks_;
};

}
