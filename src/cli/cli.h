/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */

#ifndef _FNORDMETRIC_CLI_H
#define _FNORDMETRIC_CLI_H
#include <fnordmetric/util/outputstream.h>
#include <fnordmetric/util/runtimeexception.h>
#include <fnordmetric/query/queryservice.h>

namespace fnordmetric {
class Environment;
namespace cli {
class FlagParser;

class CLI {
public:
  /**
   * Parse a command line
   */
  static void parseArgs(Environment* env, const std::vector<std::string>& argv);

  /**
   * Execute a command line but do not throw exceptions
   */
  static int executeSafely(Environment* env);

  /**
   * Execute a command line
   */
  static void execute(Environment* env);


protected:

  static const query::QueryService::kFormat getOutputFormat(Environment* env);

};

}
}
#endif
