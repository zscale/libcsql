/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <vector>
#include <string>
#include <memory>
#include <stx/exception.h>
#include <stx/exceptionhandler.h>
#include <stx/io/inputstream.h>
#include <stx/io/outputstream.h>
#include <stx/cli/flagparser.h>
#include "environment.h"
#include "cli/cli.h"
#include <csql/backends/csv/csvbackend.h>
#include <csql/backends/mysql/mysqlbackend.h>
#include <csql/backends/crate/cratebackend.h>

namespace csql {
namespace cli {

void CLI::parseArgs(Environment* env, const std::vector<std::string>& argv) {
  auto flags = env->flags();

  // Execute queries from the commandline:
  flags->defineFlag(
      "format",
      FlagParser::T_STRING,
      false,
      "f",
      "table",
      "The output format { svg, csv, table }",
      "<format>");

  flags->defineFlag(
      "output",
      FlagParser::T_STRING,
      false,
      "o",
      NULL,
      "Write output to a file",
      "<format>");

  flags->defineFlag(
      "verbose",
      FlagParser::T_SWITCH,
      false,
      NULL,
      NULL,
      "Be verbose");

  flags->defineFlag(
      "help",
      FlagParser::T_SWITCH,
      false,
      "h",
      NULL,
      "You are reading it...");

  flags->parseArgv(argv);
  env->setVerbose(flags->isSet("verbose"));
}

void CLI::parseArgs(Environment* env, int argc, const char** argv) {
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    args.emplace_back(argv[i]);
  }
  parseArgs(env, args);
}

void CLI::printUsage() {
  auto err_stream = stx::io::OutputStream::getStderr();
  err_stream->printf("usage: fnordmetric-cli [options] [file.sql]\n");
  err_stream->printf("\noptions:\n");
  env()->flags()->printUsage(err_stream.get());
  err_stream->printf("\nexamples:\n");
  err_stream->printf("    $ fnordmeric-cli -f svg -o out.svg myquery.sql\n");
  err_stream->printf("    $ fnordmeric-cli -f svg - < myquery.sql > out.svg\n");
}

int CLI::executeSafely(Environment* env) {
  auto err_stream = stx::io::OutputStream::getStderr();

  try {
    if (env->flags()->isSet("help")) {
      printUsage();
      return 0;
    }

    execute(env);
  } catch (const stx::Exception& e) {
    if (e.getTypeName() == "UsageError") {
      err_stream->printf("\n");
      printUsage();
      return 1;
    }

    if (env->verbose()) {
      env->logger()->exception("FATAL", "Fatal error", e);
    } else {
      auto msg = e.getMessage();
      err_stream->printf("[ERROR] ");
      err_stream->write(msg.c_str(), msg.size());
      err_stream->printf("\n");
    }

    return 1;
  }

  return 0;
}


void CLI::execute(Environment* env) {
  auto flags = env->flags();
  const auto& args = flags->getArgv();

  /* open input stream */
  std::shared_ptr<stx::io::InputStream> input;
  if (args.size() == 1) {
    if (args[0] == "-") {
      if (env->verbose()) {
        env->logger()->log("DEBUG", "Input from stdin");
      }

      input = std::move(stx::io::InputStream::getStdin());
    } else {
      if (env->verbose()) {
        env->logger()->log("DEBUG", "Input file: " + args[0]);
      }

      input = std::move(stx::io::FileInputStream::openFile(args[0]));
    }
  } else {
    RAISE("UsageError", "usage error");
  }

  /* open output stream */
  std::shared_ptr<stx::io::OutputStream> output;
  if (flags->isSet("output")) {
    auto output_file = flags->getString("output");

    if (env->verbose()) {
      env->logger()->log("DEBUG", "Output file: " + output_file);
    }

    output = std::move(stx::io::FileOutputStream::openFile(output_file));
  } else {
    if (env->verbose()) {
      env->logger()->log("DEBUG", "Output to stdout");
    }
    output = std::move(stx::io::OutputStream::getStdout());
  }

  /* execute query */
  csql::QueryService query_service;

  query_service.registerBackend(
      std::unique_ptr<csql::Backend>(
          new csql::mysql_backend::MySQLBackend));

  query_service.registerBackend(
      std::unique_ptr<csql::Backend>(
          new csql::csv_backend::CSVBackend));

  query_service.registerBackend(
      std::unique_ptr<csql::Backend>(
        new csql::crate_backend::CrateBackend));

  query_service.executeQuery(
      input,
      getOutputFormat(env),
      output);
}

const csql::QueryService::kFormat CLI::getOutputFormat(Environment* env) {
  auto flags = env->flags();

  if (!flags->isSet("format")) {
    return csql::QueryService::FORMAT_TABLE;
  }

  auto format_str = flags->getString("format");

  if (format_str == "csv") {
    return csql::QueryService::FORMAT_CSV;
  }

  if (format_str == "json") {
    return csql::QueryService::FORMAT_JSON;
  }

  if (format_str == "svg") {
    return csql::QueryService::FORMAT_SVG;
  }

  if (format_str == "table") {
    return csql::QueryService::FORMAT_TABLE;
  }

  RAISE(
      kRuntimeError,
      "invalid format: '%s', allowed formats are "
      "'svg', 'csv', 'json' and 'table'",
      format_str.c_str());
}

}
}
