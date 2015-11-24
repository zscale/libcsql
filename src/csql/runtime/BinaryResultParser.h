/**
 * This file is part of the "libstx" project
 *   Copyright (c) 2015 Paul Asmuth, FnordCorp B.V.
 *
 * libstx is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stx/uri.h>
#include <stx/io/file.h>
#include <stx/autoref.h>
#include <stx/http/httpmessage.h>
#include "stx/http/httprequest.h"
#include "stx/http/httpresponse.h"
#include "stx/http/httpstats.h"
#include "stx/http/httpconnectionpool.h"
#include "stx/http/httpclient.h"

namespace csql {

class BinaryResultParser : public stx::RefCounted {
public:

  void onEvent(stx::Function<void ()> fn);

  void parse(const char* data, size_t size);

protected:

  size_t parseTableHeader(const void* data, size_t size);
  size_t parseRow(const void* data, size_t size);
  size_t parseProgress(const void* data, size_t size);
  size_t parseError(const void* data, size_t size);

  stx::Buffer buf_;
  stx::Function<void ()> on_event_;
};

}
