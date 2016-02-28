/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#ifndef _FNORDMETRIC_SQLEXTENSIONS_DOMAINCONFIG_H
#define _FNORDMETRIC_SQLEXTENSIONS_DOMAINCONFIG_H
#include <algorithm>
#include <stdlib.h>
#include <assert.h>
#include <cplot/domain.h>
#include <cplot/drawable.h>
#include <csql/svalue.h>

namespace csql {

class DomainConfig {
public:

  /**
   * @param dimension 0 for x, 1 for y, etc
   */
  DomainConfig(
      stx::chart::Drawable* drawable,
      stx::chart::AnyDomain::kDimension dimension);

  void setMin(const SValue& value);
  void setMax(const SValue& value);
  void setInvert(bool invert);
  void setLogarithmic(bool logarithmic);

protected:
  stx::chart::AnyDomain* domain_;
  char dimension_letter_;
};

}
#endif
