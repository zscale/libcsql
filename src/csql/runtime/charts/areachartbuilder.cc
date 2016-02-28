/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/charts/areachartbuilder.h>
#include <csql/runtime/charts/drawstatement.h>
#include <cplot/areachart.h>

namespace csql {

AreaChartBuilder::AreaChartBuilder(
    stx::chart::Canvas* canvas,
    RefPtr<DrawStatementNode> draw_stmt) :
    ChartBuilder(canvas, draw_stmt) {}

stx::chart::Drawable* AreaChartBuilder::getChart() const {
  preconditionCheck();

  if (auto c = tryType2D<stx::chart::AreaChart2D<
        SValue::TimeType,
        SValue::FloatType>>())
    return c;

  if (auto c = tryType2D<stx::chart::AreaChart2D<
        SValue::FloatType,
        SValue::FloatType>>())
    return c;

  if (auto c = tryType2D<stx::chart::AreaChart2D<
        SValue::StringType,
        SValue::FloatType>>())
    return c;

  if (auto c = tryType3D<stx::chart::AreaChart3D<
        SValue::TimeType,
        SValue::FloatType,
        SValue::FloatType>>())
    return c;

  if (auto c = tryType3D<stx::chart::AreaChart3D<
        SValue::FloatType,
        SValue::FloatType,
        SValue::FloatType>>())
    return c;

  if (auto c = tryType3D<stx::chart::AreaChart3D<
        SValue::StringType,
        SValue::FloatType,
        SValue::FloatType>>())
    return c;

  invalidType();
  return nullptr;
}

std::string AreaChartBuilder::chartName() const {
  return "AreaChart";
}

}
