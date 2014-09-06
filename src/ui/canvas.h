/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#ifndef _FNORDMETRIC_CANVAS_H
#define _FNORDMETRIC_CANVAS_H
#include <memory>
#include <vector>
#include <string>
#include <tuple>
#include <fnordmetric/ui/axisdefinition.h>
#include <fnordmetric/ui/drawable.h>

namespace fnordmetric {
namespace ui {
class RenderTarget;

class Canvas {
public:
  static const int kAxisPadding = 5; // FIXPAUL make configurable
  static const int kTickLength = 5; // FIXPAUL make configurable
  static const int kAxisLabelHeight = 35.0f; // FIXPAUL make configurable
  static const int kAxisLabelWidth = 50.0f; // FIXPAUL make configurable
  static const int kAxisTitleLength = 20.0f; // FIXPAUL make configurable

  Canvas();

  /**
   * FIXPAUL overcomplicated, just accept a ptr
   *
   * The returned pointer is owned by the canvas instance and must not be freed
   * by the caller.
   */
  template <typename ChartType, typename... Args>
  ChartType* addChart(Args... args) {
    auto drawable = new ChartType(this, args...);
    drawables_.emplace_back(drawable);
    return drawable;
  }

  /**
   * Add an axis to this canvas. Usually axes are not specified manually using
   * this method but through one of the Chart subclasses. However it is safe
   * to call this method to explicitly define a custom axis.
   *
   * The returned pointer is owned by the canvas instance and must not be freed
   * by the caller.
   */
   AxisDefinition* addAxis(AxisDefinition::kPosition position);

  /**
   * Render the contents of this canvas to the provided render target
   *
   * @param target a RenderTarget subclass instance. Does not transfer ownership
   */
  void render(RenderTarget* target) const;

  /**
   * FIXPAUL REMOVEME
   *
   * Render the contents of this canvas as a scalabale vector graphic and return
   * as string
   */
   std::string renderSVG() const;

protected:

  /**
   * Render the charts
   */
  void renderCharts(RenderTarget* target, Viewport* viewport) const;

  /**
   * Render the axes
   */
  void renderAxes(RenderTarget* target, Viewport* viewport) const;

  /**
   * Render a top axis
   *
   * @param target the render target
   * @param axis the axis definition
   * @param padding the padding state
   * @param top the top padding for this axis
   */
  void renderTopAxis(
      RenderTarget* target,
      Viewport* viewport,
      AxisDefinition* axis,
      int top) const;

  /**
   * Render a right axis
   *
   * @param target the render target
   * @param axis the axis definition
   * @param padding the padding state
   * @param right the right padding for this axis
   */
  void renderRightAxis(
      RenderTarget* target,
      Viewport* viewport,
      AxisDefinition* axis,
      int right) const;

  /**
   * Render a bottom axis
   *
   * @param target the render target
   * @param axis the axis definition
   * @param padding the padding state
   * @param bottom the bottom padding for this axis
   */
  void renderBottomAxis(
      RenderTarget* target,
      Viewport* viewport,
      AxisDefinition* axis,
      int bottom) const;

  /**
   * Render a left axis
   *
   * @param target the render target
   * @param axis the axis definition
   * @param padding the padding state
   * @param left the left padding for this axis
   */
  void renderLeftAxis(
      RenderTarget* target,
      Viewport* viewport,
      AxisDefinition* axis,
      int left) const;

  // FIXPAUL this belongs into the rendertarget
  int width_;
  int height_;
  std::vector<std::unique_ptr<AxisDefinition>> axes_;
  std::vector<std::unique_ptr<Drawable>> drawables_;
};

}
}
#endif