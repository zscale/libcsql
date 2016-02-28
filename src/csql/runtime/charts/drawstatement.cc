/**
 * This file is part of the "FnordMetric" project
 *   Copyright (c) 2011-2014 Paul Asmuth, Google Inc.
 *
 * FnordMetric is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License v3.0. You should have received a
 * copy of the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
#include <csql/runtime/runtime.h>
#include <csql/runtime/charts/domainconfig.h>
#include <csql/runtime/charts/drawstatement.h>
#include <csql/runtime/charts/areachartbuilder.h>
#include <csql/runtime/charts/barchartbuilder.h>
#include <csql/runtime/charts/linechartbuilder.h>
#include <csql/runtime/charts/pointchartbuilder.h>

namespace csql {

DrawStatement::DrawStatement(
    Transaction* ctx,
    RefPtr<DrawStatementNode> node,
    Vector<ScopedPtr<TableExpression>> sources,
    Runtime* runtime) :
    ctx_(ctx),
    node_(node),
    sources_(std::move(sources)),
    runtime_(runtime) {
  if (sources_.empty()) {
    RAISE(kRuntimeError, "DRAW statement without any tables");
  }

  for (auto& table : sources_) {
    if (table->numColumns() != sources_[0]->numColumns()) {
      RAISE(kRuntimeError, "DRAW tables return different number of columns");
    }
  }
}

void DrawStatement::prepare(ExecutionContext* context) {
  for (auto& source : sources_) {
    source->prepare(context);
  }
}

void DrawStatement::execute(
    ExecutionContext* context,
    stx::chart::Canvas* canvas) {
  stx::chart::Drawable* chart = nullptr;

  switch (node_->chartType()) {
    case DrawStatementNode::ChartType::AREACHART:
      chart = executeWithChart<AreaChartBuilder>(context, canvas);
      break;
    case DrawStatementNode::ChartType::BARCHART:
      chart = executeWithChart<BarChartBuilder>(context, canvas);
      break;
    case DrawStatementNode::ChartType::LINECHART:
      chart = executeWithChart<LineChartBuilder>(context, canvas);
      break;
    case DrawStatementNode::ChartType::POINTCHART:
      chart = executeWithChart<PointChartBuilder>(context, canvas);
      break;
  }

  applyDomainDefinitions(chart);
  applyTitle(chart);
  applyAxisDefinitions(chart);
  applyGrid(chart);
  applyLegend(chart);
}

void DrawStatement::applyAxisDefinitions(stx::chart::Drawable* chart) const {
  for (const auto& child : node_->ast()->getChildren()) {
    if (child->getType() != ASTNode::T_AXIS ||
        child->getChildren().size() < 1 ||
        child->getChildren()[0]->getType() != ASTNode::T_AXIS_POSITION) {
      continue;
    }

    stx::chart::AxisDefinition* axis = nullptr;

    if (child->getChildren().size() < 1) {
      RAISE(kRuntimeError, "corrupt AST: AXIS has < 1 child");
    }

    switch (child->getChildren()[0]->getToken()->getType()) {
      case Token::T_TOP:
        axis = chart->addAxis(stx::chart::AxisDefinition::TOP);
        break;

      case Token::T_RIGHT:
        axis = chart->addAxis(stx::chart::AxisDefinition::RIGHT);
        break;

      case Token::T_BOTTOM:
        axis = chart->addAxis(stx::chart::AxisDefinition::BOTTOM);
        break;

      case Token::T_LEFT:
        axis = chart->addAxis(stx::chart::AxisDefinition::LEFT);
        break;

      default:
        RAISE(kRuntimeError, "corrupt AST: invalid axis position");
    }

    for (int i = 1; i < child->getChildren().size(); ++i) {
      auto prop = child->getChildren()[i];

      if (prop->getType() == ASTNode::T_PROPERTY &&
          prop->getToken() != nullptr &&
          *prop->getToken() == Token::T_TITLE &&
          prop->getChildren().size() == 1) {
        auto axis_title = runtime_->evaluateConstExpression(
            ctx_,
            prop->getChildren()[0]);
        axis->setTitle(axis_title.getString());
        continue;
      }

      if (prop->getType() == ASTNode::T_AXIS_LABELS) {
        applyAxisLabels(prop, axis);
      }
    }
  }
}

void DrawStatement::applyAxisLabels(
    ASTNode* ast,
    stx::chart::AxisDefinition* axis) const {
  for (const auto& prop : ast->getChildren()) {
    if (prop->getType() != ASTNode::T_PROPERTY ||
        prop->getToken() == nullptr) {
      continue;
    }

    switch (prop->getToken()->getType()) {
      case Token::T_INSIDE:
        axis->setLabelPosition(stx::chart::AxisDefinition::LABELS_INSIDE);
        break;
      case Token::T_OUTSIDE:
        axis->setLabelPosition(stx::chart::AxisDefinition::LABELS_OUTSIDE);
        break;
      case Token::T_OFF:
        axis->setLabelPosition(stx::chart::AxisDefinition::LABELS_OFF);
        break;
      case Token::T_ROTATE: {
        if (prop->getChildren().size() != 1) {
          RAISE(kRuntimeError, "corrupt AST: ROTATE has no children");
        }

        auto rot = runtime_->evaluateConstExpression(
            ctx_,
            prop->getChildren()[0]);
        axis->setLabelRotation(rot.getValue<double>());
        break;
      }
      default:
        RAISE(kRuntimeError, "corrupt AST: LABELS has invalid token");
    }
  }
}

void DrawStatement::applyDomainDefinitions(
    stx::chart::Drawable* chart) const {
  for (const auto& child : node_->ast()->getChildren()) {
    bool invert = false;
    bool logarithmic = false;
    ASTNode* min_expr = nullptr;
    ASTNode* max_expr = nullptr;

    if (child->getType() != ASTNode::T_DOMAIN) {
      continue;
    }

    if (child->getToken() == nullptr) {
      RAISE(kRuntimeError, "corrupt AST: DOMAIN has no token");
    }

    stx::chart::AnyDomain::kDimension dim;
    switch (child->getToken()->getType()) {
      case Token::T_XDOMAIN:
        dim = stx::chart::AnyDomain::DIM_X;
        break;
      case Token::T_YDOMAIN:
        dim = stx::chart::AnyDomain::DIM_Y;
        break;
      case Token::T_ZDOMAIN:
        dim = stx::chart::AnyDomain::DIM_Z;
        break;
      default:
        RAISE(kRuntimeError, "corrupt AST: DOMAIN has invalid token");
    }

    for (const auto& domain_prop : child->getChildren()) {
      switch (domain_prop->getType()) {
        case ASTNode::T_DOMAIN_SCALE: {
          auto min_max_expr = domain_prop->getChildren();
          if (min_max_expr.size() != 2 ) {
            RAISE(kRuntimeError, "corrupt AST: invalid DOMAIN SCALE");
          }
          min_expr = min_max_expr[0];
          max_expr = min_max_expr[1];
          break;
        }

        case ASTNode::T_PROPERTY: {
          if (domain_prop->getToken() != nullptr) {
            switch (domain_prop->getToken()->getType()) {
              case Token::T_INVERT:
                invert = true;
                continue;
              case Token::T_LOGARITHMIC:
                logarithmic = true;
                continue;
              default:
                break;
            }
          }

          RAISE(kRuntimeError, "corrupt AST: invalid DOMAIN property");
          break;
        }

        default:
          RAISE(kRuntimeError, "corrupt AST: unexpected DOMAIN child");

      }
    }

    DomainConfig domain_config(chart, dim);
    domain_config.setInvert(invert);
    domain_config.setLogarithmic(logarithmic);
    if (min_expr != nullptr && max_expr != nullptr) {
      domain_config.setMin(runtime_->evaluateConstExpression(ctx_, min_expr));
      domain_config.setMax(runtime_->evaluateConstExpression(ctx_, max_expr));
    }
  }
}

void DrawStatement::applyTitle(stx::chart::Drawable* chart) const {
  for (const auto& child : node_->ast()->getChildren()) {
    if (child->getType() != ASTNode::T_PROPERTY ||
        child->getToken() == nullptr || !(
        child->getToken()->getType() == Token::T_TITLE ||
        child->getToken()->getType() == Token::T_SUBTITLE)) {
      continue;
    }

    if (child->getChildren().size() != 1) {
      RAISE(kRuntimeError, "corrupt AST: [SUB]TITLE has != 1 child");
    }

    auto title_eval = runtime_->evaluateConstExpression(
        ctx_,
        child->getChildren()[0]);
    auto title_str = title_eval.getString();

    switch (child->getToken()->getType()) {
      case Token::T_TITLE:
        chart->setTitle(title_str);
        break;
      case Token::T_SUBTITLE:
        chart->setSubtitle(title_str);
        break;
      default:
        break;
    }
  }
}

void DrawStatement::applyGrid(stx::chart::Drawable* chart) const {
  ASTNode* grid = nullptr;

  for (const auto& child : node_->ast()->getChildren()) {
    if (child->getType() == ASTNode::T_GRID) {
      grid = child;
      break;
    }
  }

  if (!grid) {
    return;
  }

  bool horizontal = false;
  bool vertical = false;

  for (const auto& prop : grid->getChildren()) {
    if (prop->getType() == ASTNode::T_PROPERTY && prop->getToken() != nullptr) {
      switch (prop->getToken()->getType()) {
        case Token::T_HORIZONTAL:
          horizontal = true;
          break;
        case Token::T_VERTICAL:
          vertical = true;
          break;
        default:
          RAISE(kRuntimeError, "corrupt AST: invalid GRID property");
      }
    }
  }

  if (horizontal) {
    chart->addGrid(stx::chart::GridDefinition::GRID_HORIZONTAL);
  }

  if (vertical) {
    chart->addGrid(stx::chart::GridDefinition::GRID_VERTICAL);
  }
}

void DrawStatement::applyLegend(stx::chart::Drawable* chart) const {
  ASTNode* legend = nullptr;

  for (const auto& child : node_->ast()->getChildren()) {
    if (child->getType() == ASTNode::T_LEGEND) {
      legend = child;
      break;
    }
  }

  if (!legend) {
    return;
  }


  stx::chart::LegendDefinition::kVerticalPosition vert_pos =
      stx::chart::LegendDefinition::LEGEND_BOTTOM;
  stx::chart::LegendDefinition::kHorizontalPosition horiz_pos =
      stx::chart::LegendDefinition::LEGEND_LEFT;
  stx::chart::LegendDefinition::kPlacement placement =
      stx::chart::LegendDefinition::LEGEND_OUTSIDE;
  std::string title;

  for (const auto& prop : legend->getChildren()) {
    if (prop->getType() == ASTNode::T_PROPERTY && prop->getToken() != nullptr) {
      switch (prop->getToken()->getType()) {
        case Token::T_TOP:
          vert_pos = stx::chart::LegendDefinition::LEGEND_TOP;
          break;
        case Token::T_RIGHT:
          horiz_pos = stx::chart::LegendDefinition::LEGEND_RIGHT;
          break;
        case Token::T_BOTTOM:
          vert_pos = stx::chart::LegendDefinition::LEGEND_BOTTOM;
          break;
        case Token::T_LEFT:
          horiz_pos = stx::chart::LegendDefinition::LEGEND_LEFT;
          break;
        case Token::T_INSIDE:
          placement = stx::chart::LegendDefinition::LEGEND_INSIDE;
          break;
        case Token::T_OUTSIDE:
          placement = stx::chart::LegendDefinition::LEGEND_OUTSIDE;
          break;
        case Token::T_TITLE: {
          if (prop->getChildren().size() != 1) {
            RAISE(kRuntimeError, "corrupt AST: TITLE has no children");
          }

          auto sval = runtime_->evaluateConstExpression(
              ctx_,
              prop->getChildren()[0]);

          title = sval.getString();
          break;
        }
        default:
          RAISE(
              kRuntimeError,
              "corrupt AST: LEGEND has invalid property");
      }
    }
  }

  chart->addLegend(vert_pos, horiz_pos, placement, title);
}

}
