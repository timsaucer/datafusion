// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{cmp::Ordering, sync::Arc, vec};

use super::{
    dialect::CharacterLengthStyle, dialect::DateFieldExtractStyle,
    rewrite::TableAliasRewriter, Unparser,
};
use datafusion_common::{
    internal_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Column, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    expr, utils::grouping_set_to_exprlist, Aggregate, Expr, LogicalPlan,
    LogicalPlanBuilder, Projection, SortExpr, Unnest, Window,
};

use indexmap::IndexSet;
use sqlparser::ast;
use sqlparser::tokenizer::Span;

/// Recursively searches children of [LogicalPlan] to find an Aggregate node if exists
/// prior to encountering a Join, TableScan, or a nested subquery (derived table factor).
/// If an Aggregate or node is not found prior to this or at all before reaching the end
/// of the tree, None is returned.
pub(crate) fn find_agg_node_within_select(
    plan: &LogicalPlan,
    already_projected: bool,
) -> Option<&Aggregate> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return None;
    } else {
        input.first()?
    };
    // Agg nodes explicitly return immediately with a single node
    if let LogicalPlan::Aggregate(agg) = input {
        Some(agg)
    } else if let LogicalPlan::TableScan(_) = input {
        None
    } else if let LogicalPlan::Projection(_) = input {
        if already_projected {
            None
        } else {
            find_agg_node_within_select(input, true)
        }
    } else {
        find_agg_node_within_select(input, already_projected)
    }
}

/// Recursively searches children of [LogicalPlan] to find Unnest node if exist
pub(crate) fn find_unnest_node_within_select(plan: &LogicalPlan) -> Option<&Unnest> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return None;
    } else {
        input.first()?
    };

    if let LogicalPlan::Unnest(unnest) = input {
        Some(unnest)
    } else if let LogicalPlan::TableScan(_) = input {
        None
    } else if let LogicalPlan::Projection(_) = input {
        None
    } else {
        find_unnest_node_within_select(input)
    }
}

/// Recursively searches children of [LogicalPlan] to find Unnest node if exist
/// until encountering a Relation node with single input
pub(crate) fn find_unnest_node_until_relation(plan: &LogicalPlan) -> Option<&Unnest> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return None;
    } else {
        input.first()?
    };

    if let LogicalPlan::Unnest(unnest) = input {
        Some(unnest)
    } else if let LogicalPlan::TableScan(_) = input {
        None
    } else if let LogicalPlan::Subquery(_) = input {
        None
    } else if let LogicalPlan::SubqueryAlias(_) = input {
        None
    } else {
        find_unnest_node_within_select(input)
    }
}

/// Recursively searches children of [LogicalPlan] to find Window nodes if exist
/// prior to encountering a Join, TableScan, or a nested subquery (derived table factor).
/// If Window node is not found prior to this or at all before reaching the end
/// of the tree, None is returned.
pub(crate) fn find_window_nodes_within_select<'a>(
    plan: &'a LogicalPlan,
    mut prev_windows: Option<Vec<&'a Window>>,
    already_projected: bool,
) -> Option<Vec<&'a Window>> {
    // Note that none of the nodes that have a corresponding node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return prev_windows;
    } else {
        input.first()?
    };

    // Window nodes accumulate in a vec until encountering a TableScan or 2nd projection
    match input {
        LogicalPlan::Window(window) => {
            prev_windows = match &mut prev_windows {
                Some(windows) => {
                    windows.push(window);
                    prev_windows
                }
                _ => Some(vec![window]),
            };
            find_window_nodes_within_select(input, prev_windows, already_projected)
        }
        LogicalPlan::Projection(_) => {
            if already_projected {
                prev_windows
            } else {
                find_window_nodes_within_select(input, prev_windows, true)
            }
        }
        LogicalPlan::TableScan(_) => prev_windows,
        _ => find_window_nodes_within_select(input, prev_windows, already_projected),
    }
}

/// Recursively identify Column expressions and transform them into the appropriate unnest expression
///
/// For example, if expr contains the column expr "__unnest_placeholder(make_array(Int64(1),Int64(2),Int64(2),Int64(5),NULL),depth=1)"
/// it will be transformed into an actual unnest expression UNNEST([1, 2, 2, 5, NULL])
pub(crate) fn unproject_unnest_expr(expr: Expr, unnest: &Unnest) -> Result<Expr> {
    expr.transform(|sub_expr| {
            if let Expr::Column(col_ref) = &sub_expr {
                // Check if the column is among the columns to run unnest on. 
                // Currently, only List/Array columns (defined in `list_type_columns`) are supported for unnesting. 
                if unnest.list_type_columns.iter().any(|e| e.1.output_column.name == col_ref.name) {
                    if let Ok(idx) = unnest.schema.index_of_column(col_ref) {
                        if let LogicalPlan::Projection(Projection { expr, .. }) = unnest.input.as_ref() {
                            if let Some(unprojected_expr) = expr.get(idx) {
                                let unnest_expr = Expr::Unnest(expr::Unnest::new(unprojected_expr.clone()));
                                return Ok(Transformed::yes(unnest_expr));
                            }
                        }
                    }
                    return internal_err!(
                        "Tried to unproject unnest expr for column '{}' that was not found in the provided Unnest!", &col_ref.name
                    );
                }
            }

            Ok(Transformed::no(sub_expr))

        }).map(|e| e.data)
}

/// Recursively identify all Column expressions and transform them into the appropriate
/// aggregate expression contained in agg.
///
/// For example, if expr contains the column expr "COUNT(*)" it will be transformed
/// into an actual aggregate expression COUNT(*) as identified in the aggregate node.
pub(crate) fn unproject_agg_exprs(
    expr: Expr,
    agg: &Aggregate,
    windows: Option<&[&Window]>,
) -> Result<Expr> {
    expr.transform(|sub_expr| {
            if let Expr::Column(c) = sub_expr {
                if let Some(unprojected_expr) = find_agg_expr(agg, &c)? {
                    Ok(Transformed::yes(unprojected_expr.clone()))
                } else if let Some(unprojected_expr) =
                    windows.and_then(|w| find_window_expr(w, &c.name).cloned())
                {
                    // Window function can contain an aggregation columns, e.g., 'avg(sum(ss_sales_price)) over ...' that needs to be unprojected
                    return Ok(Transformed::yes(unproject_agg_exprs(unprojected_expr, agg, None)?));
                } else {
                    internal_err!(
                        "Tried to unproject agg expr for column '{}' that was not found in the provided Aggregate!", &c.name
                    )
                }
            } else {
                Ok(Transformed::no(sub_expr))
            }
        })
        .map(|e| e.data)
}

/// Recursively identify all Column expressions and transform them into the appropriate
/// window expression contained in window.
///
/// For example, if expr contains the column expr "COUNT(*) PARTITION BY id" it will be transformed
/// into an actual window expression as identified in the window node.
pub(crate) fn unproject_window_exprs(expr: Expr, windows: &[&Window]) -> Result<Expr> {
    expr.transform(|sub_expr| {
        if let Expr::Column(c) = sub_expr {
            if let Some(unproj) = find_window_expr(windows, &c.name) {
                Ok(Transformed::yes(unproj.clone()))
            } else {
                Ok(Transformed::no(Expr::Column(c)))
            }
        } else {
            Ok(Transformed::no(sub_expr))
        }
    })
    .map(|e| e.data)
}

fn find_agg_expr<'a>(agg: &'a Aggregate, column: &Column) -> Result<Option<&'a Expr>> {
    if let Ok(index) = agg.schema.index_of_column(column) {
        if matches!(agg.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
            // For grouping set expr, we must operate by expression list from the grouping set
            let grouping_expr = grouping_set_to_exprlist(agg.group_expr.as_slice())?;
            match index.cmp(&grouping_expr.len()) {
                Ordering::Less => Ok(grouping_expr.into_iter().nth(index)),
                Ordering::Equal => {
                    internal_err!(
                        "Tried to unproject column referring to internal grouping id"
                    )
                }
                Ordering::Greater => {
                    Ok(agg.aggr_expr.get(index - grouping_expr.len() - 1))
                }
            }
        } else {
            Ok(agg.group_expr.iter().chain(agg.aggr_expr.iter()).nth(index))
        }
    } else {
        Ok(None)
    }
}

fn find_window_expr<'a>(
    windows: &'a [&'a Window],
    column_name: &'a str,
) -> Option<&'a Expr> {
    windows
        .iter()
        .flat_map(|w| w.window_expr.iter())
        .find(|expr| expr.schema_name().to_string() == column_name)
}

/// Transforms all Column expressions in a sort expression into the actual expression from aggregation or projection if found.
/// This is required because if an ORDER BY expression is present in an Aggregate or Select, it is replaced
/// with a Column expression (e.g., "sum(catalog_returns.cr_net_loss)"). We need to transform it back to
/// the actual expression, such as sum("catalog_returns"."cr_net_loss").
pub(crate) fn unproject_sort_expr(
    mut sort_expr: SortExpr,
    agg: Option<&Aggregate>,
    input: &LogicalPlan,
) -> Result<SortExpr> {
    sort_expr.expr = sort_expr
        .expr
        .transform(|sub_expr| {
            match sub_expr {
                // Remove alias if present, because ORDER BY cannot use aliases
                Expr::Alias(alias) => Ok(Transformed::yes(*alias.expr)),
                Expr::Column(col) => {
                    if col.relation.is_some() {
                        return Ok(Transformed::no(Expr::Column(col)));
                    }

                    // In case of aggregation there could be columns containing aggregation functions we need to unproject
                    if let Some(agg) = agg {
                        if agg.schema.is_column_from_schema(&col) {
                            return Ok(Transformed::yes(unproject_agg_exprs(
                                Expr::Column(col),
                                agg,
                                None,
                            )?));
                        }
                    }

                    // If SELECT and ORDER BY contain the same expression with a scalar function, the ORDER BY expression will
                    // be replaced by a Column expression (e.g., "substr(customer.c_last_name, Int64(0), Int64(5))"), and we need
                    // to transform it back to the actual expression.
                    if let LogicalPlan::Projection(Projection { expr, schema, .. }) =
                        input
                    {
                        if let Ok(idx) = schema.index_of_column(&col) {
                            if let Some(Expr::ScalarFunction(scalar_fn)) = expr.get(idx) {
                                return Ok(Transformed::yes(Expr::ScalarFunction(
                                    scalar_fn.clone(),
                                )));
                            }
                        }
                    }

                    Ok(Transformed::no(Expr::Column(col)))
                }
                _ => Ok(Transformed::no(sub_expr)),
            }
        })
        .map(|e| e.data)?;
    Ok(sort_expr)
}

/// Iterates through the children of a [LogicalPlan] to find a TableScan node before encountering
/// a Projection or any unexpected node that indicates the presence of a Projection (SELECT) in the plan.
/// If a TableScan node is found, returns the TableScan node without filters, along with the collected filters separately.
/// If the plan contains a Projection, returns None.
///
/// Note: If a table alias is present, TableScan filters are rewritten to reference the alias.
///
/// LogicalPlan example:
///   Filter: ta.j1_id < 5
///     Alias:  ta
///       TableScan: j1, j1_id > 10
///
/// Will return LogicalPlan below:
///     Alias:  ta
///       TableScan: j1
/// And filters: [ta.j1_id < 5, ta.j1_id > 10]
pub(crate) fn try_transform_to_simple_table_scan_with_filters(
    plan: &LogicalPlan,
) -> Result<Option<(LogicalPlan, Vec<Expr>)>> {
    let mut filters: IndexSet<Expr> = IndexSet::new();
    let mut plan_stack = vec![plan];
    let mut table_alias = None;

    while let Some(current_plan) = plan_stack.pop() {
        match current_plan {
            LogicalPlan::SubqueryAlias(alias) => {
                table_alias = Some(alias.alias.clone());
                plan_stack.push(alias.input.as_ref());
            }
            LogicalPlan::Filter(filter) => {
                if !filters.contains(&filter.predicate) {
                    filters.insert(filter.predicate.clone());
                }
                plan_stack.push(filter.input.as_ref());
            }
            LogicalPlan::TableScan(table_scan) => {
                let table_schema = table_scan.source.schema();
                // optional rewriter if table has an alias
                let mut filter_alias_rewriter =
                    table_alias.as_ref().map(|alias_name| TableAliasRewriter {
                        table_schema: &table_schema,
                        alias_name: alias_name.clone(),
                    });

                // rewrite filters to use table alias if present
                let table_scan_filters = table_scan
                    .filters
                    .iter()
                    .cloned()
                    .map(|expr| {
                        if let Some(ref mut rewriter) = filter_alias_rewriter {
                            expr.rewrite(rewriter).data()
                        } else {
                            Ok(expr)
                        }
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;

                for table_scan_filter in table_scan_filters {
                    if !filters.contains(&table_scan_filter) {
                        filters.insert(table_scan_filter);
                    }
                }

                let mut builder = LogicalPlanBuilder::scan(
                    table_scan.table_name.clone(),
                    Arc::clone(&table_scan.source),
                    table_scan.projection.clone(),
                )?;

                if let Some(alias) = table_alias.take() {
                    builder = builder.alias(alias)?;
                }

                let plan = builder.build()?;
                let filters = filters.into_iter().collect();

                return Ok(Some((plan, filters)));
            }
            _ => {
                return Ok(None);
            }
        }
    }

    Ok(None)
}

/// Converts a date_part function to SQL, tailoring it to the supported date field extraction style.
pub(crate) fn date_part_to_sql(
    unparser: &Unparser,
    style: DateFieldExtractStyle,
    date_part_args: &[Expr],
) -> Result<Option<ast::Expr>> {
    match (style, date_part_args.len()) {
        (DateFieldExtractStyle::Extract, 2) => {
            let date_expr = unparser.expr_to_sql(&date_part_args[1])?;
            if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &date_part_args[0] {
                let field = match field.to_lowercase().as_str() {
                    "year" => ast::DateTimeField::Year,
                    "month" => ast::DateTimeField::Month,
                    "day" => ast::DateTimeField::Day,
                    "hour" => ast::DateTimeField::Hour,
                    "minute" => ast::DateTimeField::Minute,
                    "second" => ast::DateTimeField::Second,
                    _ => return Ok(None),
                };

                return Ok(Some(ast::Expr::Extract {
                    field,
                    expr: Box::new(date_expr),
                    syntax: ast::ExtractSyntax::From,
                }));
            }
        }
        (DateFieldExtractStyle::Strftime, 2) => {
            let column = unparser.expr_to_sql(&date_part_args[1])?;

            if let Expr::Literal(ScalarValue::Utf8(Some(field))) = &date_part_args[0] {
                let field = match field.to_lowercase().as_str() {
                    "year" => "%Y",
                    "month" => "%m",
                    "day" => "%d",
                    "hour" => "%H",
                    "minute" => "%M",
                    "second" => "%S",
                    _ => return Ok(None),
                };

                return Ok(Some(ast::Expr::Function(ast::Function {
                    name: ast::ObjectName::from(vec![ast::Ident {
                        value: "strftime".to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    }]),
                    args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                ast::Expr::value(ast::Value::SingleQuotedString(
                                    field.to_string(),
                                )),
                            )),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(column)),
                        ],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                    parameters: ast::FunctionArguments::None,
                    uses_odbc_syntax: false,
                })));
            }
        }
        (DateFieldExtractStyle::DatePart, _) => {
            return Ok(Some(
                unparser.scalar_function_to_sql("date_part", date_part_args)?,
            ));
        }
        _ => {}
    };

    Ok(None)
}

pub(crate) fn character_length_to_sql(
    unparser: &Unparser,
    style: CharacterLengthStyle,
    character_length_args: &[Expr],
) -> Result<Option<ast::Expr>> {
    let func_name = match style {
        CharacterLengthStyle::CharacterLength => "character_length",
        CharacterLengthStyle::Length => "length",
    };

    Ok(Some(unparser.scalar_function_to_sql(
        func_name,
        character_length_args,
    )?))
}

/// SQLite does not support timestamp/date scalars like `to_timestamp`, `from_unixtime`, `date_trunc`, etc.
/// This remaps `from_unixtime` to `datetime(expr, 'unixepoch')`, expecting the input to be in seconds.
/// It supports no other arguments, so if any are supplied it will return an error.
///
/// # Errors
///
/// - If the number of arguments is not 1 - the column or expression to convert.
/// - If the scalar function cannot be converted to SQL.
pub(crate) fn sqlite_from_unixtime_to_sql(
    unparser: &Unparser,
    from_unixtime_args: &[Expr],
) -> Result<Option<ast::Expr>> {
    if from_unixtime_args.len() != 1 {
        return internal_err!(
            "from_unixtime for SQLite expects 1 argument, found {}",
            from_unixtime_args.len()
        );
    }

    Ok(Some(unparser.scalar_function_to_sql(
        "datetime",
        &[
            from_unixtime_args[0].clone(),
            Expr::Literal(ScalarValue::Utf8(Some("unixepoch".to_string()))),
        ],
    )?))
}

/// SQLite does not support timestamp/date scalars like `to_timestamp`, `from_unixtime`, `date_trunc`, etc.
/// This uses the `strftime` function to format the timestamp as a string depending on the truncation unit.
///
/// # Errors
///
/// - If the number of arguments is not 2 - truncation unit and the column or expression to convert.
/// - If the scalar function cannot be converted to SQL.
pub(crate) fn sqlite_date_trunc_to_sql(
    unparser: &Unparser,
    date_trunc_args: &[Expr],
) -> Result<Option<ast::Expr>> {
    if date_trunc_args.len() != 2 {
        return internal_err!(
            "date_trunc for SQLite expects 2 arguments, found {}",
            date_trunc_args.len()
        );
    }

    if let Expr::Literal(ScalarValue::Utf8(Some(unit))) = &date_trunc_args[0] {
        let format = match unit.to_lowercase().as_str() {
            "year" => "%Y",
            "month" => "%Y-%m",
            "day" => "%Y-%m-%d",
            "hour" => "%Y-%m-%d %H",
            "minute" => "%Y-%m-%d %H:%M",
            "second" => "%Y-%m-%d %H:%M:%S",
            _ => return Ok(None),
        };

        return Ok(Some(unparser.scalar_function_to_sql(
            "strftime",
            &[
                Expr::Literal(ScalarValue::Utf8(Some(format.to_string()))),
                date_trunc_args[1].clone(),
            ],
        )?));
    }

    Ok(None)
}
