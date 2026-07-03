// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use config::meta::{inverted_index::IndexOptimizeMode, stream::FileKey};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    },
    physical_plan::{
        ExecutionPlan,
        aggregates::{AggregateExec, AggregateMode},
        union::UnionExec,
    },
};

use crate::service::search::{
    datafusion::plan::{
        metadata_count_exec::MetadataCountExec, tantivy_optimize_exec::TantivyOptimizeExec,
    },
    grpc::QueryParams,
    index::IndexCondition,
};

// rewrite the physical plan to add tantivy optimize exec
pub fn tantivy_optimize_rewrite(
    query: Arc<QueryParams>,
    mut file_list: Vec<FileKey>,
    mut index_condition: Option<IndexCondition>,
    index_optimize_mode: IndexOptimizeMode,
    mut physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut visitor = PartialAggregateUnionRewriter::new(move |schema| {
        Arc::new(TantivyOptimizeExec::new(
            query.clone(),
            schema,
            std::mem::take(&mut file_list),
            std::mem::take(&mut index_condition),
            index_optimize_mode.clone(),
        )) as Arc<dyn ExecutionPlan>
    });
    physical_plan = physical_plan.rewrite(&mut visitor)?.data;
    Ok(physical_plan)
}

pub fn metadata_count_rewrite(
    file_list: Vec<FileKey>,
    mut physical_plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let records = metadata_records(&file_list);
    if records == 0 {
        return Ok(physical_plan);
    }

    let mut visitor = PartialAggregateUnionRewriter::new(move |schema| {
        Arc::new(MetadataCountExec::new(schema, records)) as Arc<dyn ExecutionPlan>
    });
    physical_plan = physical_plan.rewrite(&mut visitor)?.data;
    Ok(physical_plan)
}

fn metadata_records(file_list: &[FileKey]) -> i64 {
    file_list.iter().fold(0i64, |total, file| {
        total.saturating_add(file.meta.records.max(0))
    })
}

struct PartialAggregateUnionRewriter<F>
where
    F: FnMut(SchemaRef) -> Arc<dyn ExecutionPlan>,
{
    make_extra_input: F,
    rewritten: bool,
}

impl<F> PartialAggregateUnionRewriter<F>
where
    F: FnMut(SchemaRef) -> Arc<dyn ExecutionPlan>,
{
    fn new(make_extra_input: F) -> Self {
        Self {
            make_extra_input,
            rewritten: false,
        }
    }
}

impl<F> TreeNodeRewriter for PartialAggregateUnionRewriter<F>
where
    F: FnMut(SchemaRef) -> Arc<dyn ExecutionPlan>,
{
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: Arc<dyn ExecutionPlan>) -> Result<Transformed<Self::Node>> {
        if !self.rewritten && node.name() == "AggregateExec" {
            let aggregate = node.downcast_ref::<AggregateExec>().unwrap();
            if *aggregate.mode() == AggregateMode::Partial {
                let extra_input = (self.make_extra_input)(node.schema());
                let new_node = UnionExec::try_new(vec![node, extra_input])?;
                self.rewritten = true;
                Ok(Transformed::new(new_node, true, TreeNodeRecursion::Stop))
            } else {
                Ok(Transformed::no(node))
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use config::meta::stream::{FileKey, FileMeta};
    use datafusion::{common::Result, physical_plan::displayable, prelude::SessionContext};

    use super::*;
    use crate::service::search::datafusion::table_provider::empty_table::NewEmptyTable;

    #[tokio::test]
    async fn test_metadata_count_rewrite_adds_partial_count_input() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Int64,
            false,
        )]));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(NewEmptyTable::new("t", schema)))?;
        let logical_plan = ctx
            .state()
            .create_logical_plan("SELECT count(*) FROM t")
            .await?;
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

        let files = vec![
            FileKey {
                meta: FileMeta {
                    records: 11,
                    ..Default::default()
                },
                ..Default::default()
            },
            FileKey {
                meta: FileMeta {
                    records: 31,
                    ..Default::default()
                },
                ..Default::default()
            },
        ];
        let rewritten = metadata_count_rewrite(files, physical_plan)?;
        let plan = displayable(rewritten.as_ref()).indent(false).to_string();
        let batches = datafusion::physical_plan::collect(rewritten, ctx.task_ctx()).await?;

        assert!(plan.contains("MetadataCountExec: records: 42"));
        assert_eq!(batches.len(), 1);
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 42);
        Ok(())
    }
}
