// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use lance::dataset::{WriteMode, WriteParams};
use lance::Dataset;
use lance_datafusion::{Namespace, SessionBuilder};
use lance_namespace::models::{
    CreateEmptyTableRequest, CreateNamespaceRequest, CreateTableRequest, DescribeTableRequest,
};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::ConnectBuilder;
use tempfile::TempDir;

fn create_ipc_for_a() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("failed to build IPC batch for table a");

    let mut buffer = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buffer, &schema).expect("failed to create IPC writer");
        writer
            .write(&batch)
            .expect("failed to write IPC batch for table a");
        writer.finish().expect("failed to finish IPC stream");
    }

    buffer
}

fn create_ipc_for_b() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .expect("failed to build IPC batch for table b");

    let mut buffer = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buffer, &schema).expect("failed to create IPC writer");
        writer
            .write(&batch)
            .expect("failed to write IPC batch for table b");
        writer.finish().expect("failed to finish IPC stream");
    }

    buffer
}

fn create_ipc_for_c() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 4])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .expect("failed to build IPC batch for table c");

    let mut buffer = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buffer, &schema).expect("failed to create IPC writer");
        writer
            .write(&batch)
            .expect("failed to write IPC batch for table c");
        writer.finish().expect("failed to finish IPC stream");
    }

    buffer
}

struct NamespaceTestContext {
    _root_dir: TempDir,
    _extra_dir: TempDir,
    root_ns: Arc<dyn LanceNamespace>,
    extra_ns: Arc<dyn LanceNamespace>,
    ctx: SessionContext,
}

async fn setup_test_context() -> DFResult<NamespaceTestContext> {
    let root_dir = TempDir::new().expect("failed to create root temp dir");
    let extra_dir = TempDir::new().expect("failed to create extra temp dir");

    let root_path = root_dir.path().to_string_lossy().to_string();
    let extra_path = extra_dir.path().to_string_lossy().to_string();

    let root_ns = ConnectBuilder::new("dir")
        .property("root", root_path)
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let extra_ns = ConnectBuilder::new("dir")
        .property("root", extra_path)
        .connect()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // c1.s1 under root_ns
    let mut create_c1 = CreateNamespaceRequest::new();
    create_c1.id = Some(vec!["c1".to_string()]);
    root_ns
        .create_namespace(create_c1)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_s1 = CreateNamespaceRequest::new();
    create_s1.id = Some(vec!["c1".to_string(), "s1".to_string()]);
    root_ns
        .create_namespace(create_s1)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // a in c1.s1
    let mut create_a = CreateTableRequest::new();
    create_a.id = Some(vec![
        "c1".to_string(),
        "s1".to_string(),
        "a".to_string(),
    ]);
    root_ns
        .create_table(create_a, Bytes::from(create_ipc_for_a()))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // b in c1.s1
    let mut create_b = CreateTableRequest::new();
    create_b.id = Some(vec![
        "c1".to_string(),
        "s1".to_string(),
        "b".to_string(),
    ]);
    root_ns
        .create_table(create_b, Bytes::from(create_ipc_for_b()))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // c2.s2 under extra_ns
    let mut create_c2 = CreateNamespaceRequest::new();
    create_c2.id = Some(vec!["c2".to_string()]);
    extra_ns
        .create_namespace(create_c2)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut create_s2 = CreateNamespaceRequest::new();
    create_s2.id = Some(vec!["c2".to_string(), "s2".to_string()]);
    extra_ns
        .create_namespace(create_s2)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // c in c2.s2
    let mut create_c = CreateTableRequest::new();
    create_c.id = Some(vec![
        "c2".to_string(),
        "s2".to_string(),
        "c".to_string(),
    ]);
    extra_ns
        .create_table(create_c, Bytes::from(create_ipc_for_c()))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let ctx = SessionBuilder::new()
        .with_root(Namespace::from_root(Arc::clone(&root_ns)))
        .add_catalog(
            "extra",
            Namespace::from_namespace(Arc::clone(&extra_ns), vec!["c2".to_string()]),
        )
        .build()
        .await?;

    Ok(NamespaceTestContext {
        _root_dir: root_dir,
        _extra_dir: extra_dir,
        root_ns,
        extra_ns,
        ctx,
    })
}

#[tokio::test]
async fn join_within_root_catalog() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT a.name, b.value \
             FROM c1.s1.a a \
             JOIN c1.s1.b b USING (id) \
             WHERE a.id = 2",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column should be Int32Array");

    assert_eq!(name_col.value(0), "b");
    assert_eq!(value_col.value(0), 20);

    Ok(())
}

#[tokio::test]
async fn join_across_catalogs() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "SELECT a.name, c.tag \
             FROM c1.s1.a a \
             JOIN extra.s2.c c \
             ON a.id = c.id \
             WHERE a.id = 3",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let tag_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("tag column should be StringArray");

    assert_eq!(name_col.value(0), "c");
    assert_eq!(tag_col.value(0), "y");

    Ok(())
}

#[tokio::test]
async fn aggregation_on_b() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql("SELECT COUNT(*) AS cnt, SUM(value) AS s FROM c1.s1.b")
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let cnt_col = batch.column(0);
    let cnt = if let Some(arr) = cnt_col.as_any().downcast_ref::<UInt64Array>() {
        arr.value(0)
    } else if let Some(arr) = cnt_col.as_any().downcast_ref::<Int64Array>() {
        arr.value(0) as u64
    } else {
        panic!("unexpected count column type: {:?}", cnt_col.data_type());
    };
    assert_eq!(cnt, 3);

    let sum_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum column should be Int64Array");
    assert_eq!(sum_col.value(0), 60);

    Ok(())
}

#[tokio::test]
async fn create_view_and_select() -> DFResult<()> {
    let ns = setup_test_context().await?;

    let df = ns
        .ctx
        .sql(
            "WITH v1 AS ( \
                 SELECT a.id, a.name, b.value \
                 FROM c1.s1.a a \
                 JOIN c1.s1.b b USING (id) \
             ) \
             SELECT id, name, value FROM v1 WHERE id = 1",
        )
        .await?;
    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column should be Int32Array");
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be StringArray");
    let value_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("value column should be Int32Array");

    assert_eq!(id_col.value(0), 1);
    assert_eq!(name_col.value(0), "a");
    assert_eq!(value_col.value(0), 10);

    Ok(())
}

#[tokio::test]
async fn insert_into_select() -> DFResult<()> {
    let ns = setup_test_context().await?;

    // Preview the source rows for the INSERT and verify the join + filter
    let preview_df = ns
        .ctx
        .sql(
            "SELECT id, name FROM ( \
                 SELECT a.id, a.name, b.value \
                 FROM c1.s1.a a \
                 JOIN c1.s1.b b USING (id) \
             ) v1 \
             WHERE id >= 2",
        )
        .await?;
    let preview_batches = preview_df.collect().await?;
    let preview_row_count: usize = preview_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(preview_row_count, 2);

    // Create empty target table c1.s1.target
    let mut create_empty = CreateEmptyTableRequest::new();
    create_empty.id = Some(vec![
        "c1".to_string(),
        "s1".to_string(),
        "target".to_string(),
    ]);
    let create_empty_resp = ns
        .root_ns
        .create_empty_table(create_empty)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let target_uri = create_empty_resp
        .location
        .expect("empty table must return a location");

    let target_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let empty_ids = Int32Array::from(Vec::<i32>::new());
    let empty_names = StringArray::from(Vec::<&str>::new());
    let empty_batch = RecordBatch::try_new(
        target_schema.clone(),
        vec![Arc::new(empty_ids), Arc::new(empty_names)],
    )
    .expect("failed to build empty batch for target table");
    let reader = RecordBatchIterator::new(vec![Ok(empty_batch)], target_schema);
    let write_params = WriteParams {
        mode: WriteMode::Create,
        ..Default::default()
    };
    Dataset::write(reader, &target_uri, Some(write_params))
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Insert rows into target from join view using Lance DML helper
    lance_datafusion::dml::execute_sql(
        &ns.ctx,
        "INSERT INTO c1.s1.target \
         SELECT id, name FROM ( \
             SELECT a.id, a.name, b.value \
             FROM c1.s1.a a \
             JOIN c1.s1.b b USING (id) \
         ) v1 \
         WHERE id >= 2",
    )
    .await?;

    // Resolve target table URI via namespace
    let mut describe_req = DescribeTableRequest::new();
    describe_req.id = Some(vec![
        "c1".to_string(),
        "s1".to_string(),
        "target".to_string(),
    ]);
    let describe_resp = ns
        .root_ns
        .describe_table(describe_req)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let table_uri = describe_resp
        .table_uri
        .or(describe_resp.location)
        .expect("target table must have a URI");

    let dataset = Dataset::open(&table_uri)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    // Scan dataset and collect rows
    let mut scanner = dataset.scan();
    scanner
        .project(&["id", "name"])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    let mut rows = Vec::<(i32, String)>::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32Array");
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be StringArray");

        for i in 0..batch.num_rows() {
            rows.push((id_col.value(i), name_col.value(i).to_string()));
        }
    }

    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(2, "b".to_string())));
    assert!(rows.contains(&(3, "c".to_string())));

    Ok(())
}
