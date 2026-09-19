//! Read-only diagnostic against an explicitly supplied synthetic fixture source.
//! Usage: shared_hydration_capacity <schema.json> <vtgate-uri> <first-fixture-org> <org-count>
use orbit_protocol::cdc::{RowChange, RowOp};
use orbit_protocol::schema::SyncSchema;
use orbit_protocol::value::{CellValue, Row};
use orbit_vstream::{SubscriberConfig, client::VitessEndpoint, shared_projection};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        return Err("expected schema, synthetic vtgate URI, first org and org count".into());
    }
    let schema: SyncSchema = serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
    let config = SubscriberConfig::new(VitessEndpoint::new(&args[2]), "botscribe");
    let start: usize = args[3].parse()?;
    let count: usize = args[4].parse()?;
    for table in ["Chatbot", "ChatbotPermission"] {
        let mut changes = vec![];
        for org in start..start + count {
            for item in 0..16 {
                let mut row = Row::new();
                let id = format!("fleet-{org}-item-{item}");
                row.insert(
                    if table == "Chatbot" { "id" } else { "chatbotId" }.into(),
                    CellValue::String(id.clone()),
                );
                row.insert("organizationId".into(), CellValue::String(format!("fleet-org-{org}")));
                row.insert("userId".into(), CellValue::String(format!("fleet-user-{org}")));
                changes.push(RowChange {
                    table: table.into(),
                    op: RowOp::Insert,
                    key: vec![CellValue::String(id)],
                    before: None,
                    after: Some(row),
                });
            }
        }
        let planned = Instant::now();
        let queries = shared_projection::queries(&schema, Some(&changes))?;
        let plan_ms = planned.elapsed().as_secs_f64() * 1000.;
        let started = Instant::now();
        let rows = shared_projection::load(&config, &schema, Some(&changes)).await?;
        println!(
            "{}",
            serde_json::json!({"table":table,"changes":changes.len(),"projections":queries.len(),"sqlBytes":queries.iter().map(|q|q.sql.len()).sum::<usize>(),"planMs":plan_ms,"readMs":started.elapsed().as_secs_f64()*1000.,"rows":rows.len()})
        );
    }
    Ok(())
}
