use arcane::engine::Database;
use arcane::storage::{FieldDef, FieldType};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use tempfile::TempDir;

fn random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn setup_db() -> (Arc<Database>, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(dir.path()).unwrap();
    (db, dir)
}

fn setup_db_with_bucket(bucket_def: &str) -> (Arc<Database>, TempDir) {
    let (db, dir) = setup_db();
    db.execute(bucket_def).unwrap();
    (db, dir)
}

fn bench_bucket_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_creation");

    for field_count in [5, 10, 20, 50] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_fields", field_count)),
            &field_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let (db, dir) = setup_db();
                        let fields: Vec<FieldDef> = (0..count)
                            .map(|i| FieldDef {
                                name: format!("field_{}", i),
                                ty: FieldType::String,
                            })
                            .collect();
                        (db, dir, fields)
                    },
                    |(db, _dir, fields)| {
                        db.execute(&format!(
                            "create bucket Bench ({})",
                            fields
                                .iter()
                                .map(|f| format!("{}: string", f.name))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_insert_positional(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_positional");
    group.throughput(Throughput::Elements(1));

    for size in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_bytes", size)),
            &size,
            |b, &sz| {
                let (db, _dir) = setup_db_with_bucket("create bucket Bench (data: string)");

                b.iter(|| {
                    let data = random_string(sz);
                    db.execute(&format!("insert into Bench (\"{}\")", data))
                        .unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_insert_named(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_named");
    group.throughput(Throughput::Elements(1));

    let (db, _dir) =
        setup_db_with_bucket("create bucket Users (name: string, email: string, age: int)");

    group.bench_function("3_fields", |b| {
        b.iter(|| {
            db.execute(&format!(
                "insert into Users (name: \"{}\", email: \"{}@example.com\", age: {})",
                random_string(10),
                random_string(8),
                thread_rng().gen_range(18..80)
            ))
            .unwrap();
        });
    });
    group.finish();
}

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");

    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows", count)),
            &count,
            |b, &n| {
                b.iter_batched(
                    || {
                        let (db, dir) = setup_db();
                        db.execute("create bucket Bulk (id: int, name: string, value: float)")
                            .unwrap();
                        (db, dir)
                    },
                    |(db, _dir)| {
                        for i in 0..n {
                            db.execute(&format!(
                                "insert into Bulk ({}, \"{}\", {})",
                                i,
                                random_string(20),
                                thread_rng().gen::<f64>()
                            ))
                            .unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_scan_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_all");

    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows", count)),
            &count,
            |b, &n| {
                let (db, _dir) = setup_db();
                db.execute("create bucket Scan (id: int, data: string)")
                    .unwrap();

                for i in 0..n {
                    db.execute(&format!(
                        "insert into Scan ({}, \"{}\")",
                        i,
                        random_string(50)
                    ))
                    .unwrap();
                }

                b.iter(|| {
                    black_box(db.execute("get * from Scan").unwrap());
                });
            },
        );
    }
    group.finish();
}

fn bench_hash_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_lookup");
    group.throughput(Throughput::Elements(1));

    for count in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows", count)),
            &count,
            |b, &n| {
                let (db, _dir) = setup_db();
                db.execute("create bucket Lookup (id: int, data: string)")
                    .unwrap();

                let mut hashes = Vec::new();
                for i in 0..n {
                    let result = db
                        .execute(&format!(
                            "insert into Lookup ({}, \"{}\")",
                            i,
                            random_string(50)
                        ))
                        .unwrap();
                    if let arcane::engine::QueryResult::Inserted { hash, .. } = result {
                        hashes.push(hash);
                    }
                }

                b.iter(|| {
                    black_box(
                        db.execute(&format!("get __hash__ from Lookup where id = {}", n / 2))
                            .unwrap(),
                    );
                });
            },
        );
    }
    group.finish();
}

fn bench_filter_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_scan");

    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows", count)),
            &count,
            |b, &n| {
                let (db, _dir) = setup_db();
                db.execute("create bucket Filter (category: string, value: int)")
                    .unwrap();

                let categories = ["A", "B", "C", "D", "E"];
                for i in 0..n {
                    let cat = categories[i as usize % categories.len()];
                    db.execute(&format!("insert into Filter (\"{}\", {})", cat, i))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        db.execute("get * from Filter where category = \"C\"")
                            .unwrap(),
                    );
                });
            },
        );
    }
    group.finish();
}

fn bench_schema_evolution(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_evolution");

    group.bench_function("add_field_to_empty", |b| {
        b.iter_batched(
            || {
                let (db, dir) = setup_db();
                db.execute("create bucket Evolve (field1: string)").unwrap();
                (db, dir)
            },
            |(db, _dir)| {
                db.execute("insert into Evolve (field1: \"test\", new_field: \"value\")")
                    .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    for existing_rows in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_existing_rows", existing_rows)),
            &existing_rows,
            |b, &n| {
                b.iter_batched(
                    || {
                        let (db, dir) = setup_db();
                        db.execute("create bucket Evolve (field1: string)").unwrap();
                        for i in 0..n {
                            db.execute(&format!("insert into Evolve (\"data_{}\")", i))
                                .unwrap();
                        }
                        (db, dir)
                    },
                    |(db, _dir)| {
                        db.execute("insert into Evolve (field1: \"test\", new_field: \"value\")")
                            .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_duplicate_detection(c: &mut Criterion) {
    let mut group = c.benchmark_group("duplicate_detection");
    group.throughput(Throughput::Elements(1));

    for count in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows", count)),
            &count,
            |b, &n| {
                let (db, _dir) = setup_db();
                db.execute("create bucket Dups (data: string)").unwrap();

                for i in 0..n {
                    db.execute(&format!("insert into Dups (\"unique_{}\")", i))
                        .unwrap();
                }

                b.iter(|| {
                    let _ = db.execute("insert into Dups (\"unique_0\")");
                });
            },
        );
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(100));

    group.bench_function("50_insert_50_read", |b| {
        let (db, _dir) = setup_db();
        db.execute("create bucket Mixed (id: int, data: string)")
            .unwrap();

        for i in 0..1000 {
            db.execute(&format!(
                "insert into Mixed ({}, \"{}\")",
                i,
                random_string(50)
            ))
            .unwrap();
        }

        b.iter(|| {
            for i in 0..50 {
                db.execute(&format!(
                    "insert into Mixed ({}, \"{}\")",
                    1000 + i,
                    random_string(50)
                ))
                .unwrap();
            }
            for _ in 0..50 {
                black_box(db.execute("get head(10) from Mixed").unwrap());
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_bucket_creation,
    bench_insert_positional,
    bench_insert_named,
    bench_bulk_insert,
    bench_scan_all,
    bench_hash_lookup,
    bench_filter_scan,
    bench_schema_evolution,
    bench_duplicate_detection,
    bench_mixed_workload,
);

criterion_main!(benches);
