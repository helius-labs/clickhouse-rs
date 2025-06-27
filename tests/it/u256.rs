use primitive_types::U256;
use rand::random;
use serde::{Deserialize, Serialize};

use clickhouse::Row;

#[tokio::test]
async fn u256() {
    let client = prepare_database!();

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Row)]
    struct MyRow {
        #[serde(with = "clickhouse::serde::u256")]
        id: U256,
        value: String,
    }

    client
        .query(
            "
            CREATE TABLE test(
                id UInt256,
                value String,
            ) ENGINE = MergeTree ORDER BY id
        ",
        )
        .execute()
        .await
        .unwrap();

    fn rand_u256() -> U256 {
        let bytes: [u8; 32] = random();
        U256::from_little_endian(&bytes)
    }

    let (id0, id1, id2) = (rand_u256(), rand_u256(), rand_u256());
    println!("ids: {id0}, {id1}, {id2}");

    let original_rows = vec![
        MyRow {
            id: id0,
            value: "test_0".to_string(),
        },
        MyRow {
            id: id1,
            value: "test_1".to_string(),
        },
        MyRow {
            id: id2,
            value: "test_2".to_string(),
        },
    ];

    let mut insert = client.insert("test").unwrap();
    for row in &original_rows {
        insert.write(row).await.unwrap();
    }
    insert.end().await.unwrap();

    let rows = client
        .query("SELECT ?fields FROM test WHERE id IN ? ORDER BY value")
        .bind(vec![id0, id2])
        .fetch_all::<MyRow>()
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], original_rows[0]);
    assert_eq!(rows[1], original_rows[2]);
}
