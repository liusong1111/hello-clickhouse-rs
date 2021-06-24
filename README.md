# hello-clickhouse-rs

clickhouse + rust demo

use the following crate:

* https://github.com/loyd/clickhouse.rs
* https://github.com/liusong1111/soa-derive (forked from https://github.com/lumol-org/soa-derive)

## conclusion
* Not mature yet
* Unsupported type: UUID
* Nested is not easy to use. soa_derive helps little 
* Serde support(Row) is on the right way, still quite of work to do, i.e. serde(flatten), Tuple

## Make life easy
* use plain table instead of Nested
* use UInt64 instead of UUID
