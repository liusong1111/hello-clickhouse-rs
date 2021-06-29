use clickhouse::{error::Result, Client, Row};
use itertools::izip;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use soa_derive::{soa_zip, StructOfArray};
use std::fmt;
use std::{
    fmt::Display,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DateTime64(pub u64);

impl DateTime64 {
    pub fn now() -> Self {
        let mills = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self(mills)
    }

    pub fn to_system_time(&self) -> SystemTime {
        let mills = self.0;
        let duration = Duration::from_micros(mills);
        let t = UNIX_EPOCH;
        let t = t + duration;
        t
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Row, StructOfArray)]
#[soa_derive = "Clone, Debug, Row, Serialize, Deserialize"]
pub struct Segment {
    // #[serde(rename = "segments.start")]
    start: u64,
    // #[serde(rename = "segments.end")]
    end: u64,
    // #[serde(rename = "segments.htype")]
    htype: String,
    // #[serde(rename = "segments.score")]
    score: f32,
}

#[derive(Clone, Debug, Row, Serialize)]
pub struct LecturePersonSegmentForInsert {
    tenant_id: String,
    // lecture_person_id: Uuid,
    lecture_start_time: DateTime64,
    width: u64,
    height: u64,
    // #[serde(flatten)]
    // segments: SegmentVec,
    #[serde(rename = "segments.start")]
    segments_start: Vec<u64>,
    #[serde(rename = "segments.end")]
    segments_end: Vec<u64>,
    #[serde(rename = "segments.htype")]
    segments_htype: Vec<String>,
    #[serde(rename = "segments.score")]
    segments_score: Vec<f32>,
}

#[serde_as]
#[derive(Clone, Debug, Row, Serialize, Deserialize)]
pub struct LectureAndPerson {
    pub lecture_id: i64,
    pub lecture_person_id: String,
    // pub htype: Vec<String>,
    // pub end: Vec<i64>,
    #[serde_as(as = "LectureAggVec")]
    pub agg: Vec<LectureAgg>,
}

#[derive(Clone, Debug, Row, Serialize, Deserialize, StructOfArray)]
#[soa_derive = "Clone, Debug, Serialize, Deserialize"]
pub struct LectureAgg {
    pub htype: String,
    pub end: i64,
}

#[derive(Clone, Debug, Row, Serialize, Deserialize)]
pub struct AggVec {
    pub htype: Vec<String>,
    pub end: Vec<i64>,
}

impl SerializeAs<Vec<LectureAgg>> for AggVec {
    fn serialize_as<S>(source: &Vec<LectureAgg>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // let ret = AggVec {
        //     htype: vec![],
        //     end: vec![],
        // };
        // ret.serialize(serializer)
        source.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, Vec<LectureAgg>> for AggVec {
    fn deserialize_as<D>(deserializer: D) -> Result<Vec<LectureAgg>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = AggVec::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        println!("data={:?}", data);
        let mut ret: Vec<LectureAgg> = vec![];
        for (htype, end) in izip!(data.htype, data.end) {
            ret.push(LectureAgg { htype, end });
        }
        Ok(ret)
    }
}

// impl From<&LectureAndPerson> for LectureAndPerson1 {
//     fn from(a: &LectureAndPerson) -> Self {
//         let mut persons = vec![];
//         for (htype, end) in izip!(&a.agg.htype, &a.agg.end) {
//             persons.push(LectureAgg {
//                 htype: htype.clone(),
//                 end: *end,
//             });
//         }
//         LectureAndPerson1 {
//             lecture_id: a.lecture_id,
//             lecture_person_id: a.lecture_person_id.clone(),
//             persons,
//         }
//     }
// }

#[derive(Clone, Debug, Row, Serialize, Deserialize)]
pub struct LecturePersonSegmentForSelect {
    tenant_id: String,
    // lecture_person_id: Option<Uuid>,
    lecture_start_time: DateTime64,
    width: u64,
    height: u64,
    #[serde(rename = "segments.start")]
    segments_start: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let ch_url = std::env::var("CH_URL").unwrap();
    let ch_db = std::env::var("CH_DB").unwrap();
    let ch_user = std::env::var("CH_USER").unwrap();
    let ch_password = std::env::var("CH_PASSWORD").unwrap();
    let a: Vec<LectureAgg> = vec![];
    let v = LectureAggVec::from(a);
    let cli = Client::default()
        .with_url(ch_url)
        .with_database(ch_db)
        .with_user(ch_user)
        .with_password(ch_password);
    let c: u64 = cli.query("select count(*) from tenant").fetch_one().await?;
    println!("c={}", c);
    let q = "select l.lecture_id lecture_id, lp.lecture_person_id lecture_person_id,
    groupArray(lps.htype) as htype, groupArray(lps.end) as end
from lecture l
join lecture_person lp on l.lecture_id  = lp.lecture_id 
join lecture_person_segment lps on lps.lecture_person_id = lp.lecture_person_id 
group by l.lecture_id, lp.lecture_person_id ";
    let a: Vec<LectureAndPerson> = cli.query(q).fetch_all().await?;
    println!("x={}", serde_json::to_string_pretty(&a).unwrap());

    Ok(())
}
