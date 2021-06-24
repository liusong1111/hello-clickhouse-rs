use clickhouse::{error::Result, Client, Row};
use serde::{Deserialize, Serialize};
use soa_derive::StructOfArray;
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

    let cli = Client::default()
        .with_url(ch_url)
        .with_database(ch_db)
        .with_user(ch_user)
        .with_password(ch_password);
    let c: u64 = cli.query("select count(*) from tenant").fetch_one().await?;
    println!("c={}", c);

    let tenant_id = "t1".to_string();
    let lecture_start_time = DateTime64::now();
    let width = 88;
    let height = 66;
    let mut segments = SegmentVec::new();
    for i in 0..3 {
        segments.push(Segment {
            start: 10000 * i,
            end: 20000 * i,
            score: 0.9,
            htype: "Happy".into(),
        });
    }
    // let lecture_person_id = Uuid::new_v4();
    // println!("lecture_person_id={}", lecture_person_id);
    let ps = LecturePersonSegmentForInsert {
        tenant_id,
        // lecture_person_id,
        lecture_start_time,
        width,
        height,
        segments_start: segments.start,
        segments_end: segments.end,
        segments_htype: segments.htype,
        segments_score: segments.score,
        // segments,
    };
    let mut inserter = cli.insert("lecture_person_segment").unwrap();
    if let Err(e) = inserter.write(&ps).await {
        println!("ch write error={}", e);
    }
    if let Err(e) = inserter.end().await {
        println!("ch write end error={}", e);
    }

    println!("write ok");
    let segs: Vec<LecturePersonSegmentForSelect> = cli
        .query("select ?fields from lecture_person_segment")
        .fetch_all()
        .await?;
    for seg in segs {
        println!("item={:?}", seg);
    }

    Ok(())
}
