use clickhouse::{error::Result, Client, Row};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use soa_derive::StructOfArray;

#[derive(Clone, Debug, Serialize, Deserialize, StructOfArray)]
#[soa_derive = "Clone, Debug, Serialize, Deserialize"]
pub struct LectureSegmentPart {
    pub htype: String,
    pub end: i64,
}

#[serde_as]
#[derive(Clone, Debug, Row, Serialize, Deserialize)]
pub struct LectureWithSegments {
    pub lecture_id: i64,
    pub lecture_person_id: String,
    // pub htype: Vec<String>,
    // pub end: Vec<i64>,
    #[serde_as(as = "LectureSegmentPartVec")]
    pub segments: Vec<LectureSegmentPart>,
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
    // let c: u64 = cli.query("select count(*) from tenant").fetch_one().await?;
    // println!("c={}", c);
    let q = "select 
    l.lecture_id, 
    lp.lecture_person_id lecture_person_id,
    groupArray(lps.htype) as htype, 
    groupArray(lps.end) as end
from lecture l
join lecture_person lp on l.lecture_id  = lp.lecture_id 
join lecture_person_segment lps on lps.lecture_person_id = lp.lecture_person_id 
group by l.lecture_id, lp.lecture_person_id ";
    let a: Vec<LectureWithSegments> = cli.query(q).fetch_all().await?;
    println!("x={}", serde_json::to_string_pretty(&a).unwrap());

    Ok(())
}
