use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A single synthetic sales record.
#[derive(Clone, Debug)]
pub struct SaleRecord {
    /// Day of year (0..365).
    pub date: u16,
    /// Product category (0..16).
    pub category: u8,
    /// Sales region (0..10).
    pub region: u8,
    /// Sale amount (10.0..10000.0).
    pub amount: f64,
}

/// Generate `count` pseudo-random sales records.
///
/// Uses a fixed seed so the dataset is reproducible across runs.
pub fn generate_sales(count: usize) -> Vec<SaleRecord> {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE_4242);
    let regions = REGION_NAMES.len() as u8;
    let categories = CATEGORY_NAMES.len() as u8;

    (0..count)
        .map(|_| SaleRecord {
            date: rng.gen_range(0..365),
            category: rng.gen_range(0..categories),
            region: rng.gen_range(0..regions),
            amount: rng.gen_range(10.0..10000.0),
        })
        .collect()
}

pub const REGION_NAMES: &[&str] = &[
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Northeast",
    "Southeast",
    "Northwest",
    "Southwest",
    "Overseas",
];

pub const CATEGORY_NAMES: &[&str] = &[
    "Electronics",
    "Clothing",
    "Food",
    "Books",
    "Sports",
    "Home",
    "Beauty",
    "Toys",
    "Auto",
    "Garden",
    "Music",
    "Health",
    "Office",
    "Pet",
    "Travel",
    "Other",
];
