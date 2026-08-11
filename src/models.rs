use serde::Deserialize;

/// JSON API wrapper; unknown envelope and attribute fields are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonApiResponse<T> {
    pub data: Vec<JsonApiResource<T>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonApiResource<T> {
    pub attributes: T,
}

/// Response from `/rest/v1/locations_history`
pub type LocationsHistoryResponse = JsonApiResponse<LocationHistoryAttributes>;

#[derive(Debug, Clone, Deserialize)]
pub struct LocationHistoryAttributes {
    pub id: i32,
    #[serde(default)]
    pub data_points: Vec<DataPoint>,
}

/// A single data point: [timestamp_epoch, value, logged_bool]
#[derive(Debug, Clone, Deserialize)]
#[serde(from = "RawDataPoint")]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

// viewLinc 5.2.1.859 serialises epoch fields as floats (e.g. `1780007546.0`),
// so the tuple's first element must accept both int and float.
#[derive(Debug, Clone, Deserialize)]
struct RawDataPoint(f64, Option<f64>, #[allow(dead_code)] bool);

impl From<RawDataPoint> for DataPoint {
    fn from(raw: RawDataPoint) -> Self {
        Self {
            timestamp: raw.0 as i64,
            value: raw.1.unwrap_or(0.0),
        }
    }
}

/// Response from `/rest/v1/locations`
pub type LocationsResponse = JsonApiResponse<LocationAttributes>;

#[derive(Debug, Clone, Deserialize)]
pub struct LocationAttributes {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub node_id: i32,
    #[serde(default)]
    pub leaf: bool,
    #[serde(default)]
    pub deleted: bool,
}

/// Response from `/rest/v1/locations_data`
pub type LocationsDataResponse = JsonApiResponse<LocationDataAttributes>;

#[derive(Debug, Clone, Deserialize)]
pub struct LocationDataAttributes {
    pub id: i32,
    #[serde(default)]
    pub display_units: String,
    #[serde(default)]
    pub channel_id: i32,
    #[serde(default)]
    pub logger_serial_number: String,
    #[serde(default)]
    pub probe_serial_number: String,
    #[serde(default)]
    pub sample_interval_sec: i32,
    #[serde(default)]
    pub logger_device: String,
    #[serde(default)]
    pub device_status: String,
    #[serde(default)]
    pub device_class: String,
    #[serde(default)]
    pub battery_level: i16,
    #[serde(default)]
    pub line_powered: i16,
    #[serde(default)]
    pub signal_quality: i16,
    #[serde(default)]
    pub unreachable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    // viewLinc 5.2.1.859 serialises some integer epoch fields as floats (e.g. `1780007546.0`).
    #[test]
    fn locations_history_parses_float_timestamps() {
        let json = r#"{
            "jsonapi": {"version": "5.2.1.859"},
            "data": [{"type": "locations_history", "id": "1312", "attributes": {
                "id": 1312, "name": "SDOdegC", "zone": "viewLinc/BREATHE/Saxon",
                "timestamp": 1780007546.0, "value": 10.97,
                "max": 14.83, "max_time": 1779547080.0, "avg": 10.6,
                "min": 6.76, "min_time": 1774678080.0,
                "seconds": 7748220.0, "decimal_places": 2, "std": 1.6,
                "mkt": "N/A", "samples": 12915, "realtime_samples": 0,
                "data_points": [[1772259060.0, 11.97, true]]
            }}]
        }"#;

        let resp: LocationsHistoryResponse =
            river_data_core::serde_json::from_str(json).expect("float timestamps must deserialize");
        let attrs = &resp.data[0].attributes;
        assert_eq!(attrs.id, 1312);
        assert_eq!(attrs.data_points[0].timestamp, 1_772_259_060);
        assert_eq!(attrs.data_points[0].value, 11.97);
    }

    // Integer epochs (the historical format) must keep working.
    #[test]
    fn locations_history_parses_integer_timestamps() {
        let json = r#"{
            "jsonapi": {"version": "5.0.0"},
            "data": [{"type": "locations_history", "id": "1", "attributes": {
                "id": 1, "name": "x", "zone": "z",
                "timestamp": 1780007546, "max_time": 1779547080, "min_time": 1774678080,
                "seconds": 7748220, "data_points": [[1772259060, 1.0, false]]
            }}]
        }"#;

        let resp: LocationsHistoryResponse =
            river_data_core::serde_json::from_str(json).expect("integer timestamps must deserialize");
        let attrs = &resp.data[0].attributes;
        assert_eq!(attrs.id, 1);
        assert_eq!(attrs.data_points[0].timestamp, 1_772_259_060);
    }
}
