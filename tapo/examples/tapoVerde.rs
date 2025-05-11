/// L530, L535 and L630 Multi-Device Example with Parallel Execution
use std::{env, time::Duration};
use log::{info, LevelFilter};
use tapo::{requests::Color, ApiClient};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure logging
    let log_level = env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string())
        .parse()
        .unwrap_or(LevelFilter::Info);

    pretty_env_logger::formatted_timed_builder()
        .filter(Some("tapo"), log_level)
        .init();

    // Retrieve credentials and device IPs from environment variables
    let tapo_username = env::var("TAPO_USERNAME")?;
    let tapo_password = env::var("TAPO_PASSWORD")?;

    let ip_addresses = vec![
        "192.168.202.153",
        "192.168.202.196",
        "192.168.202.99",
        "192.168.202.14",
    ];

    // Initialize devices
    let devices: Vec<_> = ip_addresses
        .iter()
        .map(|ip| {
            ApiClient::new(tapo_username.clone(), tapo_password.clone())
                .l530(ip.to_string())
        })
        .collect();

    let mut devices = futures::future::try_join_all(devices).await?;

    // Turn all devices on simultaneously
    info!("Turning all devices on...");
    futures::future::try_join_all(devices.iter_mut().map(|device| device.on())).await?;

    info!("Waiting 2 seconds...");
    sleep(Duration::from_secs(2)).await;

    // Set color to ForestGreen on all devices simultaneously
    info!("Setting color to `ForestGreen`...");
    futures::future::try_join_all(
        devices
            .iter_mut()
            .map(|device| device.set_color(Color::ForestGreen)),
    )
    .await?;

    info!("Waiting 2 seconds...");
    sleep(Duration::from_secs(2)).await;

    // Turn all devices off simultaneously
    info!("Turning all devices off...");
    futures::future::try_join_all(devices.iter_mut().map(|device| device.off())).await?;

    // Get device info and usage for the first device
    if let Some(first_device) = devices.first() {
        let device_info = first_device.get_device_info().await?;
        info!("Device info: {device_info:?}");

        let device_usage = first_device.get_device_usage().await?;
        info!("Device usage: {device_usage:?}");
    }

    Ok(())
}
