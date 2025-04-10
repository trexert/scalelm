use std::env;

use amqprs::connection::{Connection, OpenConnectionArguments};

pub async fn setup_connection() -> anyhow::Result<Connection> {
    let rabbitmq_host = env::var("RABBITMQ_CONNECTION_HOST")?;
    let rabbitmq_port: u16 = env::var("RABBITMQ_CONNECTION_PORT")?.parse()?;
    let rabbitmq_user = env::var("RABBITMQ_CONNECTION_USER")?;
    let rabbitmq_pass = env::var("RABBITMQ_CONNECTION_PASS")?;
    Ok(Connection::open(&OpenConnectionArguments::new(
        &rabbitmq_host,
        rabbitmq_port,
        &rabbitmq_user,
        &rabbitmq_pass,
    ))
    .await?)
}
