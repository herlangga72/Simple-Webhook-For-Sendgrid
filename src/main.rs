use actix_web::{post, web, App, HttpServer, HttpResponse};
use serde::Deserialize;
use tokio_postgres::{NoTls, Client};
use std::env;
use dotenv::dotenv;

#[derive(Deserialize)]
struct SendGridEvent {
    email: String,
    timestamp: i64,
    #[serde(rename = "smtp-id")]
    smtp_id: String,
    event: String,
    category: Option<Vec<String>>,
    #[serde(rename = "sg_event_id")]
    sg_event_id: String,
    #[serde(rename = "sg_message_id")]
    sg_message_id: String,
    response: Option<String>,
    attempt: Option<String>, // Optional field
    reason: Option<String>,
    status: Option<String>,
    useragent: Option<String>,
    ip: Option<String>,
    url: Option<String>,
    asm_group_id: Option<i32>, // Optional field
}

#[post("/webhook/sendgrid")]
async fn sendgrid_webhook(events: web::Json<Vec<SendGridEvent>>) -> HttpResponse {
    let (client, connection) = tokio_postgres::connect(
        "postgres://bookstore:bookstore@172.31.35.67/sendgrid_logs",
        NoTls,
    )
    .await
    .expect("Failed to connect to database");

    // Spawn a new task to handle the connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    for event in events.iter() {
        let email = &event.email;
        let event_type = &event.event;
        let timestamp = event.timestamp;
        let smtp_id = &event.smtp_id;
        let sg_event_id = &event.sg_event_id;
        let sg_message_id = &event.sg_message_id;
        let response = &event.response;
        let attempt = &event.attempt;
        let reason = &event.reason;
        let status = &event.status;
        let useragent = &event.useragent;
        let ip = &event.ip;
        let url = &event.url;
        let asm_group_id = &event.asm_group_id;

        // Insert data into PostgreSQL
        if let Err(e) = client
            .execute(
                "INSERT INTO sendgrid_events (email, event_type, timestamp, smtp_id, sg_event_id, sg_message_id, response, attempt, reason, status, user_agent, ip, url, asm_group_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                &[
                    &email,
                    &event_type,
                    &timestamp,
                    &smtp_id,
                    &sg_event_id,
                    &sg_message_id,
                    &response,
                    &attempt,
                    &reason,
                    &status,
                    &useragent,
                    &ip,
                    &url,
                    &asm_group_id,
                ],
            )
            .await
        {
            eprintln!("Failed to insert event: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    }

    HttpResponse::Ok().json("Success")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok(); // Load environment variables from .env file

    HttpServer::new(|| {
        App::new()
            .service(sendgrid_webhook)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
