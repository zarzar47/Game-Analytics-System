# Project Roadmap: Game Analytics System

This document outlines the future development path for the Game Analytics System, focusing on scaling from a prototype to a production-grade analytics platform.

## Phase 1: Data Persistence & Historical Analysis (Current Priority)
The current system streams real-time data but has limited historical retention and analysis capabilities.

*   [ ] **Real Database Integration:** Connect the Kafka Consumer (in `apps/api`) to the PostgreSQL database to actually persist the incoming events (`status`, `review`, `purchase`).
*   [ ] **Historical Charts:** Update the Frontend Detail View to query the API/Database for historical trends (e.g., "Player Count over last 24h") instead of showing mock/snapshot data.
*   [ ] **Data Modeling:** Refine the database schema to efficiently store time-series data (consider TimescaleDB extension for PostgreSQL).

## Phase 2: Advanced Analytics & AI
Leverage the captured data for deeper insights.

*   [ ] **Sentiment Analysis Pipeline:** Replace the random sentiment generation in `faker` with a real NLP service (or local model) that processes raw review text.
*   [ ] **Anomaly Detection:** Implement a background worker that monitors streams for sudden drops in player count or sentiment and triggers alerts.
*   [ ] **Player Segmentation:** Analyze purchase behavior to categorize players (e.g., "Whales", "Casuals") and display these segments in the dashboard.

## Phase 3: Architecture Scaling & Reliability
Prepare the system for high traffic and real-world deployment.

*   [ ] **Kafka Consumer Groups:** Scale the API consumers to run multiple instances for higher throughput.
*   [ ] **Caching Layer:** Implement Redis to cache frequent API queries (e.g., game lists, hourly aggregates) to reduce DB load.
*   [ ] **Container Orchestration:** Move from Docker Compose to Kubernetes (K8s) for better management of the microservices.

## Phase 4: Production Polish
*   [ ] **Authentication:** Add user login (e.g., via Supabase Auth) to secure the dashboard.
*   [ ] **Real Data Ingestion:** Create a public API endpoint and SDK (e.g., a Unity/Unreal plugin) so real games can send telemetry instead of the Faker service.
*   [ ] **Alerting System:** specific alerts (Discord/Slack/Email) when critical metrics cross defined thresholds.
