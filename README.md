# EPA Data Pipeline

Automated ETL pipeline for loading EPA environmental data into Snowflake data warehouse. This repository contains modular scripts for extracting data from various EPA sources and loading them into USG's Snowflake instance.

## 🚀 Features

- **Automated Scheduling**: Runs every 6 weeks via GitHub Actions
- **Incremental Loading**: Maintains historical records with deduplication
- **Secure Authentication**: Uses RSA key pair authentication with Snowflake
- **Modular Design**: Easy to add new data sources
- **Error Handling**: Comprehensive logging and error recovery
- **Data Versioning**: Tracks all loads with timestamps and unique IDs

## 📊 Data Sources

### Currently Implemented
- **EPA SDWA (Safe Drinking Water Act)**: Public water systems, violations, enforcement actions

### Planned Additions
- EPA Air Quality Data
- EPA Toxic Release Inventory
- EPA Brownfields Data

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   EPA ECHO API  │────>│ GitHub       │────>│  Snowflake  │
│   Data Sources  │     │ Actions      │     │  Data       │
└─────────────────┘     │ Workflow     │     │  Warehouse  │
                        └──────────────┘     └─────────────┘
                               │
                               ▼
                        ┌──────────────┐
                        │  Repository  │
                        │   Secrets    │
                        └──────────────┘
```

## 🔧 Setup Instructions

### Prerequisites
- Snowflake account with appropriate permissions
- GitHub repository with Actions enabled
- RSA key pair for Snowflake authentication

### 1. Generate RSA Key Pair

```bash
# Generate private key
openssl genrsa -out snow
