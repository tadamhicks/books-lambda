# OpenTelemetry Instrumentation for AWS Lambda (Python)

This document describes how we instrumented an AWS Lambda function with OpenTelemetry and configured it to send telemetry data to Groundcover.

## Overview

AWS Lambda function instrumented with OpenTelemetry Python SDK for automatic trace, metric, and log collection. Telemetry flows through an OpenTelemetry Collector to Groundcover.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Lambda Function                      │
│                                                             │
│  ┌─────────────────-─┐         ┌─────────────────────┐      │
│  │  Python           │         │ Lambda Runtime      │      │
│  │  Application      │         │  (Platform Events)  │      │
│  │  (Your Code)      │         │                     │      │
│  └────────┬──────────┘         └──────────┬──────────┘      │
│           │                               │                 │
│           │  Python SDK                   │  TelemetryAPI   │
│           │  (Auto-instrumentation)       │  Receiver       │
│           │                               │                 │
│           └────────────┬──────────────────┘                 │
│                        │                                    │
│                        ▼                                    │
│            ┌──────────────────────┐                         │
│            │  OpenTelemetry       │                         │
│            │  Collector           │                         │
│            │  (Processing Layer)  │                         │
│            └──────────┬───────────┘                         │
│                       │                                     │
└───────────────────────┼─────────────────────────────────────┘
                        │
                        │ OTLP HTTP
                        │
                        ▼
            ┌──────────────────────┐
            │     Groundcover      │
            │   (Observability)    │
            └──────────────────────┘
```

## Components

**Lambda Layers:**
- Python Instrumentation: `arn:aws:lambda:<region>:184161586896:layer:opentelemetry-python-0_17_0:1`
- Collector: `arn:aws:lambda:<region>:184161586896:layer:opentelemetry-collector-<amd64|arm64>-0_18_0:1`

**Collector Configuration (`otel-config.yaml`):**
- **Receivers**: `telemetryapi` (Lambda runtime), `otlp` (Python SDK on ports 4317/4318)
- **Processors**: `batch`, `decouple`, `resource` (adds `team.name` attribute)
- **Exporter**: `otlphttp/groundcover` 

## Configuration

**Collector Configuration:**
`otel-config.yaml` should be added to your root project directory (i.e. accessible at `/var/task/otel-config.yaml`)

**Environment Variables:**
```bash
OPENTELEMETRY_COLLECTOR_CONFIG_FILE=/var/task/otel-config.yaml
OTEL_SERVICE_NAME=api-proxy-lambda
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
AWS_LAMBDA_EXEC_WRAPPER=/opt/otel-instrument
```

**Environment Variable Descriptions:**

- `OPENTELEMETRY_COLLECTOR_CONFIG_FILE`: Path to the collector configuration file in the Lambda deployment package (`/var/task/otel-config.yaml`)
- `OTEL_SERVICE_NAME`: Sets the service name for all telemetry data. This automatically sets the `service.name` resource attribute
- `OTEL_EXPORTER_OTLP_ENDPOINT`: Endpoint where the Python SDK sends traces (collector's OTLP HTTP endpoint on localhost:4318)
- `AWS_LAMBDA_EXEC_WRAPPER`: Lambda extension wrapper script that loads the Python instrumentation and starts the OpenTelemetry Collector
