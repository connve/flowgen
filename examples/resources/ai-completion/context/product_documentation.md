# Product Documentation

## Authentication

Our API supports two authentication methods:

- **OAuth 2.0**: Full OAuth 2.0 implementation with authorization code flow
- **JWT Tokens**: JSON Web Tokens for service-to-service authentication

API keys are deprecated and will be removed in version 3.0.

## Rate Limits

- **Standard Tier**: 1,000 requests per minute
- **Professional Tier**: 10,000 requests per minute
- **Enterprise Tier**: Custom rate limits available

## API Endpoints

### REST API
- Base URL: `https://api.example.com/v2`
- All endpoints return JSON
- HTTPS only (HTTP redirects to HTTPS)

### GraphQL API
- Endpoint: `https://api.example.com/graphql`
- Full introspection support
- Real-time subscriptions via WebSockets

## SDKs

Official SDKs available for:
- Python (pip install example-sdk)
- JavaScript/TypeScript (npm install @example/sdk)
- Go (go get github.com/example/sdk)

## Features

- Real-time data synchronization
- Webhooks for all event types
- Built-in retry logic with exponential backoff
- Comprehensive error handling
- Request/response logging
