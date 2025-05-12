# Alethic Instruction-Based State Machine (State Router)

The State Router is a critical component of the Alethic ISM framework, responsible for routing query state entries between different processing components based on defined routes.

## Overview

This service consumes messages containing state entries and routes them to the appropriate processors based on configurations stored in a PostgreSQL database and routing files. It leverages NATS for messaging.

## Requirements

- Python 3.10 or higher
- uv package manager: `pip install uv`
- PostgreSQL database
- NATS messaging system

## Key Dependencies

- alethic-ism-core: Core ISM framework components
- alethic-ism-db: Database interaction layer
- nats-py: NATS messaging client
- psycopg2: PostgreSQL adapter
- python-dotenv: Environment variable management
- pydantic: Data validation
- pyyaml: YAML parsing for configuration

## Setup and Configuration

1. **Environment Setup**:
   - Install dependencies: `uv pip install -r requirements.txt`
   - Set up environment variables in `.env` file:
     ```
     ROUTING_FILE=path/to/.routing.yaml
     DATABASE_URL=postgresql://user:password@host:port/database
     LOG_LEVEL=INFO  # Optional, defaults to DEBUG
     ```

2. **Routing Configuration**:
   - Create a `.routing.yaml` file with message routing definitions as shown in the k8s/secret.yaml example

## Docker Deployment

- Build docker container: `make build`
- Run docker container: `make run`
- Build with custom image name: `make build IMAGE=your-image-name:tag`

## Kubernetes Deployment

The service can be deployed to a Kubernetes cluster using the configuration in the `k8s` directory:

1. Update `k8s/secret.yaml` with appropriate configuration values
2. Update `k8s/deployment.yaml` to reference your image
3. Apply the configurations to your cluster

## Development

- The service uses the `ismcore` framework for messaging
- Routes are defined in the `.routing.yaml` file
- State information is stored in PostgreSQL

## Testing

- Run tests in docker container: `make test`

## Versioning

Use `make version` to automatically bump the patch version and create a git tag.

## Future Improvements

### Golang Migration Opportunities

Migrating the state router to Golang would provide several performance benefits:

1. **Improved Performance**:
   - Significantly faster message processing due to Go's compiled nature
   - Reduced memory consumption compared to Python
   - Better handling of concurrent operations with Go's goroutines

2. **Development Advantages**:
   - Strong type system to catch errors at compile time
   - Excellent standard library for networking and concurrency
   - Built-in testing framework

3. **Implementation Plan**:
   - Create equivalent data structures for message models
   - Implement PostgreSQL connectivity with `lib/pq` or `pgx`
   - Use `nats.go` client for messaging
   - Implement equivalent wildcard routing logic

### Rust Migration Alternatives

Alternatively, Rust would provide additional benefits:

1. **Performance Benefits**:
   - Memory safety without garbage collection
   - Zero-cost abstractions for high performance
   - Predictable performance with no GC pauses

2. **Security Improvements**:
   - Memory safety guarantees
   - Thread safety without data races
   - Safe handling of errors and edge cases

3. **Implementation Libraries**:
   - `tokio` for async runtime
   - `sqlx` or `diesel` for PostgreSQL interaction
   - `async-nats` for NATS messaging
   - `serde` for serialization/deserialization