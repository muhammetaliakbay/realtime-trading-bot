# Use the official Rust image as the base image
FROM rust:slim-buster as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
  pkg-config \
  libssl-dev

RUN rustup toolchain install nightly
RUN rustup default nightly

# Set the working directory inside the container
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files to the container
COPY Cargo.toml Cargo.lock ./

# Copy the source code to the container
COPY src ./src

# Copy migrations to the container
COPY migrations ./migrations

# Build the application
RUN cargo build --release --locked

# Create a new stage for the final image
FROM debian:buster-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
  ca-certificates \
  libssl-dev

# Set the working directory inside the container
WORKDIR /app

# Copy the built application from the previous stage
COPY --from=builder /app/target/release/realtime-trading-bot .

# Set the command to run the application
CMD ["./realtime-trading-bot"]
