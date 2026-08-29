# Wybieramy oficjalny obraz Rusta
FROM rust:1.76-slim-bullseye AS builder

WORKDIR /usr/src/app
COPY . .

# Kompilujemy aplikację z flagą optymalizacyjną
RUN cargo build --release

# Odchudzamy ostateczny obraz
FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y sqlite3 libsqlite3-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/app/target/release/twoja_nazwa_projektu /app/server

# Odpalamy!
CMD ["./server"]
