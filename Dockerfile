# Wybieramy oficjalny obraz Rusta
FROM rust:1-slim-bullseye AS builder

# NOWOŚĆ: Dajemy kompilatorowi dostęp do bibliotek SQLite
RUN apt-get update && apt-get install -y libsqlite3-dev

WORKDIR /usr/src/app
COPY . .

# Kompilujemy aplikację z flagą optymalizacyjną
RUN cargo build --release

# Odchudzamy ostateczny obraz
FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y sqlite3 libsqlite3-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Używamy poprawnej nazwy Twojego pliku: poker_engine
COPY --from=builder /usr/src/app/target/release/poker_engine /app/server

# Odpalamy!
CMD ["./server"]
