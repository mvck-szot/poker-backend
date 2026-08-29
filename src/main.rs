use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::Query,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
// Usunięto nieużywany HashMap i StreamExt, żeby wyczyścić terminal
use futures::sink::SinkExt;
use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tower_http::cors::{Any, CorsLayer};

// --- STRUKTURY DANYCH ---
#[derive(Serialize, Deserialize)]
struct AuthRequest {
    username: String,
    password: String,
}

#[derive(Serialize, Deserialize)]
struct UserResponse {
    success: bool,
    message: String,
    id: Option<i32>,
    username: Option<String>,
    elo_train: Option<i32>,
    elo_1v1: Option<i32>,
    elo_1v7: Option<i32>,
}

#[derive(Serialize, Deserialize)]
struct StatsResponse {
    elo_train: i32,
    elo_1v1: i32,
    elo_1v7: i32,
    streak: i32,
    hands_played: i32,
}

#[derive(Serialize, Deserialize)]
struct UpdateStatsRequest {
    user_id: Option<i32>,
    mode: String,
    elo: i32,
    streak: i32,
    hands_played: i32,
}

#[derive(Serialize, Deserialize)]
struct WsArenaRequest {
    hand: String,
    board: String,
    pos: String,
}

#[derive(Deserialize)]
struct ArenaQuery {
    hand: String,
    board: Option<String>,
    pos: Option<String>,
    bot_elo: i32,
}

#[derive(Deserialize)]
struct SolveQuery {
    hand: String,
    board: Option<String>,
    pos: Option<String>,
    history: Option<String>,
}

// --- LOGIKA POKEROWA (GTO & Heurystyka) ---
fn evaluate_poker_strength(hand: &str, board: &str) -> f32 {
    let r1 = &hand[0..1];
    let r2 = &hand[2..3];
    let v1 = "23456789TJQKA".find(r1).unwrap_or(0) as i32;
    let v2 = "23456789TJQKA".find(r2).unwrap_or(0) as i32;
    let max_v = v1.max(v2);
    let min_v = v1.min(v2);

    if board.is_empty() {
        let suited = &hand[1..2] == &hand[3..4];
        if r1 == r2 {
            return 0.60 + (max_v as f32 / 12.0) * 0.35;
        }
        let mut strength = (max_v as f32 / 12.0) * 0.40 + (min_v as f32 / 12.0) * 0.20;
        if suited {
            strength += 0.10;
        }
        if max_v - min_v == 1 {
            strength += 0.05;
        }
        return strength.clamp(0.05, 0.95);
    }

    let s1 = &hand[1..2];
    let s2 = &hand[3..4];
    let mut board_vals = Vec::new();
    let mut flush_s1 = 1;
    let mut flush_s2 = 1;
    if s1 == s2 {
        flush_s1 = 2;
        flush_s2 = 2;
    }

    for i in (0..board.len()).step_by(2) {
        if i + 2 <= board.len() {
            let br = &board[i..i + 1];
            let bs = &board[i + 1..i + 2];
            board_vals.push("23456789TJQKA".find(br).unwrap_or(0) as i32);
            if bs == s1 {
                flush_s1 += 1;
            }
            if bs == s2 && s1 != s2 {
                flush_s2 += 1;
            }
        }
    }
    board_vals.sort_by(|a, b| b.cmp(a));
    let max_flush = flush_s1.max(flush_s2);
    if max_flush >= 5 {
        return 0.95;
    }

    let mut all_vals = board_vals.clone();
    all_vals.push(v1);
    all_vals.push(v2);
    all_vals.sort_by(|a, b| b.cmp(a));
    all_vals.dedup();
    let mut consec = 1;
    let mut max_consec = 1;
    for i in 1..all_vals.len() {
        if all_vals[i - 1] - all_vals[i] == 1 {
            consec += 1;
            if consec > max_consec {
                max_consec = consec;
            }
        } else {
            consec = 1;
        }
    }
    if all_vals.contains(&12)
        && all_vals.contains(&0)
        && all_vals.contains(&1)
        && all_vals.contains(&2)
        && all_vals.contains(&3)
    {
        max_consec = 5;
    }
    if max_consec >= 5 {
        return 0.93;
    }

    let mut matches_v1 = 0;
    let mut matches_v2 = 0;
    for &bv in &board_vals {
        if bv == v1 {
            matches_v1 += 1;
        }
        if bv == v2 {
            matches_v2 += 1;
        }
    }

    if matches_v1 == 2 || matches_v2 == 2 {
        return 0.90;
    }
    if matches_v1 == 1 && matches_v2 == 1 {
        return 0.85;
    }

    let max_matches = matches_v1.max(matches_v2);
    if max_matches == 1 {
        let paired_val = if matches_v1 == 1 { v1 } else { v2 };
        if paired_val >= board_vals[0] {
            return 0.75;
        } else if board_vals.len() > 1 && paired_val >= board_vals[1] {
            return 0.60;
        } else {
            return 0.40;
        }
    }

    if v1 == v2 {
        if board_vals.is_empty() || v1 > board_vals[0] {
            return 0.80;
        }
        return 0.50;
    }
    if max_flush == 4 {
        return 0.65;
    }
    if max_consec == 4 {
        return 0.55;
    }
    if max_v >= 10 {
        return 0.35;
    }
    0.15
}

fn get_smart_gto_strategy(hand: &str, board: &str, history: &str, _position: &str) -> [f32; 4] {
    let strength = evaluate_poker_strength(hand, board);
    let is_preflop = board.is_empty();
    let facing_bet = if is_preflop {
        !history.is_empty()
    } else {
        history.ends_with('B') || history.ends_with('S') || history.ends_with('R')
    };

    if facing_bet {
        if strength < 0.45 {
            [0.90, 0.05, 0.05, 0.0]
        } else if strength < 0.65 {
            [0.30, 0.60, 0.10, 0.0]
        } else if strength < 0.85 {
            [0.0, 0.60, 0.30, 0.10]
        } else {
            [0.0, 0.10, 0.40, 0.50]
        }
    } else {
        if strength < 0.45 {
            [0.0, 0.85, 0.10, 0.05]
        } else if strength < 0.65 {
            [0.0, 0.65, 0.25, 0.10]
        } else if strength < 0.85 {
            [0.0, 0.20, 0.60, 0.20]
        } else {
            [0.0, 0.05, 0.35, 0.60]
        }
    }
}

fn simulate_bot_action(strategy: &[f32; 4], bot_elo: i32, offset: u64) -> (String, f32) {
    let mut max_prob = 0.0;
    let mut optimal_idx = 0;
    for i in 0..4 {
        if strategy[i] > max_prob {
            max_prob = strategy[i];
            optimal_idx = i;
        }
    }
    let error_chance = (1.0 - (bot_elo as f32 / 2100.0)).clamp(0.05, 0.60);

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + offset;
    let pseudo_random = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let rand_val = ((pseudo_random >> 33) % 100) as f32 / 100.0;

    let chosen_idx = if rand_val < error_chance {
        let mut fallback = (pseudo_random % 4) as usize;
        if fallback == optimal_idx {
            fallback = (fallback + 1) % 4;
        }
        fallback
    } else {
        optimal_idx
    };
    let ev_loss = (max_prob - strategy[chosen_idx]) * 100.0;
    let actions = ["FOLD", "CALL", "RAISE 33%", "RAISE 75%"];
    (actions[chosen_idx].to_string(), ev_loss)
}

fn init_db() -> Result<()> {
    let conn = Connection::open("poker.db")?;
    conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL, elo_train INTEGER NOT NULL DEFAULT 1000, elo_1v1 INTEGER NOT NULL DEFAULT 1000, elo_1v7 INTEGER NOT NULL DEFAULT 1000, streak INTEGER NOT NULL DEFAULT 0, hands_played INTEGER NOT NULL DEFAULT 0)", [])?;
    conn.execute("CREATE TABLE IF NOT EXISTS stats (id INTEGER PRIMARY KEY, elo_train INTEGER NOT NULL DEFAULT 1000, elo_1v1 INTEGER NOT NULL DEFAULT 1000, elo_1v7 INTEGER NOT NULL DEFAULT 1000, streak INTEGER NOT NULL DEFAULT 0, hands_played INTEGER NOT NULL DEFAULT 0)", [])?;
    let count: i32 = conn.query_row("SELECT COUNT(*) FROM stats", [], |row| row.get(0))?;
    if count == 0 {
        conn.execute("INSERT INTO stats (id, elo_train, elo_1v1, elo_1v7, streak, hands_played) VALUES (1, 1000, 1000, 1000, 0, 0)", [])?;
    }
    Ok(())
}

// --- HANDLERY AXUM ---
async fn login_handler(Json(payload): Json<AuthRequest>) -> Json<UserResponse> {
    let conn = Connection::open("poker.db").unwrap();
    let mut stmt = conn.prepare("SELECT id, username, elo_train, elo_1v1, elo_1v7 FROM users WHERE username = ?1 AND password = ?2").unwrap();
    match stmt.query_row(params![payload.username, payload.password], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i32>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, i32>(4)?,
        ))
    }) {
        Ok((id, username, et, e1, e7)) => Json(UserResponse {
            success: true,
            message: "OK".to_string(),
            id: Some(id),
            username: Some(username),
            elo_train: Some(et),
            elo_1v1: Some(e1),
            elo_1v7: Some(e7),
        }),
        Err(_) => Json(UserResponse {
            success: false,
            message: "Błędne dane".to_string(),
            id: None,
            username: None,
            elo_train: None,
            elo_1v1: None,
            elo_1v7: None,
        }),
    }
}

async fn register_handler(Json(payload): Json<AuthRequest>) -> Json<UserResponse> {
    let conn = Connection::open("poker.db").unwrap();
    match conn.execute(
        "INSERT INTO users (username, password) VALUES (?1, ?2)",
        params![payload.username, payload.password],
    ) {
        Ok(_) => Json(UserResponse {
            success: true,
            message: "OK".to_string(),
            id: Some(conn.last_insert_rowid() as i32),
            username: Some(payload.username),
            elo_train: Some(1000),
            elo_1v1: Some(1000),
            elo_1v7: Some(1000),
        }),
        Err(_) => Json(UserResponse {
            success: false,
            message: "Zajęte".to_string(),
            id: None,
            username: None,
            elo_train: None,
            elo_1v1: None,
            elo_1v7: None,
        }),
    }
}

async fn get_stats_handler() -> Json<StatsResponse> {
    let conn = Connection::open("poker.db").unwrap();
    let mut stmt = conn
        .prepare("SELECT elo_train, elo_1v1, elo_1v7, streak, hands_played FROM stats WHERE id = 1")
        .unwrap();
    let stats = stmt
        .query_row([], |row| {
            Ok(StatsResponse {
                elo_train: row.get(0)?,
                elo_1v1: row.get(1)?,
                elo_1v7: row.get(2)?,
                streak: row.get(3)?,
                hands_played: row.get(4)?,
            })
        })
        .unwrap_or(StatsResponse {
            elo_train: 1000,
            elo_1v1: 1000,
            elo_1v7: 1000,
            streak: 0,
            hands_played: 0,
        });
    Json(stats)
}

async fn update_stats_handler(Json(payload): Json<UpdateStatsRequest>) -> Json<serde_json::Value> {
    let conn = Connection::open("poker.db").unwrap();
    if let Some(uid) = payload.user_id {
        let q = match payload.mode.as_str() {
            "1v1" => "UPDATE users SET elo_1v1 = ?1, streak = ?2, hands_played = ?3 WHERE id = ?4",
            "1v7" => "UPDATE users SET elo_1v7 = ?1, streak = ?2, hands_played = ?3 WHERE id = ?4",
            _ => "UPDATE users SET elo_train = ?1, streak = ?2, hands_played = ?3 WHERE id = ?4",
        };
        let _ = conn.execute(
            q,
            params![payload.elo, payload.streak, payload.hands_played, uid],
        );
    } else {
        let q = match payload.mode.as_str() {
            "1v1" => "UPDATE stats SET elo_1v1 = ?1, streak = ?2, hands_played = ?3 WHERE id = 1",
            "1v7" => "UPDATE stats SET elo_1v7 = ?1, streak = ?2, hands_played = ?3 WHERE id = 1",
            _ => "UPDATE stats SET elo_train = ?1, streak = ?2, hands_played = ?3 WHERE id = 1",
        };
        let _ = conn.execute(
            q,
            params![payload.elo, payload.streak, payload.hands_played],
        );
    }
    Json(serde_json::json!({"status": "ok"}))
}

async fn solve_handler(Query(params): Query<SolveQuery>) -> Json<serde_json::Value> {
    let board = params.board.unwrap_or_default();
    let pos = params.pos.unwrap_or_default();
    let history = params.history.unwrap_or_default();
    let strategy = get_smart_gto_strategy(&params.hand, &board, &history, &pos);
    Json(serde_json::json!({
        "strategy": strategy,
        "equity": evaluate_poker_strength(&params.hand, &board),
        "villain_range": []
    }))
}

// NOWOŚĆ: Handler obsługujący Arenę 1v1
async fn arena_handler(Query(params): Query<ArenaQuery>) -> Json<serde_json::Value> {
    let board = params.board.unwrap_or_default();
    let pos = params.pos.unwrap_or_default();
    let strategy = get_smart_gto_strategy(&params.hand, &board, "", &pos);

    // Obliczamy losowy ruch bota bazując na jego ELO (wyższa szansa na błąd przy niskim ELO)
    let (bot_action, bot_damage) = simulate_bot_action(&strategy, params.bot_elo, 0);

    Json(serde_json::json!({
        "strategy": strategy,
        "bot_action": bot_action,
        "bot_damage": bot_damage
    }))
}

// NOWOŚĆ: Dummy handler dla tabelek Preflop (zwraca pusty JSON, aby zadowolić frontend)
async fn preflop_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

// === WEBSOCKET: MAGICZNY REAL-TIME DLA ARENY 1v7 ===
async fn ws_arena_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    if let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            if let Ok(text) = msg.to_text() {
                if let Ok(req) = serde_json::from_str::<WsArenaRequest>(text) {
                    let strategy = get_smart_gto_strategy(&req.hand, &req.board, "", &req.pos);

                    // Najpierw wysyłamy strategię dla Hero
                    let strat_msg = serde_json::json!({ "type": "strategy", "strategy": strategy });
                    let _ = socket.send(Message::Text(strat_msg.to_string())).await;

                    // Definiujemy boty
                    let bot_elos = [800, 1000, 1200, 1500, 1800, 2100, 2500];

                    // STRUMIENIOWANIE - Boty grają po kolei z opóźnieniem!
                    for (i, elo) in bot_elos.iter().enumerate() {
                        // Sztuczne "myślenie" bota - 800 milisekund przerwy
                        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

                        let (action, damage) =
                            simulate_bot_action(&strategy, *elo, (i * 1000) as u64);
                        let bot_msg = serde_json::json!({
                            "type": "bot_action", "bot_id": i + 1, "action": action, "damage": damage
                        });
                        if socket
                            .send(Message::Text(bot_msg.to_string()))
                            .await
                            .is_err()
                        {
                            break; // Rozłączono
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    init_db().expect("Błąd bazy");
    println!("🚀 Uruchamianie ASYNCHRONICZNEGO serwera (Tokio + Axum)...");

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/login", post(login_handler))
        .route("/api/register", post(register_handler))
        .route(
            "/api/stats",
            get(get_stats_handler).post(update_stats_handler),
        )
        .route("/api/solve", get(solve_handler))
        .route("/api/arena", get(arena_handler)) // PODPIĘTO ARENĘ 1v1
        .route("/api/preflop", get(preflop_handler)) // PODPIĘTO DUMMY PREFLOP
        .route("/ws/arena8", get(ws_arena_handler))
        .layer(cors);

    // CHMURA: Pobieramy port przydzielony przez serwer, a domyślnie używamy 3001 (dla testów u Ciebie)
    let port = std::env::var("PORT").unwrap_or_else(|_| "3001".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("✅ Gotowe! Serwer działa na adresie: {}", addr);

    axum::serve(listener, app).await.unwrap();
}
