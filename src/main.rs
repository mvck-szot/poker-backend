use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};

// --- STRUKTURY DANYCH BAZOWE ---
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

#[derive(Serialize, Deserialize)]
struct WsArenaRequest {
    hand: String,
    board: String,
    pos: String,
}

// --- STRUKTURY DLA ZNAJOMYCH & RANKINGU ---
#[derive(Serialize, Deserialize)]
struct FriendRequest {
    user_id: i32,
    friend_username: String,
}

#[derive(Serialize, Deserialize)]
struct FriendAction {
    user_id: i32,
    friend_id: i32,
}

#[derive(Serialize, Deserialize)]
struct FriendInfo {
    id: i32,
    username: String,
    elo_1v1: i32,
    status: String,
}

// NOWOŚĆ: Struktura dla rankingu
#[derive(Serialize)]
struct LeaderboardEntry {
    rank: usize,
    username: String,
    elo: i32,
}

// --- ZAAWANSOWANE STRUKTURY DLA GTO DUEL (LIVE) ---
#[derive(Clone)]
struct DuelPlayer {
    user_id: i32,
    username: String,
    hp: i32,
    action: Option<usize>,
    sender: mpsc::UnboundedSender<String>,
}

struct DuelRoom {
    players: Vec<DuelPlayer>,
    strategy: Option<[f32; 4]>,
}

type SharedState = Arc<Mutex<HashMap<String, DuelRoom>>>;

#[derive(Deserialize)]
struct DuelQuery {
    room_id: String,
    user_id: i32,
    username: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum DuelClientMessage {
    #[serde(rename = "new_round")]
    NewRound {
        hand: String,
        board: String,
        pos: String,
    },
    #[serde(rename = "action")]
    Action { action_idx: usize },
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
    conn.execute("CREATE TABLE IF NOT EXISTS friends (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, friend_id INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', UNIQUE(user_id, friend_id))", [])?;

    let count: i32 = conn.query_row("SELECT COUNT(*) FROM stats", [], |row| row.get(0))?;
    if count == 0 {
        conn.execute("INSERT INTO stats (id, elo_train, elo_1v1, elo_1v7, streak, hands_played) VALUES (1, 1000, 1000, 1000, 0, 0)", [])?;
    }
    Ok(())
}

async fn update_duel_elo(winner_id: i32, loser_id: i32) -> Result<(), rusqlite::Error> {
    let conn = Connection::open("poker.db")?;
    conn.execute("UPDATE users SET elo_1v1 = elo_1v1 + 25, streak = streak + 1, hands_played = hands_played + 1 WHERE id = ?1", params![winner_id])?;
    conn.execute("UPDATE users SET elo_1v1 = MAX(0, elo_1v1 - 25), streak = 0, hands_played = hands_played + 1 WHERE id = ?1", params![loser_id])?;
    Ok(())
}

// --- HANDLERY AXUM (REST API) ---
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

// NOWOŚĆ: Handler dla pobierania rankingu (TOP 10 w trybie 1v1)
async fn get_leaderboard_handler() -> Json<Vec<LeaderboardEntry>> {
    let conn = Connection::open("poker.db").unwrap();
    let mut stmt = conn
        .prepare("SELECT username, elo_1v1 FROM users ORDER BY elo_1v1 DESC LIMIT 10")
        .unwrap();

    let mut leaderboard = Vec::new();
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
        })
        .unwrap();

    for (i, row) in rows.enumerate() {
        if let Ok((username, elo)) = row {
            leaderboard.push(LeaderboardEntry {
                rank: i + 1,
                username,
                elo,
            });
        }
    }
    Json(leaderboard)
}

async fn solve_handler(Query(params): Query<SolveQuery>) -> Json<serde_json::Value> {
    let board = params.board.unwrap_or_default();
    let pos = params.pos.unwrap_or_default();
    let history = params.history.unwrap_or_default();
    let strategy = get_smart_gto_strategy(&params.hand, &board, &history, &pos);
    Json(
        serde_json::json!({ "strategy": strategy, "equity": evaluate_poker_strength(&params.hand, &board), "villain_range": [] }),
    )
}

async fn arena_handler(Query(params): Query<ArenaQuery>) -> Json<serde_json::Value> {
    let board = params.board.unwrap_or_default();
    let pos = params.pos.unwrap_or_default();
    let strategy = get_smart_gto_strategy(&params.hand, &board, "", &pos);
    let (bot_action, bot_damage) = simulate_bot_action(&strategy, params.bot_elo, 0);
    Json(
        serde_json::json!({ "strategy": strategy, "bot_action": bot_action, "bot_damage": bot_damage }),
    )
}

async fn preflop_handler() -> Json<serde_json::Value> {
    // Generujemy uproszczone, ale realistyczne zakresy GTO (Proof of Concept)
    // R - Raise (Czerwony), C - Call (Zielony), F - Fold (Szary)

    let mut utg_range = HashMap::new();
    let mut btn_range = HashMap::new();
    let ranks = [
        "A", "K", "Q", "J", "T", "9", "8", "7", "6", "5", "4", "3", "2",
    ];

    for i in 0..13 {
        for j in 0..13 {
            let hand = if i == j {
                format!("{}{}", ranks[i], ranks[j]) // Pary np. AA, KK
            } else if i < j {
                format!("{}s", format!("{}{}", ranks[i], ranks[j])) // Suited np. AKs
            } else {
                format!("{}o", format!("{}{}", ranks[j], ranks[i])) // Offsuit np. AKo
            };

            // Logika dla UTG (Bardzo ciasno - tight)
            let utg_action = if i == j && i <= 6 {
                "R"
            }
            // Pary 88+
            else if hand == "AKs" || hand == "AQs" || hand == "AJs" || hand == "KQs" {
                "R"
            } else if hand == "AKo" || hand == "AQo" {
                "R"
            } else {
                "F"
            };
            utg_range.insert(hand.clone(), utg_action);

            // Logika dla BTN (Szeroko - loose)
            let btn_action = if i == j {
                "R"
            }
            // Wszystkie pary
            else if i == 0 {
                "R"
            }
            // Wszystkie asy (A2s+, A2o+)
            else if (i < 5 && j < 5) || (hand.contains('s') && i <= 8 && j - i <= 2) {
                "R"
            }
            // Broadwaye i suited connectory
            else if hand == "KJo" || hand == "QJo" {
                "C"
            } else {
                "F"
            };
            btn_range.insert(hand, btn_action);
        }
    }

    Json(serde_json::json!({
        "UTG": utg_range,
        "BTN": btn_range
    }))
}

async fn send_friend_request(Json(payload): Json<FriendRequest>) -> Json<serde_json::Value> {
    let conn = Connection::open("poker.db").unwrap();
    let friend_id: Result<i32, _> = conn.query_row(
        "SELECT id FROM users WHERE username = ?1",
        params![payload.friend_username],
        |row| row.get(0),
    );
    match friend_id {
        Ok(fid) => {
            if fid == payload.user_id {
                return Json(
                    serde_json::json!({"success": false, "message": "Nie możesz dodać samego siebie"}),
                );
            }
            let _ = conn.execute("INSERT OR IGNORE INTO friends (user_id, friend_id, status) VALUES (?1, ?2, 'pending')", params![payload.user_id, fid]);
            Json(serde_json::json!({"success": true, "message": "Wysłano zaproszenie!"}))
        }
        Err(_) => Json(serde_json::json!({"success": false, "message": "Nie znaleziono gracza"})),
    }
}

async fn accept_friend_request(Json(payload): Json<FriendAction>) -> Json<serde_json::Value> {
    let conn = Connection::open("poker.db").unwrap();
    let _ = conn.execute(
        "UPDATE friends SET status = 'accepted' WHERE user_id = ?1 AND friend_id = ?2",
        params![payload.friend_id, payload.user_id],
    );
    let _ = conn.execute(
        "INSERT OR IGNORE INTO friends (user_id, friend_id, status) VALUES (?1, ?2, 'accepted')",
        params![payload.user_id, payload.friend_id],
    );
    Json(serde_json::json!({"success": true, "message": "Zaakceptowano!"}))
}

async fn get_friends(Query(params): Query<HashMap<String, String>>) -> Json<Vec<FriendInfo>> {
    let user_id = params
        .get("user_id")
        .and_then(|id| id.parse::<i32>().ok())
        .unwrap_or(0);
    let conn = Connection::open("poker.db").unwrap();
    let mut friends_list = Vec::new();
    let mut stmt = conn.prepare("
        SELECT u.id, u.username, u.elo_1v1, f.status FROM users u JOIN friends f ON u.id = f.friend_id WHERE f.user_id = ?1 AND f.status = 'accepted'
        UNION
        SELECT u.id, u.username, u.elo_1v1, f.status FROM users u JOIN friends f ON u.id = f.user_id WHERE f.friend_id = ?1 AND f.status = 'pending'
    ").unwrap();
    let friend_iter = stmt
        .query_map(params![user_id], |row| {
            Ok(FriendInfo {
                id: row.get(0)?,
                username: row.get(1)?,
                elo_1v1: row.get(2)?,
                status: row.get(3)?,
            })
        })
        .unwrap();
    for f in friend_iter {
        if let Ok(friend) = f {
            friends_list.push(friend);
        }
    }
    Json(friends_list)
}

// === WEBSOCKET 1v7 (STARE) ===
async fn ws_arena_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    if let Some(msg) = socket.recv().await {
        if let Ok(msg) = msg {
            if let Ok(text) = msg.to_text() {
                if let Ok(req) = serde_json::from_str::<WsArenaRequest>(text) {
                    let strategy = get_smart_gto_strategy(&req.hand, &req.board, "", &req.pos);
                    let strat_msg = serde_json::json!({ "type": "strategy", "strategy": strategy });
                    let _ = socket.send(Message::Text(strat_msg.to_string())).await;
                    let bot_elos = [800, 1000, 1200, 1500, 1800, 2100, 2500];
                    for (i, elo) in bot_elos.iter().enumerate() {
                        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                        let (action, damage) =
                            simulate_bot_action(&strategy, *elo, (i * 1000) as u64);
                        let bot_msg = serde_json::json!({ "type": "bot_action", "bot_id": i + 1, "action": action, "damage": damage });
                        if socket
                            .send(Message::Text(bot_msg.to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        }
    }
}

// === WEBSOCKET 1v1 LIVE (GTO DUEL) ===
async fn ws_duel_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<DuelQuery>,
    State(state): State<SharedState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_duel_socket(socket, params, state))
}

async fn handle_duel_socket(socket: WebSocket, params: DuelQuery, state: SharedState) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    {
        let mut rooms = state.lock().unwrap();
        let room = rooms
            .entry(params.room_id.clone())
            .or_insert_with(|| DuelRoom {
                players: Vec::new(),
                strategy: None,
            });
        if room.players.len() >= 2 {
            let _ = tx
                .send(serde_json::json!({"type": "error", "msg": "Pokój jest pełny"}).to_string());
            return;
        }
        room.players.push(DuelPlayer {
            user_id: params.user_id,
            username: params.username.clone(),
            hp: 1000,
            action: None,
            sender: tx.clone(),
        });
        if room.players.len() == 2 {
            let ready_msg = serde_json::json!({"type": "ready"}).to_string();
            for p in &room.players {
                let _ = p.sender.send(ready_msg.clone());
            }
        }
    }

    while let Some(Ok(msg)) = receiver.next().await {
        if let Ok(text) = msg.to_text() {
            if let Ok(client_msg) = serde_json::from_str::<DuelClientMessage>(text) {
                match client_msg {
                    DuelClientMessage::NewRound { hand, board, pos } => {
                        let mut rooms = state.lock().unwrap();
                        if let Some(room) = rooms.get_mut(&params.room_id) {
                            let strategy = get_smart_gto_strategy(&hand, &board, "", &pos);
                            room.strategy = Some(strategy);
                            let start_msg = serde_json::json!({ "type": "round_start", "hand": hand, "board": board, "pos": pos }).to_string();
                            for p in &mut room.players {
                                p.action = None;
                                let _ = p.sender.send(start_msg.clone());
                            }
                        }
                    }
                    DuelClientMessage::Action { action_idx } => {
                        process_duel_action(&params.room_id, params.user_id, action_idx, &state);
                    }
                }
            }
        }
    }

    {
        let mut rooms = state.lock().unwrap();
        if let Some(room) = rooms.get_mut(&params.room_id) {
            room.players.retain(|p| p.user_id != params.user_id);
            let disc_msg = serde_json::json!({"type": "opponent_disconnected"}).to_string();
            for p in &room.players {
                let _ = p.sender.send(disc_msg.clone());
            }
            if room.players.is_empty() {
                rooms.remove(&params.room_id);
            }
        }
    }
}

fn process_duel_action(room_id: &str, user_id: i32, action_idx: usize, state: &SharedState) {
    let mut rooms = state.lock().unwrap();
    if let Some(room) = rooms.get_mut(room_id) {
        for p in &mut room.players {
            if p.user_id == user_id {
                p.action = Some(action_idx);
            }
        }
        if room.players.len() == 2
            && room.players[0].action.is_some()
            && room.players[1].action.is_some()
        {
            if let Some(strategy) = room.strategy {
                let mut max_prob = 0.0;
                for prob in &strategy {
                    if *prob > max_prob {
                        max_prob = *prob;
                    }
                }
                let a1 = room.players[0].action.unwrap();
                let a2 = room.players[1].action.unwrap();
                let loss1 = ((max_prob - strategy[a1]) * 100.0).round() as i32;
                let loss2 = ((max_prob - strategy[a2]) * 100.0).round() as i32;
                room.players[0].hp = std::cmp::max(0, room.players[0].hp - loss1);
                room.players[1].hp = std::cmp::max(0, room.players[1].hp - loss2);

                let mut game_over = false;
                let mut winner_id = None;
                let mut loser_id = None;
                if room.players[0].hp == 0 || room.players[1].hp == 0 {
                    game_over = true;
                    if room.players[0].hp > room.players[1].hp {
                        winner_id = Some(room.players[0].user_id);
                        loser_id = Some(room.players[1].user_id);
                    } else if room.players[1].hp > room.players[0].hp {
                        winner_id = Some(room.players[1].user_id);
                        loser_id = Some(room.players[0].user_id);
                    }
                }

                let result_msg = serde_json::json!({
                    "type": "round_result", "p1_id": room.players[0].user_id, "p1_hp": room.players[0].hp, "p1_loss": loss1,
                    "p2_id": room.players[1].user_id, "p2_hp": room.players[1].hp, "p2_loss": loss2, "game_over": game_over, "winner_id": winner_id,
                });
                for p in &mut room.players {
                    let _ = p.sender.send(result_msg.to_string());
                    p.action = None;
                }
                if game_over {
                    if let (Some(w), Some(l)) = (winner_id, loser_id) {
                        tokio::spawn(async move {
                            let _ = update_duel_elo(w, l).await;
                        });
                    }
                    rooms.remove(room_id);
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
    let shared_state: SharedState = Arc::new(Mutex::new(HashMap::new()));

    let app = Router::new()
        .route("/api/login", post(login_handler))
        .route("/api/register", post(register_handler))
        .route(
            "/api/stats",
            get(get_stats_handler).post(update_stats_handler),
        )
        .route("/api/solve", get(solve_handler))
        .route("/api/arena", get(arena_handler))
        .route("/api/preflop", get(preflop_handler))
        .route("/api/friends/add", post(send_friend_request))
        .route("/api/friends/accept", post(accept_friend_request))
        .route("/api/friends/list", get(get_friends))
        .route("/api/leaderboard", get(get_leaderboard_handler)) // NOWE: RANKING
        .route("/ws/arena8", get(ws_arena_handler))
        .route("/ws/duel", get(ws_duel_handler))
        .layer(cors)
        .with_state(shared_state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3001".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("✅ Gotowe! Serwer działa na adresie: {}", addr);

    axum::serve(listener, app).await.unwrap();
}
