import sqlite3
import os
from src.agent.parser import ParsedEvent

DB_PATH = os.getenv("EVENT_DB_PATH", "data/events.db")

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    con = get_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id   TEXT NOT NULL,
            event_type    TEXT,
            start_date    TEXT,
            end_date      TEXT,
            confidence    REAL,
            special_event INTEGER,
            notes         TEXT,
            created_at    TEXT DEFAULT (date('now'))
        )
    """)
    con.commit()
    con.close()

def save_event(customer_id: str, event: ParsedEvent) -> bool:
    """Save event only if same customer + event_type + start_date doesn't exist."""
    con = get_connection()
    existing = con.execute("""
        SELECT id FROM events
        WHERE customer_id = ?
          AND event_type = ?
          AND start_date = ?
    """, (customer_id, event.event_type, event.start_date)).fetchone()

    if existing:
        con.close()
        return False  # duplicate, skip

    con.execute("""
        INSERT INTO events
            (customer_id, event_type, start_date, end_date, confidence, special_event, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        customer_id,
        event.event_type,
        event.start_date,
        event.end_date,
        event.confidence,
        int(event.special_event),
        event.notes
    ))
    con.commit()
    con.close()
    return True  # saved

def get_recent_events(customer_id: str, limit: int = 5) -> list[dict]:
    con = get_connection()
    cur = con.execute("""
        SELECT event_type, start_date, end_date, confidence, notes, created_at
        FROM events
        WHERE customer_id = ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (customer_id, limit))
    rows = cur.fetchall()
    con.close()
    return [
        {
            "event_type": r[0],
            "start_date": r[1],
            "end_date": r[2],
            "confidence": r[3],
            "notes": r[4],
            "created_at": r[5]
        }
        for r in rows
    ]

def check_conflict(customer_id: str, event: ParsedEvent) -> str | None:
    """Check if new event overlaps with an existing one for the same customer."""
    if not event.start_date:
        return None
    recent = get_recent_events(customer_id)
    for past in recent:
        if past["start_date"] and past["end_date"] and event.end_date:
            if past["start_date"] <= event.start_date <= past["end_date"]:
                return f"Overlaps with existing {past['event_type']} from {past['start_date']} to {past['end_date']}"
    return None
