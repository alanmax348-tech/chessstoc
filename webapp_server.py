"""
Telegram Mini App chess server.
FastAPI + python-telegram-bot (v21.4) + Redis, single process.

Run:  python webapp_server.py
"""
import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl

import chess
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from telegram import (BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
                      InlineQueryResultArticle, InputTextMessageContent,
                      MenuButtonWebApp, Update, WebAppInfo)
from telegram.ext import (Application, CallbackQueryHandler, ChosenInlineResultHandler,
                          CommandHandler, ContextTypes, InlineQueryHandler)

log = logging.getLogger("chess")
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------- config
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBAPP_BASE_URL = os.getenv("WEBAPP_BASE_URL", "http://localhost:8000/webapp")
STOCKFISH_PATH = os.getenv("STOCKFISH_PATH", "stockfish")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
API_HOST = os.getenv("WEBAPP_API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("WEBAPP_API_PORT", "8000"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MINIAPP_SHORT_NAME = os.getenv("MINIAPP_SHORT_NAME", "game")
RANDOM_COLORS = os.getenv("RANDOM_COLORS", "0") == "1"
DEV_MODE = os.getenv("DEV_MODE", "0") == "1"
REVIEW_DEPTH = int(os.getenv("REVIEW_DEPTH", "12"))
REVIEW_TZ = os.getenv("REVIEW_TZ", "UTC")            # timezone whose 00:00 resets the daily review
REVIEW_DAILY_LIMIT = os.getenv("REVIEW_DAILY_LIMIT", "1") == "1"   # set 0 to disable the limit
STATIC_DIR = Path(__file__).parent / "static"

INVITE_TTL = 600            # exactly 10 minutes
REVIEW_TTL = 86400          # exactly 1 day
DISCONNECT_LIMIT = 60       # seconds before an absent player forfeits (friend games)

TIME_CONTROLS = {           # key: (base seconds, increment seconds, label)
    "bullet": (60, 0, "Bullet 1|0"),
    "blitz": (180, 2, "Blitz 3|2"),
    "rapid": (600, 5, "Rapid 10|5"),
    "secret_queen": (600, 5, "Secret Queen 10|5"),
    "bot": (600, 5, "Play Vs Bot 10|5"),
}
DIFFICULTIES = {
    "kid": dict(name="Kid", elo=100, blunder=0.55, skill=0, depth=1),
    "beginner": dict(name="Beginner", elo=500, blunder=0.25, skill=0, depth=1),
    "intermediate": dict(name="Intermediate", elo=1000, blunder=0.08, skill=4, depth=3),
    "advanced": dict(name="Advanced", elo=1500, uci_elo=1500, movetime=200),
    "magnus": dict(name="Magnus Carlsen", elo=2700, uci_elo=2700, movetime=1000),
}
VAL = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9}

EXPIRED_TEXT = ("No one joined the game, the invitation is no longer valid.\n\n"
                "To create a new game invitation, click one of the buttons below or go to the chat "
                "which you want to send the invitation to, type in @{bot}, and add a space.")


def card_text(name: str) -> str:
    return (f"User {name} wants to play chess.\n"
            "Game Rules: Timer: 3 min + 2 sec. Random color.\n"
            "Click the button below to join the game.")


# --------------------------------------------------------------------------- redis
class MemoryRedis:
    """Tiny fallback so the app runs without Redis in development."""
    def __init__(self):
        self.kv, self.z = {}, {}

    async def ping(self): return True
    async def get(self, k):
        v = self.kv.get(k)
        if not v: return None
        if v[1] and v[1] < time.time():
            self.kv.pop(k, None); return None
        return v[0]
    async def set(self, k, v, ex=None): self.kv[k] = (v, time.time() + ex if ex else None)
    async def zadd(self, k, m): self.z.setdefault(k, {}).update(m)
    async def zrangebyscore(self, k, lo, hi): return [m for m, s in self.z.get(k, {}).items() if lo <= s <= hi]
    async def zrem(self, k, *ms):
        for m in ms: self.z.get(k, {}).pop(m, None)


R = None
TG: Optional[Application] = None
BOT_USERNAME = ""
GAMES: dict = {}


# --------------------------------------------------------------------------- auth
def verify_init_data(init_data: str) -> dict:
    if DEV_MODE and init_data.startswith("dev:"):
        _, uid, name = init_data.split(":", 2)
        return {"id": int(uid), "first_name": name, "username": name.lower()}
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        their_hash = pairs.pop("hash")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        mine = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mine, their_hash):
            raise ValueError("bad hash")
        if time.time() - int(pairs.get("auth_date", "0")) > 86400 * 2:
            raise ValueError("expired")
        u = json.loads(pairs["user"])
        return {"id": u["id"], "first_name": u.get("first_name", ""), "last_name": u.get("last_name", ""),
                "username": u.get("username", ""), "photo_url": u.get("photo_url")}
    except Exception:
        raise HTTPException(401, "Invalid Telegram initData")


def pub(u):
    if not u: return None
    name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip() or u.get("name", "Player")
    return {"id": u["id"], "name": name, "username": u.get("username", ""),
            "photo": u.get("photo_url"), "bot": u.get("bot", False), "elo": u.get("elo")}


# --------------------------------------------------------------------------- game model
class IllegalMove(Exception):
    pass


def other(c): return "b" if c == "w" else "w"


class Game:
    def __init__(self, gid, mode, tc_key, host=None):
        self.id, self.mode, self.tc_key = gid, mode, tc_key          # mode: friend | bot
        self.variant = "secret_queen" if tc_key == "secret_queen" else "standard"
        base, inc, _ = TIME_CONTROLS[tc_key]
        self.base, self.inc = base, inc
        self.players = {"w": None, "b": None}
        self.host_id = host["id"] if host else None
        self.board = chess.Board()
        self.moves = []
        self.status = "waiting"          # waiting | setup | active | over | cancelled
        self.result = self.reason = self.winner = None
        self.clocks = {"w": base * 1000.0, "b": base * 1000.0}
        self.last_ts = None
        self.secret = {"w": None, "b": None}
        self.revealed = {"w": False, "b": False}
        self.captured = {"w": [], "b": []}
        self.draw_offer = None
        self.bot_key = None
        self.bot_color = None
        self.inline_message_id = None
        self.rematch_id = None
        self.last_event = None
        self.created = time.time()
        # runtime only
        self.conns = set()
        self.lock = asyncio.Lock()
        self.bot_busy = False
        self.away = {}

    # ---- persistence
    def to_dict(self):
        return {k: getattr(self, k) for k in (
            "id", "mode", "tc_key", "players", "host_id", "moves", "status", "result", "reason", "winner",
            "clocks", "last_ts", "secret", "revealed", "captured", "draw_offer", "bot_key", "bot_color",
            "inline_message_id", "rematch_id", "last_event", "created")}

    @classmethod
    def from_dict(cls, d):
        g = cls(d["id"], d["mode"], d["tc_key"])
        for k, v in d.items():
            if k not in ("id", "mode", "tc_key"): setattr(g, k, v)
        g.board = chess.Board()
        for m in g.moves:
            mv = chess.Move.from_uci(m["uci"])
            if m.get("reveal"):
                g.board.set_piece_at(mv.from_square, chess.Piece(chess.QUEEN, g.board.turn))
            g.board.push(mv)
        return g

    # ---- helpers
    def role_of(self, uid):
        for c in "wb":
            if self.players[c] and self.players[c]["id"] == uid: return c
        return "spectator"

    def turn(self): return "w" if self.board.turn else "b"

    def humans(self): return [c for c in "wb" if self.players[c] and not self.players[c].get("bot")]

    def clocks_now(self):
        c = dict(self.clocks)
        if self.status == "active" and self.last_ts:
            t = self.turn()
            c[t] = max(0.0, c[t] - (time.time() - self.last_ts) * 1000)
        return c

    def legal_ucis(self, color):
        """Legal moves for `color` (must be side to move). Includes hidden secret-queen moves."""
        b = self.board
        out = {m.uci() for m in b.legal_moves}
        sq = self.secret[color]
        if self.variant == "secret_queen" and sq is not None and not self.revealed[color]:
            p = b.piece_at(sq)
            if p and p.piece_type == chess.PAWN:
                tmp = b.copy(stack=False)
                tmp.set_piece_at(sq, chess.Piece(chess.QUEEN, b.turn))
                out |= {m.uci() for m in tmp.legal_moves if m.from_square == sq}
        return sorted(out)

    def material(self, color):
        return sum(VAL[t] * len(self.board.pieces(t, color)) for t in VAL)

    def _counts(self, color):
        return {t: len(self.board.pieces(t, color)) for t in (chess.PAWN, chess.KNIGHT, chess.BISHOP, chess.ROOK, chess.QUEEN)}

    def insufficient(self):
        w, k = self._counts(chess.WHITE), self._counts(chess.BLACK)
        if any(m[t] for m in (w, k) for t in (chess.PAWN, chess.ROOK, chess.QUEEN)): return False
        wm, km = w[chess.KNIGHT] + w[chess.BISHOP], k[chess.KNIGHT] + k[chess.BISHOP]
        if wm + km <= 1: return True                                   # K v K, K+minor v K
        if wm == 1 and km == 1: return True                            # K+minor v K+minor
        if (w[chess.KNIGHT] == 2 and wm == 2 and km == 0) or (k[chess.KNIGHT] == 2 and km == 2 and wm == 0):
            return True                                                # K+2N v K
        return False

    def can_mate(self, color):
        m = self._counts(chess.WHITE if color == "w" else chess.BLACK)
        if m[chess.PAWN] or m[chess.ROOK] or m[chess.QUEEN]: return True
        return m[chess.BISHOP] >= 2 or (m[chess.BISHOP] and m[chess.KNIGHT]) or m[chess.KNIGHT] >= 3

    def finish(self, winner, reason):
        self.status = "over"
        self.winner, self.reason = winner, reason
        self.result = "1-0" if winner == "w" else "0-1" if winner == "b" else "1/2-1/2"
        self.draw_offer = None
        self.last_ts = None

    def banner(self):
        if self.status != "over": return None
        if self.winner: return f"{'White' if self.winner == 'w' else 'Black'} won by {self.reason}"
        return f"Draw by {self.reason}"

    def start_clock(self):
        self.status = "active"
        self.last_ts = time.time()

    def maybe_start(self):
        """Move waiting/setup -> active once every human has finalised their pawn pick."""
        if not (self.players["w"] and self.players["b"]): return
        if self.variant == "secret_queen":
            if any(self.secret[c] is None for c in self.humans()):
                self.status = "setup"; return
        self.start_clock()

    # ---- move execution
    def play(self, color, mv_str):
        b = self.board
        try:
            move = chess.Move.from_uci(mv_str)
        except ValueError:
            try: move = b.parse_san(mv_str)
            except ValueError: raise IllegalMove()
        reveal = False
        if move not in b.legal_moves:
            sq = self.secret[color]
            if (self.variant == "secret_queen" and sq is not None and not self.revealed[color]
                    and move.from_square == sq and move.promotion is None):
                p = b.piece_at(sq)
                if p and p.piece_type == chess.PAWN:
                    tmp = b.copy(stack=False)
                    tmp.set_piece_at(sq, chess.Piece(chess.QUEEN, b.turn))
                    reveal = move in tmp.legal_moves
            if not reveal: raise IllegalMove()
            b.set_piece_at(sq, chess.Piece(chess.QUEEN, b.turn))
            self.revealed[color] = True
        cap = None
        if b.is_capture(move):
            cap = chess.Piece(chess.PAWN, not b.turn) if b.is_en_passant(move) else b.piece_at(move.to_square)
        castle, promo = b.is_castling(move), move.promotion is not None
        san = b.san(move)
        b.push(move)
        if cap: self.captured[color].append(cap.symbol().lower())
        self.moves.append({"uci": move.uci(), "san": san, "reveal": reveal})
        if self.variant == "secret_queen":
            for c, col in (("w", chess.WHITE), ("b", chess.BLACK)):
                sq = self.secret[c]
                if sq is None: continue
                if c == color and move.from_square == sq: sq = move.to_square
                p = b.piece_at(sq)
                want = chess.QUEEN if self.revealed[c] else chess.PAWN
                self.secret[c] = sq if (p and p.color == col and p.piece_type == want) else None
        # clocks
        now = time.time()
        if self.last_ts:
            self.clocks[color] -= (now - self.last_ts) * 1000
        self.clocks[color] += self.inc * 1000
        self.last_ts = now
        self.draw_offer = None
        self.last_event = {"ply": len(self.moves), "san": san, "capture": bool(cap), "check": b.is_check(),
                           "reveal": reveal, "castle": castle, "promo": promo, "square": chess.square_name(move.to_square)}
        self.evaluate_end()
        return self.last_event

    def evaluate_end(self):
        b, stm = self.board, self.turn()
        if not self.legal_ucis(stm):
            if b.is_check(): self.finish(other(stm), "Checkmate")
            else: self.finish(None, "Stalemate")
        elif self.insufficient(): self.finish(None, "Insufficient Material")
        elif b.halfmove_clock >= 100: self.finish(None, "50-Move Rule")
        elif b.is_repetition(3): self.finish(None, "Threefold Repetition")

    def timeout(self, loser):
        if self.can_mate(other(loser)): self.finish(other(loser), "Timeout")
        else: self.finish(None, "Timeout vs Insufficient Material")

    def pgn(self):
        n = lambda c: (pub(self.players[c]) or {"name": "?"})["name"]
        res = self.result or "*"
        hdr = [("Event", "Secret Queen Chess" if self.variant == "secret_queen" else "Telegram Chess"),
               ("Site", "Telegram Mini App"), ("Date", time.strftime("%Y.%m.%d", time.gmtime(self.created))),
               ("White", n("w")), ("Black", n("b")), ("Result", res), ("TimeControl", f"{self.base}+{self.inc}")]
        parts = []
        for i, m in enumerate(self.moves):
            if i % 2 == 0: parts.append(f"{i // 2 + 1}.")
            parts.append(m["san"])
        return "\n".join(f'[{k} "{v}"]' for k, v in hdr) + "\n\n" + " ".join(parts) + " " + res

    # ---- per-viewer state (secret info is only revealed to its owner)
    def view(self, uid):
        role = self.role_of(uid)
        b, turn = self.board, self.turn()
        legal = self.legal_ucis(turn) if (self.status == "active" and role == turn) else []
        cs = None
        if b.is_check() and self.status in ("active", "over"):
            k = b.king(b.turn)
            cs = chess.square_name(k) if k is not None else None
        c = self.clocks_now()
        lm = self.moves[-1]["uci"] if self.moves else None
        return {
            "id": self.id, "variant": self.variant, "mode": self.mode, "tc": self.tc_key,
            "tc_label": TIME_CONTROLS[self.tc_key][2], "status": self.status, "role": role,
            "fen": b.fen(), "turn": turn, "ply": len(self.moves), "last_move": lm,
            "players": {"w": pub(self.players["w"]), "b": pub(self.players["b"])},
            "clocks": c, "running": turn if self.status == "active" else None,
            "in_check": bool(cs), "check_sq": cs, "legal": legal,
            "captured": self.captured, "material_diff": self.material(chess.WHITE) - self.material(chess.BLACK),
            "result": self.result, "banner": self.banner(), "reason": self.reason, "winner": self.winner,
            "draw_offer": self.draw_offer, "event": self.last_event,
            "secret": {"own": chess.square_name(self.secret[role]) if role in "wb" and self.secret.get(role) is not None else None,
                       "picked": {c2: (self.secret[c2] is not None or self.revealed[c2]) for c2 in "wb"}},
            "review_available": self.variant == "standard" and self.status == "over" and len(self.moves) > 0,
            "pgn": self.pgn() if self.status == "over" else None,
            "rematch_id": self.rematch_id, "bot": DIFFICULTIES.get(self.bot_key, {}).get("name") if self.bot_key else None,
            "bot_key": self.bot_key, "expires": (self.created + INVITE_TTL) if self.status == "waiting" else None,
            "server_time": time.time(),
        }


async def save(g):
    await R.set(f"game:{g.id}", json.dumps(g.to_dict()), ex=7 * 86400)


async def get_game(gid) -> Game:
    g = GAMES.get(gid)
    if g: return g
    raw = await R.get(f"game:{gid}")
    if not raw: raise HTTPException(404, "Game not found")
    g = Game.from_dict(json.loads(raw))
    GAMES[gid] = g
    return g


async def broadcast(g):
    dead = []
    for conn in list(g.conns):
        ws, uid = conn
        try: await ws.send_json({"type": "state", "state": g.view(uid)})
        except Exception: dead.append(conn)
    for d in dead: g.conns.discard(d)


def new_id(): return uuid.uuid4().hex[:10]


# --------------------------------------------------------------------------- UCI engine (async subprocess)
class UCI:
    def __init__(self, path=None): self.path, self.p = path or STOCKFISH_PATH, None

    async def start(self):
        self.p = await asyncio.create_subprocess_exec(
            self.path, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        await self.send("uci"); await self.wait("uciok")

    async def send(self, cmd):
        self.p.stdin.write((cmd + "\n").encode()); await self.p.stdin.drain()

    async def wait(self, token, collect=None, timeout=60):
        while True:
            line = await asyncio.wait_for(self.p.stdout.readline(), timeout)
            if not line: raise RuntimeError("engine terminated")
            s = line.decode().strip()
            if collect is not None: collect.append(s)
            if s.startswith(token): return s

    async def ready(self):
        await self.send("isready"); await self.wait("readyok")

    async def setopt(self, name, value): await self.send(f"setoption name {name} value {value}")

    async def stop(self):
        try:
            await self.send("quit"); await asyncio.wait_for(self.p.wait(), 3)
        except Exception:
            try: self.p.kill()
            except Exception: pass

    async def evaluate(self, fen, depth):
        await self.send(f"position fen {fen}")
        lines = []
        await self.send(f"go depth {depth}")
        await self.wait("bestmove", lines)
        pv = {}
        for l in lines:
            if l.startswith("info") and " score " in l and " pv " in l:
                t = l.split()
                mpv = int(t[t.index("multipv") + 1]) if "multipv" in t else 1
                i = t.index("score")
                pv[mpv] = (cp_of(t[i + 1], int(t[i + 2])), t[t.index("pv") + 1])
        if 1 not in pv: return {"cp": 0, "best": None, "second": None}
        return {"cp": pv[1][0], "best": pv[1][1], "second": pv[2][0] if 2 in pv else None}


def cp_of(kind, val):
    if kind == "cp": return max(-10000, min(10000, val))
    if val == 0: return -10000
    return (10000 - abs(val) * 10) * (1 if val > 0 else -1)


ENGINE_SEM = asyncio.Semaphore(4)
REVIEW_SEM = asyncio.Semaphore(2)


# --------------------------------------------------------------------------- bot opponent
async def pick_bot_move(g):
    d = DIFFICULTIES[g.bot_key]
    legal = list(g.board.legal_moves)
    if random.random() < d.get("blunder", 0):
        return random.choice(legal).uci()
    async with ENGINE_SEM:
        u = UCI()
        try:
            await u.start()
            if "uci_elo" in d:
                await u.setopt("UCI_LimitStrength", "true"); await u.setopt("UCI_Elo", d["uci_elo"])
            else:
                await u.setopt("Skill Level", d["skill"])
            await u.ready()
            await u.send(f"position fen {g.board.fen()}")
            lines = []
            await u.send(f"go movetime {d['movetime']}" if "movetime" in d else f"go depth {d['depth']}")
            s = await u.wait("bestmove", lines)
            mv = s.split()[1]
            return mv if chess.Move.from_uci(mv) in g.board.legal_moves else random.choice(legal).uci()
        except Exception as e:
            log.warning("engine failure, random move: %s", e)
            return random.choice(legal).uci()
        finally:
            await u.stop()


def schedule_bot(g):
    if g.mode == "bot" and g.status == "active" and g.turn() == g.bot_color and not g.bot_busy:
        asyncio.create_task(bot_move(g))


async def bot_move(g):
    g.bot_busy = True
    try:
        await asyncio.sleep(random.uniform(0.4, 1.1))
        if g.status != "active" or g.turn() != g.bot_color: return
        uci = await pick_bot_move(g)
        async with g.lock:
            if g.status != "active" or g.turn() != g.bot_color: return
            g.play(g.bot_color, uci)
        await save(g); await broadcast(g)
    except Exception:
        log.exception("bot move failed")
    finally:
        g.bot_busy = False


# --------------------------------------------------------------------------- game review (Stockfish, on demand)
def wp(cp): return 50 + 50 * (2 / (1 + math.exp(-0.00368208 * cp)) - 1)


def is_sacrifice(board, mv):
    p = board.piece_at(mv.from_square)
    if not p or p.piece_type in (chess.PAWN, chess.KING): return False
    cv = 0
    if board.is_capture(mv):
        cv = 1 if board.is_en_passant(mv) else VAL.get(board.piece_at(mv.to_square).piece_type, 0)
    val = VAL[p.piece_type]
    after = board.copy(); after.push(mv)
    opp = after.turn
    atk = after.attackers(opp, mv.to_square)
    if not atk or val - cv < 2: return False
    min_att = min(VAL.get(after.piece_at(s).piece_type, 100) for s in atk)
    defended = bool(after.attackers(not opp, mv.to_square))
    return min_att < val or not defended


def review_phase(i, board):
    if i < 20: return "opening"
    npm = sum(VAL[t] * len(board.pieces(t, c)) for t in (chess.KNIGHT, chess.BISHOP, chess.ROOK, chess.QUEEN) for c in (True, False))
    return "endgame" if npm <= 14 else "middlegame"


async def run_review(g):
    b = chess.Board(); fens = [b.fen()]; mvs = []
    for m in g.moves[:300]:
        mv = chess.Move.from_uci(m["uci"]); mvs.append(mv); b.push(mv); fens.append(b.fen())
    async with REVIEW_SEM:
        u = UCI()
        evals = []
        try:
            await u.start()
            await u.setopt("MultiPV", 2); await u.setopt("Threads", 1); await u.setopt("Hash", 32)
            await u.ready()
            for fen in fens:
                bb = chess.Board(fen)
                if bb.is_checkmate(): evals.append({"cp": -10000, "best": None, "second": None})
                elif bb.is_stalemate() or bb.is_insufficient_material(): evals.append({"cp": 0, "best": None, "second": None})
                else: evals.append(await u.evaluate(fen, REVIEW_DEPTH))
        finally:
            await u.stop()

    board = chess.Board(); prev_cls = None; out = []
    acc = {"w": [], "b": []}
    cpl = {"w": {"opening": [], "middlegame": [], "endgame": []}, "b": {"opening": [], "middlegame": [], "endgame": []}}
    counts = {"w": {}, "b": {}}
    clamp = lambda x: max(-1000, min(1000, x))
    for i, mv in enumerate(mvs):
        color = "w" if board.turn else "b"
        before, after = evals[i], evals[i + 1]
        best_cp, after_cp = before["cp"], -after["cp"]
        wb, wa = wp(best_cp), wp(after_cp)
        loss = max(0.0, wb - wa)
        cpl_i = max(0, clamp(best_cp) - clamp(after_cp))
        is_best = before["best"] == mv.uci()
        best_san = board.san(chess.Move.from_uci(before["best"])) if before["best"] else None
        phase = review_phase(i, board)
        great = is_best and before["second"] is not None and (wb - wp(before["second"])) >= 12 and wb < 95
        brilliant = (is_best or loss <= 1) and wa >= 45 and wb <= 90 and is_sacrifice(board, mv)
        if brilliant: cls = "brilliant"
        elif i < 12 and loss <= 2.5 and abs(after_cp) <= 80: cls = "book"
        elif great: cls = "great"
        elif is_best: cls = "best"
        elif loss <= 2: cls = "excellent"
        elif loss <= 5: cls = "good"
        elif prev_cls in ("mistake", "blunder") and 8 <= loss < 30: cls = "miss"
        elif loss <= 10: cls = "inaccuracy"
        elif loss <= 20: cls = "mistake"
        else: cls = "blunder"
        acc[color].append(max(0.0, min(100.0, 103.1668 * math.exp(-0.04354 * loss) - 3.1669)))
        cpl[color][phase].append(cpl_i)
        counts[color][cls] = counts[color].get(cls, 0) + 1
        white_cp = clamp(evals[i + 1]["cp"] if board.turn is False else -evals[i + 1]["cp"])  # after move, side to move flipped
        out.append({"ply": i + 1, "color": color, "san": board.san(mv), "uci": mv.uci(), "cls": cls,
                    "loss": round(loss, 1), "best_san": best_san, "best_uci": before["best"], "eval": white_cp})
        board.push(mv); prev_cls = cls

    def elo(vals):
        if not vals: return None
        acpl = sum(vals) / len(vals)
        return int(max(200, min(3100, 3100 * math.exp(-0.019 * acpl))))

    def eval_white(i):
        e = evals[i]["cp"]
        return clamp(e if chess.Board(fens[i]).turn else -e)

    return {
        "game_id": g.id, "fens": fens, "evals": [eval_white(i) for i in range(len(fens))], "moves": out,
        "players": {c: pub(g.players[c]) for c in "wb"},
        "accuracy": {c: round(sum(acc[c]) / len(acc[c]), 1) if acc[c] else None for c in "wb"},
        "performance": {c: {ph: elo(cpl[c][ph]) for ph in cpl[c]} for c in "wb"},
        "counts": counts, "banner": g.banner(), "depth": REVIEW_DEPTH,
    }


REVIEW_LOCKS: dict = {}


# --------------------------------------------------------------------------- telegram bot
def webapp_kb(label="Play Chess ♟️", query=""):
    sep = "&" if "?" in WEBAPP_BASE_URL else "?"
    url = WEBAPP_BASE_URL + (f"{sep}{query}" if query else "")
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, web_app=WebAppInfo(url=url))]])


def invite_kb(gid):
    return InlineKeyboardMarkup([[InlineKeyboardButton("Join Game ♟️", url=f"https://t.me/{BOT_USERNAME}/{MINIAPP_SHORT_NAME}?startapp={gid}")]])


def expired_kb(gid):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Play Again 🔄", callback_data=f"again:{gid}")],
        [InlineKeyboardButton("Play with someone else 👥", switch_inline_query="")]])


async def expire_invite(g):
    g.status = "cancelled"
    await save(g); await broadcast(g)
    if g.inline_message_id and TG:
        try:
            await TG.bot.edit_message_text(text=EXPIRED_TEXT.format(bot=BOT_USERNAME),
                                           inline_message_id=g.inline_message_id, reply_markup=expired_kb(g.id))
        except Exception as e:
            log.warning("editMessageText failed: %s", e)


def tg_user(u): return {"id": u.id, "first_name": u.first_name or "", "last_name": u.last_name or "", "username": u.username or ""}


async def register_invite(gid, host, inline_message_id):
    g = Game(gid, "friend", "blitz", host)
    g.players["w"] = host
    g.inline_message_id = inline_message_id
    GAMES[gid] = g
    await save(g)
    await R.zadd("invites", {gid: time.time() + INVITE_TTL})
    return g


async def on_inline(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.inline_query
    gid = new_id()
    res = InlineQueryResultArticle(
        id=gid, title="Challenge to a chess game", description="Blitz 3|2 · tap to send the invitation",
        input_message_content=InputTextMessageContent(card_text(q.from_user.first_name)),
        reply_markup=invite_kb(gid))
    await q.answer([res], cache_time=0, is_personal=True)


async def on_chosen(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    c = update.chosen_inline_result      # requires /setinlinefeedback = 100% in BotFather
    await register_invite(c.result_id, tg_user(c.from_user), c.inline_message_id)


async def on_again(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    old_id = q.data.split(":", 1)[1]
    try: old = await get_game(old_id)
    except HTTPException: old = None
    if old and old.host_id != q.from_user.id:
        await q.answer("Only the host can restart this invitation.", show_alert=True); return
    gid = new_id()
    await register_invite(gid, tg_user(q.from_user), q.inline_message_id)
    await q.answer()
    await ctx.bot.edit_message_text(text=card_text(q.from_user.first_name), inline_message_id=q.inline_message_id,
                                    reply_markup=invite_kb(gid))


async def cmd_start(update: Update, ctx):
    await update.message.reply_text("Chess, right inside Telegram. Tap below to play.\n\n"
                                    "Challenge a friend in any chat: type @" + BOT_USERNAME + " and add a space.",
                                    reply_markup=webapp_kb())


def mode_cmd(mode, label):
    async def h(update: Update, ctx): await update.message.reply_text(label, reply_markup=webapp_kb(label, f"mode={mode}"))
    return h


async def on_error(update, ctx):
    log.error("bot error", exc_info=ctx.error)
    await notify_admin(f"⚠️ Bot error: {ctx.error!r}")


async def notify_admin(text):
    if TG and ADMIN_CHAT_ID:
        try: await TG.bot.send_message(ADMIN_CHAT_ID, text)
        except Exception: pass


def build_tg():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler(["start", "play"], cmd_start))
    for cmd, mode, label in (("bullet", "bullet", "Bullet 1|0"), ("blitz", "blitz", "Blitz 3|2"), ("rapid", "rapid", "Rapid 10|5"),
                             ("secretqueen", "secret_queen", "Secret Queen 10|5"), ("bot", "bot", "Play Vs Bot 10|5")):
        app.add_handler(CommandHandler(cmd, mode_cmd(mode, label)))
    app.add_handler(InlineQueryHandler(on_inline))
    app.add_handler(ChosenInlineResultHandler(on_chosen))
    app.add_handler(CallbackQueryHandler(on_again, pattern=r"^again:"))
    app.add_error_handler(on_error)
    return app


# --------------------------------------------------------------------------- background loops
async def clock_loop():
    while True:
        await asyncio.sleep(0.4)
        now = time.time()
        for g in list(GAMES.values()):
            try:
                if g.status != "active": continue
                t = g.turn()
                if g.clocks_now()[t] <= 0:
                    async with g.lock:
                        if g.status == "active" and g.clocks_now()[g.turn()] <= 0:
                            g.clocks[g.turn()] = 0; g.timeout(g.turn())
                            await save(g); await broadcast(g)
                    continue
                if g.mode == "friend":
                    for c in g.humans():
                        ts = g.away.get(c)
                        if ts and now - ts > DISCONNECT_LIMIT:
                            g.away.pop(c, None); g.timeout(c)          # abandoned -> loses on time
                            await save(g); await broadcast(g)
                            break
            except Exception:
                log.exception("clock loop")
        # drop finished games from memory after an hour (they stay in Redis)
        for gid, g in list(GAMES.items()):
            if g.status in ("over", "cancelled") and not g.conns and now - g.created > 3600: GAMES.pop(gid, None)


async def invite_sweeper():
    while True:
        await asyncio.sleep(10)
        try:
            for gid in await R.zrangebyscore("invites", 0, time.time()):
                await R.zrem("invites", gid)
                try: g = await get_game(gid)
                except HTTPException: continue
                if g.status == "waiting": await expire_invite(g)
        except Exception:
            log.exception("invite sweeper")


# --------------------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(app):
    global R, TG, BOT_USERNAME
    try:
        R = aioredis.from_url(REDIS_URL, decode_responses=True)
        await R.ping()
    except Exception:
        log.warning("Redis unavailable at %s - using in-memory fallback (dev only)", REDIS_URL)
        R = MemoryRedis()
    if BOT_TOKEN:
        TG = build_tg()
        await TG.initialize(); await TG.start()
        BOT_USERNAME = (await TG.bot.get_me()).username
        await TG.bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Play Chess", web_app=WebAppInfo(url=WEBAPP_BASE_URL)))
        await TG.bot.set_my_commands([BotCommand("bullet", "Bullet 1|0"), BotCommand("blitz", "Blitz 3|2"),
                                      BotCommand("rapid", "Rapid 10|5"), BotCommand("secretqueen", "Secret Queen 10|5"),
                                      BotCommand("bot", "Play Vs Bot 10|5"), BotCommand("play", "Open the chess app")])
        await TG.updater.start_polling(allowed_updates=["message", "inline_query", "chosen_inline_result", "callback_query"],
                                       drop_pending_updates=True)
        await notify_admin(f"♟️ Chess server started as @{BOT_USERNAME}")
    else:
        log.warning("TELEGRAM_BOT_TOKEN not set - bot disabled (DEV_MODE web only)")
    tasks = [asyncio.create_task(clock_loop()), asyncio.create_task(invite_sweeper())]
    yield
    for t in tasks: t.cancel()
    if TG:
        await TG.updater.stop(); await TG.stop(); await TG.shutdown()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
@app.get("/webapp")
async def index():
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


class Auth(BaseModel): init_data: str
class NewBot(Auth):
    difficulty: str = "beginner"; time_control: str = "bot"; color: str = "random"; secret_queen: Optional[str] = None
class NewFriend(Auth): time_control: str = "blitz"
class MoveReq(Auth): move: str
class PickReq(Auth): square: str
class DrawReq(Auth): action: str = "offer"


def player_of(g, user):
    role = g.role_of(user["id"])
    if role == "spectator": raise HTTPException(403, "Spectators cannot do this")
    return role


@app.get("/api/config")
async def config(): return {"bot_username": BOT_USERNAME, "miniapp": MINIAPP_SHORT_NAME, "dev": DEV_MODE}


@app.get("/api/difficulties")
async def difficulties():
    return [{"key": k, "name": d["name"], "elo": d["elo"]} for k, d in DIFFICULTIES.items()]


def validate_pick(color, sq_name):
    try: sq = chess.parse_square(sq_name)
    except ValueError: raise HTTPException(400, "Bad square")
    rank = 1 if color == "w" else 6
    if chess.square_rank(sq) != rank: raise HTTPException(400, "Pick one of your starting pawns")
    return sq


@app.post("/api/new_game/bot")
async def new_bot(req: NewBot):
    user = verify_init_data(req.init_data)
    if req.difficulty not in DIFFICULTIES: raise HTTPException(400, "Unknown difficulty")
    tc = req.time_control if req.time_control in TIME_CONTROLS else "bot"
    g = Game(new_id(), "bot", tc, user)
    human = random.choice("wb") if req.color == "random" else ("w" if req.color == "white" else "b")
    g.bot_key, g.bot_color = req.difficulty, other(human)
    d = DIFFICULTIES[req.difficulty]
    g.players[human] = user
    g.players[g.bot_color] = {"id": -1, "first_name": f"{d['name']}", "username": "stockfish", "bot": True, "elo": d["elo"]}
    if g.variant == "secret_queen" and req.secret_queen:
        g.secret[human] = validate_pick(human, req.secret_queen)
    g.maybe_start()
    GAMES[g.id] = g
    await save(g); schedule_bot(g)
    return {"game_id": g.id, "state": g.view(user["id"])}


@app.post("/api/new_game/friend")
async def new_friend(req: NewFriend):
    user = verify_init_data(req.init_data)
    tc = req.time_control if req.time_control in TIME_CONTROLS and req.time_control != "bot" else "blitz"
    g = Game(new_id(), "friend", tc, user)
    g.players["w"] = user
    GAMES[g.id] = g
    await save(g)
    await R.zadd("invites", {g.id: time.time() + INVITE_TTL})
    return {"game_id": g.id, "state": g.view(user["id"])}


@app.post("/api/game/{gid}/join")
async def join(gid: str, req: Auth):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    async with g.lock:
        if g.role_of(user["id"]) == "spectator":
            if g.status == "cancelled":
                raise HTTPException(410, "This invitation is no longer valid")
            free = [c for c in "bw" if g.players[c] is None]
            if free and g.status == "waiting":
                seat = free[0]
                g.players[seat] = user
                if RANDOM_COLORS and g.mode == "friend" and random.random() < 0.5 and not g.moves:
                    g.players["w"], g.players["b"] = g.players["b"], g.players["w"]
                await R.zrem("invites", gid)
                g.maybe_start()
                await save(g)
                await broadcast(g)
    return {"game_id": gid, "state": g.view(user["id"])}


@app.post("/api/game/{gid}/state")
async def state(gid: str, req: Auth):
    user = verify_init_data(req.init_data)
    return (await get_game(gid)).view(user["id"])


@app.post("/api/game/{gid}/secret_pick")
async def secret_pick(gid: str, req: PickReq):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    async with g.lock:
        role = player_of(g, user)
        if g.variant != "secret_queen": raise HTTPException(400, "Not a Secret Queen game")
        if g.status not in ("waiting", "setup") or g.secret[role] is not None:
            raise HTTPException(400, "Pick already locked in")
        g.secret[role] = validate_pick(role, req.square)
        g.maybe_start()
        await save(g)
    await broadcast(g); schedule_bot(g)
    return g.view(user["id"])


@app.post("/api/game/{gid}/move")
async def move(gid: str, req: MoveReq):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    async with g.lock:
        role = player_of(g, user)
        if g.status != "active": raise HTTPException(400, "Game is not active")
        if g.clocks_now()[g.turn()] <= 0: raise HTTPException(400, "Time is up")
        if g.turn() != role: raise HTTPException(400, "Not your turn")
        try:
            g.play(role, req.move)
        except IllegalMove:
            raise HTTPException(422, {"error": "illegal_move", "in_check": g.board.is_check()})
        await save(g)
    await broadcast(g); schedule_bot(g)
    return g.view(user["id"])


@app.post("/api/game/{gid}/resign")
async def resign(gid: str, req: Auth):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    async with g.lock:
        role = player_of(g, user)
        if g.status != "active": raise HTTPException(400, "Game is not active")
        g.finish(other(role), "Resignation")
        await save(g)
    await broadcast(g)
    return g.view(user["id"])


@app.post("/api/game/{gid}/draw_offer")
async def draw_offer(gid: str, req: DrawReq):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    async with g.lock:
        role = player_of(g, user)
        if g.status != "active": raise HTTPException(400, "Game is not active")
        msg = None
        if req.action == "offer":
            if g.mode == "bot": msg = "The bot declined your draw offer."
            else: g.draw_offer = role
        elif req.action == "accept" and g.draw_offer == other(role):
            g.finish(None, "Agreement")
        elif req.action in ("decline", "cancel"):
            g.draw_offer = None
        await save(g)
    await broadcast(g)
    return {"message": msg, "state": g.view(user["id"])}


@app.post("/api/game/{gid}/cancel")
async def cancel(gid: str, req: Auth):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    if g.host_id != user["id"] or g.status != "waiting": raise HTTPException(400, "Cannot cancel this game")
    await R.zrem("invites", gid)
    await expire_invite(g)
    return {"ok": True}


@app.post("/api/game/{gid}/rematch")
async def rematch(gid: str, req: Auth):
    user = verify_init_data(req.init_data)
    g = await get_game(gid)
    role = player_of(g, user)
    if g.status != "over": raise HTTPException(400, "Game is still running")
    if g.rematch_id:                                    # opponent already asked: just return that game
        return {"game_id": g.rematch_id, "state": (await get_game(g.rematch_id)).view(user["id"])}
    ng = Game(new_id(), g.mode, g.tc_key, user)
    ng.bot_key = g.bot_key
    if g.mode == "bot":
        ng.bot_color = role                             # colours swap: bot takes the side the human just had
        ng.players[role] = g.players[other(role)]
        ng.players[other(role)] = user
        ng.maybe_start()
    else:
        ng.players[other(role)] = user                  # requester takes the colour they did NOT have
    GAMES[ng.id] = ng
    g.rematch_id = ng.id
    await save(ng); await save(g); await broadcast(g)
    if g.mode == "bot": schedule_bot(ng)
    return {"game_id": ng.id, "state": ng.view(user["id"])}


def quota_window():
    """(date string, epoch of next 00:00, seconds until then) in REVIEW_TZ."""
    now = datetime.now(ZoneInfo(REVIEW_TZ))
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return now.strftime("%Y-%m-%d"), int(nxt.timestamp()), int((nxt - now).total_seconds()) + 1


USER_LOCKS: dict = {}


@app.get("/api/game_review/{gid}")
async def game_review(gid: str, x_init_data: str = Header(default="")):
    user = verify_init_data(x_init_data)
    g = await get_game(gid)
    if g.variant == "secret_queen":
        raise HTTPException(400, "Game review is disabled for the Secret Queen variant")
    if g.status != "over": raise HTTPException(400, "Game is not finished")
    day, reset_at, ttl = quota_window()
    qkey = f"review_quota:{user['id']}:{day}"
    async with USER_LOCKS.setdefault(user["id"], asyncio.Lock()):
        used = await R.get(qkey) if REVIEW_DAILY_LIMIT else None
        if used and used != gid:           # one game per user per day; re-opening that same game is free
            raise HTTPException(429, {"error": "review_limit", "reset_at": reset_at, "seconds": reset_at - int(time.time())})
        key = f"game_review:{gid}"
        raw = await R.get(key)
        if not raw:
            lock = REVIEW_LOCKS.setdefault(gid, asyncio.Lock())
            async with lock:
                raw = await R.get(key)
                if not raw:
                    try:
                        payload = await run_review(g)
                    except FileNotFoundError:
                        raise HTTPException(503, "Stockfish binary not found (check STOCKFISH_PATH)")
                    raw = json.dumps(payload)
                    await R.set(key, raw, ex=REVIEW_TTL)
            REVIEW_LOCKS.pop(gid, None)
        if REVIEW_DAILY_LIMIT and not used:
            await R.set(qkey, gid, ex=ttl)
    return json_response(raw)


def json_response(raw):
    from fastapi.responses import Response
    return Response(content=raw, media_type="application/json")


@app.websocket("/ws/{gid}")
async def ws_endpoint(ws: WebSocket, gid: str):
    await ws.accept()
    conn = None; g = None; uid = None
    try:
        msg = await asyncio.wait_for(ws.receive_json(), 10)
        user = verify_init_data(msg.get("init_data", ""))
        uid = user["id"]
        g = await get_game(gid)
        conn = (ws, uid)
        g.conns.add(conn)
        role = g.role_of(uid)
        if role in "wb": g.away.pop(role, None)
        await ws.send_json({"type": "state", "state": g.view(uid)})
        while True:
            await ws.receive_text()                     # keep-alive pings from the client
    except (WebSocketDisconnect, asyncio.TimeoutError, HTTPException):
        pass
    except Exception:
        log.exception("ws")
    finally:
        if g and conn:
            g.conns.discard(conn)
            role = g.role_of(uid)
            if role in ("w", "b") and g.status == "active" and not any(u == uid for _, u in g.conns):
                g.away[role] = time.time()


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
