# Telegram Chess Mini App (with Secret Queen variant)

Single process: FastAPI + python-telegram-bot 21.4 + Redis. Full gameplay, clocks, bot opponents,
spectators, inline challenges and on-demand Stockfish review all happen inside the Mini App.

## Setup
1. `pip install -r requirements.txt` and install Stockfish (`apt install stockfish`).
2. Copy your 13 image files into `static/images/` (see `static/images/README.txt`).
3. `cp .env.example .env`, fill it in, then `export $(grep -v '^#' .env | xargs)`.
4. In **BotFather**:
   - `/newapp` -> create a Mini App with short name `game` (matches `MINIAPP_SHORT_NAME`), URL = `WEBAPP_BASE_URL`.
   - `/setinline` -> enable inline mode (placeholder e.g. "Challenge a friend").
   - `/setinlinefeedback` -> **100%**. Required: the invitation-expiry edit needs `inline_message_id`
     which Telegram only delivers through chosen_inline_result.
5. `WEBAPP_BASE_URL` must be public HTTPS (e.g. behind Caddy/nginx/ngrok) with WebSocket upgrade enabled.
6. `python webapp_server.py`

Local browser testing without Telegram: `DEV_MODE=1 python webapp_server.py`, open two different
browsers/profiles at http://localhost:8000/webapp (each gets a guest identity).

## Where each spec section lives
| Spec | Location |
|---|---|
| Assets, board texture | `static/index.html` (`pimg`, `.board` CSS) |
| Haptics / check feedback / audio | `hap`, `snd`, `illegalFeedback()` |
| Secret Queen rules | `Game.play()`, `Game.legal_ucis()`, `Game.view()` (owner-only marker) |
| Terminations (8 rules) | `Game.evaluate_end()`, `Game.timeout()`, resign/draw endpoints |
| Invitations + expiry | `on_inline`, `on_chosen`, `expire_invite`, `invite_sweeper` (10 min) |
| Review (async subprocess, Redis 1-day cache) | `UCI`, `run_review`, `GET /api/game_review/{id}` |

## Design notes worth knowing
- **Server-authoritative**: moves, clocks and terminations are decided on the server; the client only renders.
- **Secret info never leaves the server** for non-owners. The unrevealed secret queen is a plain pawn in the FEN
  sent to everyone else; the owner additionally gets `secret.own` and queen destinations in `legal`.
  On reveal the server swaps the pawn for a real queen, so both clients simply render `wq/bq.png`.
- **Checkmate/stalemate** take the hidden queen into account (a side with a legal secret-queen move is not mated).
- **Colour assignment**: section 13 says host = White, first joiner = Black; the challenge card text says
  "Random color". Default follows section 13; set `RANDOM_COLORS=1` to randomise on join.
- **Bot strength**: Stockfish's `UCI_Elo` floor is 1320, so Kid/Beginner/Intermediate use Skill Level, shallow
  depth and a random-move probability instead. Advanced/Magnus use `UCI_LimitStrength` + `UCI_Elo`.
- **Review classification** is a heuristic on win-probability loss (Chess.com does not publish its algorithm).
  "Book" is approximated as low-loss opening moves (no ECO database is bundled); "Brilliant" = sound sacrifice.
- Games persist in Redis for 7 days; finished games leave server memory after an hour and reload on demand.
- A friend who stays disconnected for 60 s during a live game loses on time (`DISCONNECT_LIMIT`).
