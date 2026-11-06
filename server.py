"""29 Card Game – WebSocket server (v10 multiplayer).

The server manages rooms, seats, and broadcasts game state between
connected clients.  The game engine itself runs in the browser
(the v10 HTML file).
"""
import asyncio
import json
import os
import random
import secrets
from pathlib import Path

from aiohttp import web

PORT = int(os.environ.get('PORT', 3200))
STATIC_DIR = Path(__file__).parent

# Timing knobs (seconds)
DISCONNECT_ANNOUNCE = 3
WAITING_FREE = 12
BOT_REPLACE_GRACE = 10
EMPTY_GAME_DELETE = 30


# ── Room / game state ──────────────────────────────────────────────

ROOMS = {}          # room_id -> dict   (see _make_room)

def _make_room(room_id, host_seat, host_ws, host_name):
    return {
        'id': room_id,
        'seats': [None, None, None, None],   # None or {'ws', 'name', 'token'}
        'host_seat': host_seat,
        'game_state': None,                   # None = in lobby; JSON string = running
        'game_id': 0,                          # increments on every deal
    }


def _room_info(room, ws):
    """Return a serialisable snapshot of the room."""
    seats = []
    for i, s in enumerate(room['seats']):
        if s is None:
            seats.append(None)
        else:
            # A seated player is "connected" if their ws is not None
            seated = s.get('ws') is not None
            seats.append({
                'seat': i,
                'name': s['name'],
                'connected': seated,
                'av': s.get('avatar', 0),
            })
    return {
        'roomId': room['id'],
        'hostSeat': room['host_seat'],
        'players': seats,
        'gameState': room['game_state'],
        'gameId': room['game_id'],
    }


async def _send(ws, msg):
    if ws is None:
        return
    try:
        await ws.send_str(json.dumps(msg))
    except Exception:
        pass


async def _broadcast(room, msg, except_ws=None):
    data = json.dumps(msg)
    for s in room['seats']:
        if s is None:
            continue
        target = s['ws']
        if target == except_ws or target is None:
            continue
        try:
            await target.send_str(data)
        except Exception:
            pass


# ── Disconnect / seat management ───────────────────────────────────

async def _monitor_disconnect(room, seat):
    await asyncio.sleep(DISCONNECT_ANNOUNCE)
    if ROOMS.get(room['id']) is not room:
        return
    s = room['seats'][seat]
    if s is None or s.get('left') or s['connected'] or s['ws'] is not None:
        return
    # Player still disconnected after delay
    await _broadcast(room, {
        'type': 'player-status',
        'seat': seat,
        'connected': False,
    })

    if room['game_state'] is not None:
        # Game running: wait a grace period for a resume before offering a bot.
        await asyncio.sleep(BOT_REPLACE_GRACE)
        if ROOMS.get(room['id']) is not room:
            return
        s = room['seats'][seat]
        if s is None or s.get('left') or s['connected'] or s['ws'] is not None:
            return
        if s.get('notified'):
            return
        s['notified'] = True
        if room['host_seat'] == seat:
            await _promote_host(room, seat, 'disconnected')
        else:
            await _broadcast(room, {
                'type': 'player-left',
                'seat': seat,
                'name': s['name'],
                'reason': 'disconnected',
                'wasHost': False,
            })
        return

    # Waiting in lobby: free the seat
    await asyncio.sleep(WAITING_FREE)
    if ROOMS.get(room['id']) is not room:
        return
    s = room['seats'][seat]
    if s is None or s.get('left') or s['connected'] or s['ws'] is not None:
        return
    room['seats'][seat] = None

    if not any(room['seats']):
        ROOMS.pop(room['id'], None)
        return

    host = room['host_seat']
    if room['seats'][host] is None:
        room['host_seat'] = next(i for i in range(4) if room['seats'][i] is not None)

    await _broadcast(room, {
        'type': 'room-updated',
        **_room_info(room, None),
    })


async def _promote_host(room, old_seat, reason):
    """Promote the next connected human to host after the host leaves."""
    if ROOMS.get(room['id']) is not room:
        return
    new_seat = None
    for i in range(4):
        if i == old_seat:
            continue
        s = room['seats'][i]
        if s and not s.get('left') and s.get('ws') is not None and s.get('connected'):
            new_seat = i
            break
    if new_seat is None:
        ROOMS.pop(room['id'], None)
        return
    old = room['seats'][old_seat]
    old_name = old['name'] if old else None
    if old:
        old['left'] = True
        old['ws'] = None
        old['connected'] = False
        old['token'] = secrets.token_hex(4)
    room['host_seat'] = new_seat
    await _broadcast(room, {
        'type': 'host-changed',
        'leftSeat': old_seat,
        'leftName': old_name,
        'leftReason': reason,
        'state': room['game_state'],
        **_room_info(room, None),
    })


async def _handle_disconnect(room, seat, ws):
    s = room['seats'][seat]
    if s is None or s['ws'] is not ws:
        return
    s['ws'] = None
    s['connected'] = False
    if s.get('left'):
        return
    asyncio.create_task(_monitor_disconnect(room, seat))


def _clean_name(raw):
    return str(raw or '').strip()[:16] or None


# ── WebSocket handler ──────────────────────────────────────────────

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session = {'room_id': None, 'seat': None, 'name': 'Player'}
    room = None
    seat = None

    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            mt = data.get('type')
            room_id = session.get('room_id')
            room = ROOMS.get(room_id) if room_id else None
            seat = session.get('seat')

            # ── Create room ───────────────────────────────────────
            if mt == 'create-room':
                if room_id and room:
                    # Leave current room first
                    s = room['seats'][seat] if isinstance(seat, int) and 0 <= seat < len(room['seats']) else None
                    if s:
                        await _handle_disconnect(room, seat, ws)
                name = _clean_name(data.get('playerName')) or 'Player'
                rid = ''.join(random.choices('ABCDEF0123456789', k=6)).upper()
                while rid in ROOMS:
                    rid = ''.join(random.choices('ABCDEF0123456789', k=6)).upper()
                new_room = _make_room(rid, 0, ws, name)
                new_room['seats'][0] = {
                    'ws': ws, 'name': name,
                    'token': secrets.token_hex(4), 'connected': True,
                }
                ROOMS[rid] = new_room
                session['room_id'] = rid
                session['seat'] = 0
                session['name'] = name
                await _send(ws, {
                    'type': 'room-created',
                    'yourSeat': 0,
                    'token': new_room['seats'][0]['token'],
                    **_room_info(new_room, ws),
                })

            # ── Join room ─────────────────────────────────────────
            elif mt == 'join-room':
                wanted = str(data.get('roomId', '')).strip().upper()
                target = ROOMS.get(wanted)
                if target is None:
                    # Room doesn't exist — create it (first person is host)
                    name = _clean_name(data.get('playerName')) or 'Player'
                    new_room = _make_room(wanted, 0, ws, name)
                    new_room['seats'][0] = {
                        'ws': ws, 'name': name,
                        'token': secrets.token_hex(4), 'connected': True,
                    }
                    ROOMS[wanted] = new_room
                    session['room_id'] = wanted
                    session['seat'] = 0
                    session['name'] = name
                    await _send(ws, {
                        'type': 'room-joined',
                        'yourSeat': 0,
                        'token': new_room['seats'][0]['token'],
                        **_room_info(new_room, ws),
                    })
                else:
                    if room_id and room_id != wanted and room:
                        await _handle_disconnect(room, seat, ws)
                    name = _clean_name(data.get('playerName')) or 'Player'
                    # Respect team preference (0 = Team A [seats 0,2], 1 = Team B [seats 1,3])
                    team = data.get('team')
                    free_seat = None
                    if isinstance(team, int) and not isinstance(team, bool):
                        # Try preferred team seats first
                        team_seats = [0, 2] if team == 0 else [1, 3]
                        for i in team_seats:
                            if target['seats'][i] is None:
                                free_seat = i
                                break
                    # Fall back to any empty seat
                    if free_seat is None:
                        for i in range(4):
                            if target['seats'][i] is None:
                                free_seat = i
                                break
                    if free_seat is None:
                        await _send(ws, {'type': 'room-error', 'message': 'Room is full'})
                        continue
                    target['seats'][free_seat] = {
                        'ws': ws, 'name': name,
                        'token': secrets.token_hex(4), 'connected': True,
                    }
                    session['room_id'] = wanted
                    session['seat'] = free_seat
                    session['name'] = name
                    await _send(ws, {
                        'type': 'room-joined',
                        'yourSeat': free_seat,
                        'token': target['seats'][free_seat]['token'],
                        **_room_info(target, ws),
                    })
                    await _broadcast(target, {
                        'type': 'room-updated',
                        **_room_info(target, ws),
                    })

            # ── Resume session ────────────────────────────────────
            elif mt == 'resume':
                wanted = str(data.get('roomId', ''))
                want_seat = data.get('seat')
                token = str(data.get('token') or '')
                target = ROOMS.get(wanted)
                player = None
                if (target is not None
                        and isinstance(want_seat, int)
                        and not isinstance(want_seat, bool)
                        and 0 <= want_seat < len(target['seats'])
                        and target['seats'][want_seat] is not None):
                    player = target['seats'][want_seat]
                if player is None or not secrets.compare_digest(player['token'], token):
                    await _send(ws, {
                        'type': 'resume-failed',
                        'message': 'Room no longer exists or your session expired',
                    })
                    continue
                if room_id and room_id != wanted and room:
                    await _handle_disconnect(room, seat, ws)
                was_connected = player['ws'] is ws and player['connected']
                player['ws'] = ws
                player['connected'] = True
                player['notified'] = False
                session['room_id'] = wanted
                session['seat'] = want_seat
                session['name'] = player['name']
                # Send current room state + any running game state
                await _send(ws, {
                    'type': 'room-joined',
                    'yourSeat': want_seat,
                    'token': player['token'],
                    **_room_info(target, ws),
                })
                if not was_connected:
                    await _broadcast(target, {
                        'type': 'player-status',
                        'seat': want_seat,
                        'connected': True,
                    })

            # ── Host starts game ──────────────────────────────────
            elif mt == 'start-game':
                if room is None or seat is None:
                    continue
                if room['host_seat'] != seat:
                    continue
                if room['game_state'] is not None:
                    await _send(ws, {'type': 'room-error', 'message': 'A round is already in progress'})
                    continue
                if sum(1 for s in room['seats'] if s is not None) < 2:
                    await _send(ws, {'type': 'room-error', 'message': 'At least 2 players are required to start'})
                    continue
                room['game_state'] = ''       # empty string = v10 will initialise
                room['game_id'] += 1
                # Broadcast game-started to everyone (including host)
                msg = {'type': 'game-started', **_room_info(room, ws)}
                await _broadcast(room, msg)

            # ── Game state sync (v10) ─────────────────────────────
            # In v10 multiplayer, the host is the source of truth.
            # The host pushes game state changes; server broadcasts them.
            elif mt == 'game-state':
                if room is None or seat is None:
                    continue
                player = room['seats'][seat]
                if player is None:
                    continue
                # Only accept game state from the host
                if room['host_seat'] != seat:
                    continue
                if room['game_state'] is None:
                    await _send(ws, {'type': 'room-error', 'message': 'No game in progress'})
                    continue
                # Broadcast the full game state to everyone
                state_json = data.get('state', '')
                room['game_state'] = state_json
                room['game_id'] += 1
                print(f'[server] game-state-sync: broadcasting to room {room["id"]}')
                await _broadcast(room, {
                    'type': 'game-state-sync',
                    'state': state_json,
                    **_room_info(room, ws),
                })

            # ── Client asks for the latest state (self-heal) ──────
            elif mt == 'resync':
                if room is None or seat is None:
                    continue
                if room['game_state']:
                    await _send(ws, {
                        'type': 'game-state-sync',
                        'state': room['game_state'],
                        **_room_info(room, ws),
                    })

            # ── Leave game (any seated player) ────────────────────
            elif mt == 'leave-game':
                if room is None or seat is None:
                    continue
                s = room['seats'][seat]
                if s is None:
                    continue
                if room['game_state'] is not None:
                    if room['host_seat'] == seat:
                        await _promote_host(room, seat, 'left')
                    else:
                        s['left'] = True
                        s['ws'] = None
                        s['connected'] = False
                        s['token'] = secrets.token_hex(4)
                        await _broadcast(room, {
                            'type': 'player-left',
                            'seat': seat,
                            'name': s['name'],
                            'reason': 'left',
                            'wasHost': False,
                        })
                else:
                    room['seats'][seat] = None
                    host = room['host_seat']
                    if room['seats'][host] is None:
                        nxt = next((i for i in range(4) if room['seats'][i] is not None), None)
                        if nxt is None:
                            ROOMS.pop(room['id'], None)
                            continue
                        room['host_seat'] = nxt
                    await _broadcast(room, {'type': 'room-updated', **_room_info(room, None)})
                session['room_id'] = None
                session['seat'] = None

            # ── Replace a left seat with a bot (host only) ────────
            elif mt == 'bot-seat':
                if room is None or seat is None:
                    continue
                if room['host_seat'] != seat:
                    continue
                want = data.get('seat')
                if not isinstance(want, int) or isinstance(want, bool) or not (0 <= want < len(room['seats'])):
                    continue
                if want == room['host_seat']:
                    continue
                s = room['seats'][want]
                if s is None:
                    continue
                if s.get('ws') is not None and s.get('connected'):
                    continue
                s['left'] = True
                s['bot'] = True
                s['ws'] = None
                s['connected'] = False
                s['token'] = secrets.token_hex(4)
                await _broadcast(room, {'type': 'seat-bot', 'seat': want, 'name': s.get('name')})

            # ── Cancel game (host only) ───────────────────────────
            elif mt == 'cancel-game':
                if room is None or seat is None:
                    continue
                if room['host_seat'] != seat:
                    continue
                if room['game_state'] is None:
                    continue
                room['game_state'] = None
                room['game_id'] += 1
                await _broadcast(room, {
                    'type': 'game-cancelled',
                    'message': 'The Host has cancelled the game.',
                    **_room_info(room, ws),
                })
                ROOMS.pop(room['id'], None)

            # ── Action relay (v10) ────────────────────────────────
            # Non-host players send game actions here.
            # Server forwards them to the host, who processes them.
            elif mt == 'action':
                if room is None or seat is None:
                    continue
                player = room['seats'][seat]
                if player is None:
                    continue
                # Forward to host's connection
                # game_state '' means game started but no state synced yet
                host_ws = room['seats'][room['host_seat']].get('ws') if room.get('game_state') is not None else None
                if host_ws:
                    print(f'[server] relay action from seat {seat} to host')
                    await _send(host_ws, {
                        'type': 'relay-action',
                        'from': seat,
                        # client sends {"type":"action","a":"<json>"}; accept both keys
                        'action': data.get('a', '') or data.get('action', ''),
                    })
                else:
                    print(f'[server] relay FAILED: host_ws={host_ws}, game_state={room.get("game_state")!r}')

            # ── Quick chat (any seated player) ────────────────────
            elif mt == 'chat':
                if room is None or seat is None:
                    continue
                s = room['seats'][seat]
                if s is None:
                    continue
                text = str(data.get('message', '')).strip()[:200]
                if not text:
                    continue
                await _broadcast(room, {
                    'type': 'chat-message',
                    'seat': seat,
                    'name': s['name'],
                    'message': text,
                })

            # ── Reactions (any seated player) ─────────────────────
            elif mt == 'react':
                if room is None or seat is None:
                    continue
                if room['seats'][seat] is None:
                    continue
                e = str(data.get('e', ''))
                if e not in ('👍', '👎', '👏', '🙌', '🤝', '💪', '🙏', '\u270c\ufe0f', '👌', '🔥', '⭐', '💥', '🎉', '😀', '😂', '😮', '😢', '😡', '🤔', '🥳'):
                    continue
                await _broadcast(room, {'type': 'reaction', 'seat': seat, 'e': e})

            # ── Sit in lobby (v10 style) ──────────────────────────
            elif mt == 'sit':
                if room is None or seat is None:
                    continue
                if room['game_state'] is not None:
                    await _send(ws, {'type': 'room-error', 'message': 'Seats are locked once the game has started'})
                    continue
                target_seat = data.get('seat', seat)
                if not isinstance(target_seat, int) or isinstance(target_seat, bool) or target_seat not in range(4):
                    continue
                s = room['seats'][target_seat]
                if s is not None and s['ws'] is not ws:
                    await _send(ws, {'type': 'room-error', 'message': 'Seat taken'})
                    continue
                # If this player already has a seat, free it
                if room['seats'][seat] is not None and seat != target_seat:
                    room['seats'][seat] = None
                # If seat was empty, assign it
                if s is None:
                    room['seats'][target_seat] = {
                        'ws': ws, 'name': session['name'],
                        'token': secrets.token_hex(4), 'connected': True,
                    }
                # Update session to the new seat
                session['seat'] = target_seat
                # Keep the host's seat pointer valid if the host moves
                if room['host_seat'] == seat:
                    room['host_seat'] = target_seat
                await _send(ws, {
                    'type': 'room-updated',
                    'yourSeat': target_seat,
                    **_room_info(room, ws),
                })
                await _broadcast(room, {
                    'type': 'room-updated',
                    **_room_info(room, ws),
                })

            # ── Avatar change ─────────────────────────────────────
            elif mt == 'set-avatar':
                if room is None or seat is None:
                    continue
                s = room['seats'][seat]
                if s is None:
                    continue
                s['avatar'] = data.get('avatar', 0)
                await _broadcast(room, {
                    'type': 'room-updated',
                    **_room_info(room, ws),
                })

    except Exception as exc:
        print(f'websocket error: {exc!r}')
    finally:
        # Use the session, not the loop-local room/seat: after a join-room the
        # locals still point at the pre-join state (None), which would make the
        # server miss disconnects entirely.
        rid = session.get('room_id')
        sseat = session.get('seat')
        sroom = ROOMS.get(rid) if rid else None
        if sroom is not None and sseat is not None:
            await _handle_disconnect(sroom, sseat, ws)
        session['room_id'] = None
        session['seat'] = None

    return ws


# ── Middleware: no-cache for HTML ──────────────────────────────────

async def no_cache_middleware(app, handler):
    """Prevent browser caching so back-button always loads fresh page."""
    async def middleware(request):
        response = await handler(request)
        if hasattr(response, 'headers') and 'text/html' in (response.content_type or ''):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        return response
    return middleware


# ── Static files ───────────────────────────────────────────────────

async def index(request):
    resp = web.FileResponse(STATIC_DIR / 'index.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


async def game29(request):
    resp = web.FileResponse(STATIC_DIR / '29_Card_Game_v10.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


async def gameLudo(request):
    resp = web.FileResponse(STATIC_DIR / 'ludo.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


def create_app():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/', index)
    app.router.add_get('/29', game29)
    app.router.add_get('/play', game29)
    app.router.add_get('/ludo', gameLudo)
    app.router.add_static('/', STATIC_DIR, name='static')
    app.middlewares.append(no_cache_middleware)
    return app


async def main():
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f'29 Card Game server running at http://localhost:{PORT}')
    print('Share this URL with friends on the same network to play!')
    print('Find your local IP with: hostname -I')
    await asyncio.Event().wait()


if __name__ == '__main__':
    asyncio.run(main())
