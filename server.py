"""29 Card Game – WebSocket server (v10 multiplayer).

The server manages rooms, seats, and broadcasts game state between
connected clients.  The game engine itself runs in the browser
(the v10 HTML file).
"""
import asyncio
import json
import os
import random
import re
import secrets
import time
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

ROOMS = {}          # 29-card rooms
RUMMY_ROOMS = {}    # Rummy rooms are isolated from 29-card rooms

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



# ── Rummy multiplayer rooms (separate from 29-card rooms) ──────────
def _rummy_info(room):
    return {
        'roomId': room['id'], 'mode': room['mode'],
        'maxPlayers': room['max_players'], 'hostSeat': room['host_seat'],
        'seatMap': room.get('seat_map'),
        'players': [({'seat': i, 'name': p['name'], 'connected': p['ws'] is not None}
                     if p else None) for i, p in enumerate(room['seats'])],
        'started': room['state'] is not None,
    }

async def _rummy_send(ws, payload):
    if ws is not None:
        try: await ws.send_str(json.dumps(payload))
        except Exception: pass

async def _rummy_broadcast(room, payload, exclude=None):
    raw = json.dumps(payload)
    for player in room['seats']:
        if player and player['ws'] is not None and player['ws'] is not exclude:
            try: await player['ws'].send_str(raw)
            except Exception: pass

async def _rummy_sync(room):
    if room['state'] is None: return
    try: state = json.loads(room['state'])
    except (TypeError, json.JSONDecodeError): return
    for seat, player in enumerate(room['seats']):
        if not player or player['ws'] is None: continue
        view = json.loads(json.dumps(state))
        # Only the host receives the authoritative full state. Other clients
        # receive their own hand; every other hand is replaced by card-count placeholders.
        if seat != room['host_seat']:
            seat_map = room.get('seat_map') or list(range(len(view.get('hands', []))))
            own_index = seat_map.index(seat) if seat in seat_map else -1
            for i, hand in enumerate(view.get('hands', [])):
                if i != own_index:
                    view['hands'][i] = [None] * len(hand)
        await _rummy_send(player['ws'], {
            'type': 'rummy-state', 'state': view,
            **_rummy_info(room), 'yourSeat': seat,
        })

async def rummy_websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    session = {'room': None, 'seat': None}
    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT: continue
            try: data = json.loads(msg.data)
            except (ValueError, TypeError): continue
            kind = data.get('type')
            rid, seat = session['room'], session['seat']
            room = RUMMY_ROOMS.get(rid) if rid else None
            if kind == 'rummy-create':
                name = _clean_name(data.get('name')) or 'Player'
                mode = 'std' if data.get('mode') == 'std' else 'in'
                cap = 4 if mode == 'std' else 6
                rid = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=6))
                while rid in RUMMY_ROOMS: rid = ''.join(random.choices('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', k=6))
                room = {'id': rid, 'mode': mode, 'max_players': cap, 'host_seat': 0,
                        'seats': [None] * cap, 'state': None, 'tokens': {}}
                token = secrets.token_urlsafe(18)
                room['seats'][0] = {'name': name, 'ws': ws, 'token': token}
                RUMMY_ROOMS[rid] = room; session.update(room=rid, seat=0)
                await _rummy_send(ws, {'type':'rummy-room', 'token':token, 'yourSeat':0, **_rummy_info(room)})
            elif kind == 'rummy-join':
                rid = str(data.get('roomId','')).strip().upper(); room = RUMMY_ROOMS.get(rid)
                if not room:
                    await _rummy_send(ws, {'type':'rummy-error','message':'Room not found'}); continue
                if room['state'] is not None:
                    await _rummy_send(ws, {'type':'rummy-error','message':'This game has already started'}); continue
                if data.get('mode') and data.get('mode') != room['mode']:
                    await _rummy_send(ws, {'type':'rummy-error','message':'This room uses a different Rummy mode'}); continue
                seat = next((i for i,p in enumerate(room['seats']) if p is None), None)
                if seat is None:
                    await _rummy_send(ws, {'type':'rummy-error','message':'Room is full'}); continue
                name = _clean_name(data.get('name')) or 'Player'; token = secrets.token_urlsafe(18)
                room['seats'][seat] = {'name':name,'ws':ws,'token':token}
                session.update(room=rid,seat=seat)
                await _rummy_send(ws, {'type':'rummy-room','token':token,'yourSeat':seat,**_rummy_info(room)})
                await _rummy_broadcast(room, {'type':'rummy-lobby',**_rummy_info(room)})
            elif kind == 'rummy-resume':
                rid = str(data.get('roomId','')).upper(); room = RUMMY_ROOMS.get(rid)
                seat = data.get('seat'); token = str(data.get('token',''))
                if not room or not isinstance(seat,int) or isinstance(seat,bool) or seat not in range(len(room['seats'])) or not room['seats'][seat] or not secrets.compare_digest(room['seats'][seat]['token'],token):
                    await _rummy_send(ws, {'type':'rummy-error','message':'Your room session has expired'}); continue
                room['seats'][seat]['ws']=ws; session.update(room=rid,seat=seat)
                await _rummy_send(ws, {'type':'rummy-room','token':token,'yourSeat':seat,**_rummy_info(room)})
                await _rummy_sync(room)
                await _rummy_broadcast(room, {'type':'rummy-lobby',**_rummy_info(room)}, exclude=ws)
            elif kind == 'rummy-start':
                if not room or seat != room['host_seat']: continue
                count = sum(p is not None for p in room['seats'])
                if count < 2:
                    await _rummy_send(ws, {'type':'rummy-error','message':'At least two players are required'}); continue
                room['state'] = ''
                room['seat_map'] = [i for i, player in enumerate(room['seats']) if player is not None]
                await _rummy_broadcast(room, {'type':'rummy-started',**_rummy_info(room)})
            elif kind == 'rummy-state':
                if not room or seat != room['host_seat'] or room['state'] is None: continue
                raw = data.get('state')
                if not isinstance(raw,str) or len(raw)>200000: continue
                try:
                    parsed=json.loads(raw)
                    if not isinstance(parsed,dict) or not isinstance(parsed.get('hands'),list): continue
                except (ValueError,TypeError): continue
                room['state']=raw
                await _rummy_sync(room)
            elif kind == 'rummy-action':
                if not room or room['state'] is None: continue
                host=room['seats'][room['host_seat']]
                if host and host['ws']:
                    await _rummy_send(host['ws'], {'type':'rummy-relay','from':seat,'action':data.get('action',{})})
            elif kind == 'rummy-leave':
                if not room or seat is None: continue
                player=room['seats'][seat]
                if player:
                    player['ws']=None
                    if room['state'] is None:
                        room['seats'][seat]=None
                        if seat==room['host_seat']:
                            nxt=next((i for i,p in enumerate(room['seats']) if p),None)
                            if nxt is None: RUMMY_ROOMS.pop(room['id'],None)
                            else: room['host_seat']=nxt
                        await _rummy_broadcast(room, {'type':'rummy-lobby',**_rummy_info(room)})
                    else:
                        player['left']=True
                        host = room['seats'][room['host_seat']]
                        if seat == room['host_seat']:
                            successor = next((i for i, other in enumerate(room['seats'])
                                              if i != seat and other and other['ws'] is not None), None)
                            if successor is not None:
                                room['host_seat'] = successor
                        elif host and host['ws'] is not None:
                            await _rummy_send(host['ws'], {'type':'rummy-player-left','seat':seat,'name':player['name']})
                        await _rummy_broadcast(room, {'type':'rummy-lobby',**_rummy_info(room)})
                        await _rummy_sync(room)
                session.update(room=None,seat=None)
    except Exception as exc:
        print(f'[rummy] websocket error: {exc!r}')
    finally:
        rid, seat = session.get('room'), session.get('seat')
        room = RUMMY_ROOMS.get(rid) if rid else None
        if room and seat is not None and room['seats'][seat] and room['seats'][seat]['ws'] is ws:
            room['seats'][seat]['ws'] = None
            # If the host disappears, hand authority to the next connected
            # player. The server still holds the full state and can safely
            # send it only to the newly elected host.
            if seat == room['host_seat']:
                successor = next((i for i, player in enumerate(room['seats'])
                                  if i != seat and player and player['ws'] is not None), None)
                if successor is not None:
                    room['host_seat'] = successor
            await _rummy_broadcast(room, {'type':'rummy-lobby',**_rummy_info(room)})
            await _rummy_sync(room)
    return ws




# ── Chess multiplayer (authoritative server) ──────────────────────
# The browser client replays the move list from the start position on
# every sync, so the server only has to validate moves, run the clocks
# and rebroadcast.  Every outbound message is sync-shaped — the chess
# client has no message-type dispatcher.

CHESS_ROOMS = {}       # room code -> room dict


def _ch_init():
    cells = []
    for row in 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR'.split('/'):
        for ch in row:
            cells.extend([' '] * int(ch) if ch.isdigit() else [ch])
    return {'b': cells, 't': 'w', 'c': 'KQkq', 'ep': -1, 'hm': 0}


def _ch_key(s):
    return ''.join(s['b']) + s['t'] + s['c'] + str(s['ep'])


_CH_KN = [(1, 2), (2, 1), (-1, 2), (-2, 1), (1, -2), (2, -1), (-1, -2), (-2, -1)]
_CH_D = [(1, 0), (-1, 0), (0, 1), (0, -1), (1, 1), (1, -1), (-1, 1), (-1, -1)]


def _ch_col(p):
    if p == ' ':
        return None
    return 'w' if p == p.upper() else 'b'


def _ch_ok(r, f):
    return 0 <= r < 8 and 0 <= f < 8


def _ch_att(b, s, by):
    r, f = s >> 3, s & 7
    w = by == 'w'
    pr = r + 1 if w else r - 1
    for d in (-1, 1):
        if _ch_ok(pr, f + d) and b[pr * 8 + f + d] == ('P' if w else 'p'):
            return 1
    for a, c in _CH_KN:
        if _ch_ok(r + a, f + c) and b[(r + a) * 8 + f + c] == ('N' if w else 'n'):
            return 1
    for a, c in _CH_D:
        y, x, k = r + a, f + c, 1
        while _ch_ok(y, x):
            p = b[y * 8 + x]
            if p != ' ':
                if _ch_col(p) != by:
                    break
                u = p.upper()
                if (u == 'Q' or (u == 'R' and (a == 0 or c == 0))
                        or (u == 'B' and a and c) or (u == 'K' and k == 1)):
                    return 1
                break
            y += a
            x += c
            k += 1
    return 0


def _ch_apply(s, m):
    b = list(s['b'])
    p = b[m['f']]
    u = p.upper()
    w = s['t'] == 'w'
    cap = b[m['t']] != ' ' or bool(m.get('ep'))
    if m.get('p'):
        b[m['t']] = m['p'] if w else m['p'].lower()
    else:
        b[m['t']] = p
    b[m['f']] = ' '
    if m.get('ep'):
        b[m['t'] + (8 if w else -8)] = ' '
    if m.get('cs'):
        h = m['t'] & 56
        if m['cs'] > 0:
            b[h + 5] = b[h + 7]
            b[h + 7] = ' '
        else:
            b[h + 3] = b[h]
            b[h] = ' '
    c = s['c']
    if u == 'K':
        c = c.replace('K' if w else 'k', '').replace('Q' if w else 'q', '')
    for q in (m['f'], m['t']):
        if q == 63:
            c = c.replace('K', '')
        elif q == 56:
            c = c.replace('Q', '')
        elif q == 7:
            c = c.replace('k', '')
        elif q == 0:
            c = c.replace('q', '')
    return {'b': b, 't': 'b' if w else 'w', 'c': c,
            'ep': (m['f'] + m['t']) // 2 if m.get('dp') else -1,
            'hm': 0 if (u == 'P' or cap) else s['hm'] + 1}


def _ch_gen(s):
    o = []
    b = s['b']
    me = s['t']
    en = 'b' if me == 'w' else 'w'
    w = me == 'w'
    for i in range(64):
        p = b[i]
        if _ch_col(p) != me:
            continue
        u = p.upper()
        r, f = divmod(i, 8)

        def add(t, _i=i, **kw):
            o.append(dict(f=_i, t=t, **kw))

        if u == 'P':
            d = -1 if w else 1
            pm = r + d == (0 if w else 7)

            def ps(t, _pm=pm):
                if _pm:
                    for q in 'QRBN':
                        add(t, p=q)
                else:
                    add(t)

            if b[i + d * 8] == ' ':
                ps(i + d * 8)
                if r == (6 if w else 1) and b[i + d * 16] == ' ':
                    add(i + d * 16, dp=1)
            for df in (-1, 1):
                if not _ch_ok(r + d, f + df):
                    continue
                t = i + d * 8 + df
                if _ch_col(b[t]) == en:
                    ps(t)
                elif t == s['ep']:
                    add(t, ep=1)
        elif u in ('N', 'K'):
            for a, c in (_CH_KN if u == 'N' else _CH_D):
                if _ch_ok(r + a, f + c) and _ch_col(b[(r + a) * 8 + f + c]) != me:
                    add((r + a) * 8 + f + c)
            if u == 'K' and not _ch_att(b, i, en):
                h = 56 if w else 0
                R = 'R' if w else 'r'
                if ((w and 'K' in s['c']) or (not w and 'k' in s['c'])) and \
                   b[h + 5] == ' ' and b[h + 6] == ' ' and b[h + 7] == R and \
                   not _ch_att(b, h + 5, en) and not _ch_att(b, h + 6, en):
                    add(h + 6, cs=1)
                if ((w and 'Q' in s['c']) or (not w and 'q' in s['c'])) and \
                   b[h + 1] == ' ' and b[h + 2] == ' ' and b[h + 3] == ' ' and \
                   b[h] == R and not _ch_att(b, h + 3, en) and not _ch_att(b, h + 2, en):
                    add(h + 2, cs=-1)
        else:
            for a, c in _CH_D:
                if (u == 'R' and a and c) or (u == 'B' and not (a and c)):
                    continue
                y, x = r + a, f + c
                while _ch_ok(y, x):
                    t = y * 8 + x
                    if _ch_col(b[t]) == me:
                        break
                    add(t)
                    if b[t] != ' ':
                        break
                    y += a
                    x += c
    out = []
    for m in o:
        n = _ch_apply(s, m)
        if not _ch_att(n['b'], n['b'].index('K' if w else 'k'), en):
            out.append(m)
    return out


def _ch_chk(s):
    w = s['t'] == 'w'
    return _ch_att(s['b'], s['b'].index('K' if w else 'k'), 'b' if w else 'w')


def _ch_stat(n, h):
    w = 'b' if n['t'] == 'w' else 'w'
    if not _ch_gen(n):
        return w + ':mate' if _ch_chk(n) else 'd:stalemate'
    if n['hm'] >= 100:
        return 'd:50-move rule'
    o = [p for p in n['b'] if p not in (' ', 'K', 'k')]
    if not o or (len(o) == 1 and o[0].lower() in ('n', 'b')):
        return 'd:insufficient material'
    if h.count(h[-1]) >= 3:
        return 'd:repetition'
    return None


def _ch_make_room(code, data):
    try:
        t = int(data.get('time'))
    except (TypeError, ValueError):
        t = 300
    try:
        inc = int(data.get('inc'))
    except (TypeError, ValueError):
        inc = 0
    t = min(max(t, 10), 7200)
    inc = min(max(inc, 0), 60)
    st = _ch_init()
    return {
        'id': code,
        'seats': {c: {'ws': None, 'pid': None, 'name': None} for c in 'wb'},
        'mv': [],
        'state': st,
        'hist': [_ch_key(st)],
        'clk': [float(t), float(t)],
        'last': time.monotonic(),
        'time': t,
        'inc': inc,
        'res': None,
        'draw': None,
        'rematch': set(),
        'specs': set(),
    }


def _ch_turn(room):
    return 'w' if len(room['mv']) % 2 == 0 else 'b'


def _ch_full(room):
    return sum(1 for c in 'wb' if room['seats'][c]['ws'] is not None)


def _ch_clock(room):
    """Settle the running clock up to now.  Only valid while both seats
    have been continuously connected since the last update (callers
    update `last` whenever connectivity changes)."""
    now = time.monotonic()
    if room['res'] is None and _ch_full(room) == 2:
        i = 0 if _ch_turn(room) == 'w' else 1
        room['clk'][i] = max(0.0, room['clk'][i] - (now - room['last']))
    room['last'] = now


def _ch_sync(room, color):
    return {
        'c': color,
        'names': {c: room['seats'][c]['name'] for c in 'wb'},
        'clk': [round(room['clk'][0], 1), round(room['clk'][1], 1)],
        'full': _ch_full(room),
        'res': room['res'],
        'draw': room['draw'],
        'mv': room['mv'],
    }


async def _ch_send(ws, room, color):
    try:
        await ws.send_str(json.dumps(_ch_sync(room, color)))
    except Exception:
        pass


async def _ch_broadcast(room):
    _ch_clock(room)
    for c in 'wb':
        seat = room['seats'][c]
        if seat['ws'] is not None:
            await _ch_send(seat['ws'], room, c)
    for spec in list(room['specs']):
        await _ch_send(spec, room, None)


async def chess_websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    session = {'room': None, 'color': None}
    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except (ValueError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            kind = data.get('t')
            rid = session.get('room')
            room = CHESS_ROOMS.get(rid) if rid else None

            if kind == 'join':
                code = ''.join(ch for ch in str(data.get('room') or '')
                               if ch.isalnum())[:8].upper()
                if not code:
                    continue
                # Drop tables nobody has touched for a while.
                nowm = time.monotonic()
                for stale in [k for k, r in CHESS_ROOMS.items()
                              if not r['specs'] and _ch_full(r) == 0
                              and nowm - r['last'] > 1800]:
                    CHESS_ROOMS.pop(stale, None)
                if code not in CHESS_ROOMS:
                    CHESS_ROOMS[code] = _ch_make_room(code, data)
                room = CHESS_ROOMS[code]
                pid = str(data.get('pid') or '')[:40]
                name = _clean_name(data.get('name')) or 'Player'
                want = data.get('color')
                if want not in ('w', 'b'):
                    want = 'r'
                _ch_clock(room)
                color = None
                for c in 'wb':
                    seat = room['seats'][c]
                    if pid and seat['pid'] == pid:
                        color = c
                        break
                if color is None:
                    free = [c for c in 'wb' if room['seats'][c]['ws'] is None]
                    if free:
                        color = want if want in free else (
                            random.choice(free) if want == 'r' else free[0])
                if color:
                    seat = room['seats'][color]
                    seat.update(ws=ws, pid=pid, name=name)
                    session.update(room=code, color=color)
                    room['specs'].discard(ws)
                else:
                    session.update(room=code, color=None)
                    room['specs'].add(ws)
                room['last'] = time.monotonic()
                await _ch_broadcast(room)
                continue

            if room is None:
                continue
            color = session.get('color')
            seated = bool(color) and room['seats'][color]['ws'] is ws

            if kind == 'move':
                if not seated or room['res'] is not None or _ch_full(room) < 2:
                    continue
                _ch_clock(room)
                if _ch_turn(room) != color:
                    await _ch_send(ws, room, color)
                    continue
                try:
                    a = int(data.get('a'))
                    b_ = int(data.get('b'))
                except (TypeError, ValueError):
                    continue
                p = str(data.get('p') or '')
                if p not in ('', 'Q', 'R', 'B', 'N'):
                    p = ''
                found = None
                for m in _ch_gen(room['state']):
                    if m['f'] == a and m['t'] == b_ and (m.get('p') or '') == p:
                        found = m
                        break
                if found is None:
                    # Illegal move or not your colour — resync the sender.
                    await _ch_send(ws, room, color)
                    continue
                state = _ch_apply(room['state'], found)
                room['state'] = state
                room['mv'].append([a, b_, p])
                room['hist'].append(_ch_key(state))
                room['clk'][0 if color == 'w' else 1] += room['inc']
                room['draw'] = None
                room['res'] = _ch_stat(state, room['hist'])
                room['last'] = time.monotonic()
                await _ch_broadcast(room)

            elif kind == 'flag':
                if not seated or room['res'] is not None or _ch_full(room) < 2:
                    continue
                _ch_clock(room)
                turn = _ch_turn(room)
                if room['clk'][0 if turn == 'w' else 1] <= 0:
                    room['res'] = ('b' if turn == 'w' else 'w') + ':time'
                await _ch_broadcast(room)

            elif kind == 'draw':
                if not seated or room['res'] is not None or _ch_full(room) < 2:
                    continue
                if room['draw'] is None:
                    room['draw'] = color
                elif room['draw'] != color:
                    room['res'] = 'd:agreement'
                    room['draw'] = None
                await _ch_broadcast(room)

            elif kind == 'resign':
                if not seated or room['res'] is not None:
                    continue
                _ch_clock(room)
                room['res'] = ('b' if color == 'w' else 'w') + ':resign'
                await _ch_broadcast(room)

            elif kind == 'rm':
                if not seated or room['res'] is None:
                    continue
                room['rematch'].add(color)
                if len(room['rematch']) == 2:
                    st = _ch_init()
                    room['mv'] = []
                    room['state'] = st
                    room['hist'] = [_ch_key(st)]
                    room['clk'] = [float(room['time']), float(room['time'])]
                    room['res'] = None
                    room['draw'] = None
                    room['rematch'] = set()
                    room['last'] = time.monotonic()
                await _ch_broadcast(room)

    except Exception as exc:
        print(f'[chess] websocket error: {exc!r}')
    finally:
        rid = session.get('room')
        room = CHESS_ROOMS.get(rid) if rid else None
        if room is not None:
            color = session.get('color')
            seat = room['seats'].get(color) if color else None
            if seat is not None and seat['ws'] is ws:
                _ch_clock(room)
                seat['ws'] = None
                room['last'] = time.monotonic()
                await _ch_broadcast(room)
            elif ws in room['specs']:
                room['specs'].discard(ws)
                await _ch_broadcast(room)
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


# ── Carrom multiplayer (room relay) ───────────────────────────────
# The carrom client runs the whole game locally. The server only pairs
# two players, keeps their seats through short disconnects, and relays
# game messages between them.
# Client -> server: create_table / join_table / game / leave
# Server -> client: table_created / table_joined / opponent_* / game

CARROM_TABLES = {}      # table code -> room dict
CARROM_GRACE = 45.0     # seconds a disconnected seat stays reserved
CARROM_STALE = 1800.0   # empty room lifetime


def _carrom_code():
    while True:
        code = ''.join(secrets.choice('ABCDEFGHJKLMNPQRSTUVWXYZ23456789')
                       for _ in range(4))
        if code not in CARROM_TABLES:
            return code


def _carrom_make_room(code):
    return {'code': code, 'seats': [None, None], 'last': time.monotonic()}


def _carrom_seat(room, ws):
    for i, seat in enumerate(room['seats']):
        if seat is not None and seat['ws'] is ws:
            return i
    return None


def _carrom_other(room, i):
    if i is None:
        return None
    seat = room['seats'][1 - i]
    return seat['ws'] if seat is not None else None


def _carrom_cleanup():
    now = time.monotonic()
    for code, room in list(CARROM_TABLES.items()):
        for i, seat in enumerate(room['seats']):
            if seat is not None and seat['ws'] is None:
                if now - seat.get('lost', room['last']) > CARROM_GRACE:
                    room['seats'][i] = None
        if all(s is None for s in room['seats']):
            if now - room['last'] > CARROM_STALE:
                CARROM_TABLES.pop(code, None)


async def _carrom_send(ws, payload):
    if ws is None:
        return
    try:
        await ws.send_str(json.dumps(payload))
    except Exception:
        pass


async def carrom_websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    session = {'table': None, 'seat': None}
    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            kind = str(data.get('type') or '').lower()
            rid = session.get('table')
            room = CARROM_TABLES.get(rid) if rid else None

            if kind == 'create_table':
                _carrom_cleanup()
                code = _carrom_code()
                room = _carrom_make_room(code)
                room['seats'][0] = {
                    'ws': ws,
                    'pid': str(data.get('pid') or '')[:40],
                    'name': _clean_name(data.get('name')) or 'Player',
                    'lost': 0,
                }
                CARROM_TABLES[code] = room
                session.update(table=code, seat=0)
                await _carrom_send(ws, {'type': 'table_created',
                                        'table': code, 'seat': 0})
                continue

            if kind == 'join_table':
                _carrom_cleanup()
                code = ''.join(ch for ch in str(data.get('table') or '')
                               if ch.isalnum())[:8].upper()
                room = CARROM_TABLES.get(code)
                if room is None:
                    await _carrom_send(ws, {'type': 'error',
                                            'message': 'Table not found.'})
                    continue
                pid = str(data.get('pid') or '')[:40]
                name = _clean_name(data.get('name')) or 'Player'
                seat_i = None
                resumed = False
                for i, seat in enumerate(room['seats']):
                    if seat is not None and pid and seat['pid'] == pid:
                        seat_i = i
                        resumed = True
                        break
                if seat_i is None:
                    for i, seat in enumerate(room['seats']):
                        if seat is None:
                            seat_i = i
                            break
                if seat_i is None:
                    waiting = any(s is not None and s['ws'] is None
                                  for s in room['seats'])
                    await _carrom_send(ws, {
                        'type': 'error',
                        'message': ('Table is waiting for a player to reconnect.'
                                    if waiting else 'Table is full.')})
                    continue
                seat = room['seats'][seat_i]
                old_ws = seat['ws'] if seat is not None else None
                if seat is None:
                    seat = {'ws': ws, 'pid': pid, 'name': name, 'lost': 0}
                    room['seats'][seat_i] = seat
                else:
                    seat.update(ws=ws, pid=pid or seat['pid'],
                                name=name or seat['name'], lost=0)
                session.update(table=code, seat=seat_i)
                room['last'] = time.monotonic()
                if old_ws is not None and old_ws is not ws:
                    try:
                        await old_ws.close(message=b'replaced')
                    except Exception:
                        pass
                await _carrom_send(ws, {'type': 'table_joined', 'table': code,
                                        'seat': seat_i,
                                        'resumed': resumed})
                other = _carrom_other(room, seat_i)
                if other is not None:
                    await _carrom_send(other, {
                        'type': ('opponent_reconnected' if resumed
                                 else 'opponent_joined'),
                        'table': code})
                continue

            if room is None:
                continue
            seat_i = _carrom_seat(room, ws)
            if seat_i is None:
                continue

            if kind == 'leave':
                room['seats'][seat_i] = None
                room['last'] = time.monotonic()
                other = _carrom_other(room, seat_i)
                if other is not None:
                    await _carrom_send(other, {'type': 'opponent_left',
                                               'table': rid})
                session.update(table=None, seat=None)
                if all(s is None for s in room['seats']):
                    CARROM_TABLES.pop(rid, None)
                continue

            if kind == 'game':
                other = _carrom_other(room, seat_i)
                if other is not None:
                    await _carrom_send(other, {'type': 'game', 'table': rid,
                                               'data': data.get('data')})
                room['last'] = time.monotonic()
    finally:
        rid = session.get('table')
        seat_i = session.get('seat')
        if rid is not None and seat_i is not None:
            room = CARROM_TABLES.get(rid)
            if room is not None:
                seat = room['seats'][seat_i]
                if seat is not None and seat['ws'] is ws:
                    seat['ws'] = None
                    seat['lost'] = time.monotonic()
                    room['last'] = time.monotonic()
                    other = _carrom_other(room, seat_i)
                    if other is not None:
                        await _carrom_send(other, {'type': 'opponent_left',
                                                   'table': rid})
    return ws


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


async def gameRummy(request):
    resp = web.FileResponse(STATIC_DIR / 'rummy.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


async def gameMemory(request):
    resp = web.FileResponse(STATIC_DIR / 'memory.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


async def gameChess(request):
    resp = web.FileResponse(STATIC_DIR / 'chess.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


async def gameCarrom(request):
    resp = web.FileResponse(STATIC_DIR / 'carrom.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


# ── Meme-sound list (auto-discovered from the sound effect folders) ──
# Drop `name.mp3` into "sound effect/sounds/" and `name.jpg` into
# "sound effect/image/" and the pair shows up in the sound board.

SFX_AUDIO_EXT = {'.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac'}
SFX_IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}


def _sfx_list():
    """Pair each sound with an image that shares the same base name."""
    sdir = STATIC_DIR / 'sound effect' / 'sounds'
    idir = STATIC_DIR / 'sound effect' / 'image'
    imgs = {}
    if idir.is_dir():
        for p in idir.iterdir():
            if p.is_file() and p.suffix.lower() in SFX_IMAGE_EXT:
                imgs.setdefault(p.stem, p.name)
    out = []
    if sdir.is_dir():
        for p in sorted(sdir.iterdir(), key=lambda x: x.name.lower()):
            if not (p.is_file() and p.suffix.lower() in SFX_AUDIO_EXT):
                continue
            img = imgs.get(p.stem)
            if img is None:
                continue
            # "are-baap-re-yaad-aya (mp3cut.net)" -> "Are baap re yaad aya"
            label = re.sub(r'\([^)]*\)', ' ', p.stem)
            label = re.sub(r'[-_]+', ' ', label)
            label = re.sub(r'\s+', ' ', label).strip()
            label = label[:1].upper() + label[1:] if label else p.stem
            out.append({'f': 'sound effect/sounds/' + p.name,
                        'i': 'sound effect/image/' + img,
                        'n': label})
    return out


async def apiSfx(request):
    return web.json_response(_sfx_list(),
                              headers={'Cache-Control': 'no-store'})


def create_app():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/rummy-ws', rummy_websocket_handler)
    app.router.add_get('/ws/chess', chess_websocket_handler)
    app.router.add_get('/carrom-ws', carrom_websocket_handler)
    app.router.add_get('/', index)
    app.router.add_get('/29', game29)
    app.router.add_get('/play', game29)
    app.router.add_get('/ludo', gameLudo)
    app.router.add_get('/memory', gameMemory)
    app.router.add_get('/rummy', gameRummy)
    app.router.add_get('/chess', gameChess)
    app.router.add_get('/carrom', gameCarrom)
    app.router.add_get('/api/sfx', apiSfx)
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
