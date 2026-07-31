// Dice Football room server — Worker + Durable Objects
// GameRoom: one per room code; stores latest state, broadcasts to all sockets.
// Lobby: singleton directory of open rooms (auto-pruned after 24h idle).
// Admin (ADMIN_KEY secret) is required to create or close rooms; listing/joining is open.

export class GameRoom {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4,8})/);
    const code = m ? m[1].toUpperCase() : null;

    if (url.pathname.endsWith('/reset')) {           // admin wipe (auth checked upstream)
      for (const s of this.ctx.getWebSockets()) { try { s.close(1000, 'room closed'); } catch (e) {} }
      await this.ctx.storage.deleteAll();
      return new Response('reset');
    }

    if (request.headers.get('Upgrade') !== 'websocket')
      return new Response('expected websocket', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    if (code) await this.ctx.storage.put('code', code);
    const game = await this.ctx.storage.get('game');
    const seq = (await this.ctx.storage.get('seq')) || 0;
    if (game) server.send(JSON.stringify({ type: 'state', seq, state: game }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, data) {
    let m;
    try { m = JSON.parse(data); } catch (e) { return; }
    if (m.type === 'state' && m.state) {
      const seq = ((await this.ctx.storage.get('seq')) || 0) + 1;
      await this.ctx.storage.put({ game: m.state, seq });
      const out = JSON.stringify({ type: 'state', seq, state: m.state });
      for (const s of this.ctx.getWebSockets()) { try { s.send(out); } catch (e) {} }
      // best-effort lobby heartbeat with a scoreboard summary
      try {
        const code = await this.ctx.storage.get('code');
        if (code) {
          const st = m.state, t = st.teams || [];
          await this.lobby().fetch('https://lobby/', { method: 'POST', body: JSON.stringify({
            code, gname: st.gname || code,
            names: t.map(x => x.name), scores: t.map(x => x.score),
            qtr: st.qtr, phase: st.phase, updated: Date.now()
          }) });
        }
      } catch (e) {}
    } else if (m.type === 'ping') {
      try { ws.send('{"type":"pong"}'); } catch (e) {}
    }
  }

  lobby() { return this.env.LOBBY.get(this.env.LOBBY.idFromName('lobby')); }
  webSocketClose(ws) { try { ws.close(); } catch (e) {} }
  webSocketError(ws) { try { ws.close(); } catch (e) {} }
}

export class Lobby {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === 'GET') {
      const all = await this.ctx.storage.list();
      const now = Date.now(), rooms = [];
      for (const [k, v] of all) {
        if (!k.startsWith('room:')) continue;
        if (now - (v.updated || 0) > 86400000) { await this.ctx.storage.delete(k); continue; } // 24h idle prune
        rooms.push(v);
      }
      rooms.sort((a, b) => b.updated - a.updated);
      return new Response(JSON.stringify(rooms), { headers: { 'Content-Type': 'application/json' } });
    }
    if (request.method === 'POST') {
      const v = await request.json();
      if (v && v.code) await this.ctx.storage.put('room:' + v.code, v);
      return new Response('ok');
    }
    if (request.method === 'DELETE') {
      const code = url.searchParams.get('code');
      if (code) await this.ctx.storage.delete('room:' + code.toUpperCase());
      return new Response('ok');
    }
    return new Response('bad request', { status: 400 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    };
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    const lobby = () => env.LOBBY.get(env.LOBBY.idFromName('lobby'));
    const isAdmin = () => {
      const k = url.searchParams.get('key');
      return !env.ADMIN_KEY || (k && k === env.ADMIN_KEY); // open until the secret is configured
    };

    if (url.pathname === '/rooms') {
      const r = await lobby().fetch('https://lobby/');
      return new Response(await r.text(), { headers: { 'Content-Type': 'application/json', ...cors } });
    }

    if (url.pathname === '/new') {
      if (!isAdmin()) return new Response('admin key required', { status: 403, headers: cors });
      const alpha = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
      const code = Array.from({ length: 5 }, () => alpha[Math.floor(Math.random() * alpha.length)]).join('');
      await lobby().fetch('https://lobby/', { method: 'POST', body: JSON.stringify({ code, gname: '(new room)', names: [], scores: [], updated: Date.now() }) });
      return new Response(JSON.stringify({ code }), { headers: { 'Content-Type': 'application/json', ...cors } });
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4,8})(\/ws)?$/);
    if (m) {
      const code = m[1].toUpperCase();
      const room = env.ROOMS.get(env.ROOMS.idFromName(code));
      if (request.method === 'DELETE') {
        if (!isAdmin()) return new Response('admin key required', { status: 403, headers: cors });
        await lobby().fetch('https://lobby/?code=' + code, { method: 'DELETE' });
        await room.fetch(new Request(url.origin + '/room/' + code + '/reset'));
        return new Response('closed', { headers: cors });
      }
      return room.fetch(request);
    }

    return new Response('Dice Football room server', { headers: cors });
  }
};
