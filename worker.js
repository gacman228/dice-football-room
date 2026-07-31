// Dice Football room server — Cloudflare Worker + Durable Object
// One Durable Object instance per room code. Clients connect via WebSocket,
// send full game state on every change; the DO stores it and broadcasts to all.
// Last write wins, sequenced by the DO so every phone converges.

export class GameRoom {
  constructor(ctx, env) { this.ctx = ctx; }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket')
      return new Response('expected websocket', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server); // hibernation API: DO can sleep between messages
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
    } else if (m.type === 'ping') {
      try { ws.send('{"type":"pong"}'); } catch (e) {}
    }
  }

  webSocketClose(ws) { try { ws.close(); } catch (e) {} }
  webSocketError(ws) { try { ws.close(); } catch (e) {} }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    };
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    if (url.pathname === '/new') {
      const alpha = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no 0/O/1/I/L ambiguity
      const code = Array.from({ length: 5 }, () => alpha[Math.floor(Math.random() * alpha.length)]).join('');
      return new Response(JSON.stringify({ code }), { headers: { 'Content-Type': 'application/json', ...cors } });
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4,8})\/ws$/);
    if (m) {
      const id = env.ROOMS.idFromName(m[1].toUpperCase());
      return env.ROOMS.get(id).fetch(request);
    }

    return new Response('Dice Football room server', { headers: cors });
  }
};
