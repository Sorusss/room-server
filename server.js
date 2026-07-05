// Serveur de rooms : relaie le chat, les propositions de streaming, les
// réactions et les commandes de synchronisation vidéo entre participants.
//
// Plusieurs rooms peuvent coexister : une room est créée quand la première
// personne la rejoint (elle en devient l'hôte, et son mot de passe éventuel
// protège la room), et détruite quand la dernière personne part.
//
// Trois façons de l'utiliser :
//  - embarqué dans l'appli (celui qui crée une room héberge) ;
//  - standalone : `node server.js [port]` ;
//  - en ligne (Render, etc.) : le port vient de process.env.PORT.
const http = require('http');
const { WebSocketServer } = require('ws');

function startRoomServer(port = 8765) {
  return new Promise((resolve, reject) => {
    // Un vrai serveur HTTP en dessous : les hébergeurs (Render...) vérifient
    // la santé du service avec une requête GET.
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Private Streaming room server OK');
    });
    const wss = new WebSocketServer({ server });

    const rooms = new Map(); // nom -> { clients, hostId, nextId, settings, password }

    function getOrCreateRoom(name, password) {
      let room = rooms.get(name);
      if (!room) {
        room = {
          clients: new Map(), // ws -> { id, pseudo, muted, kicked }
          hostId: null,
          coHosts: new Set(), // ids promus par l'hôte (modération partagée)
          nextId: 1,
          settings: { hostOnlyControl: false, chatLocked: false, reactionsLocked: false },
          password: String(password || '')
        };
        rooms.set(name, room);
      }
      return room;
    }

    // Hôte ou co-hôte = pouvoirs de modération
    function isMod(room, id) {
      return id === room.hostId || room.coHosts.has(id);
    }

    function broadcast(room, obj, exceptWs = null) {
      const msg = JSON.stringify(obj);
      for (const ws of room.clients.keys()) {
        if (ws !== exceptWs && ws.readyState === 1) ws.send(msg);
      }
    }

    function roster(room) {
      return [...room.clients.values()].map((c) => ({
        id: c.id,
        pseudo: c.pseudo,
        muted: !!c.muted,
        coHost: room.coHosts.has(c.id)
      }));
    }

    wss.on('connection', (ws) => {
      ws.isAlive = true;
      ws.on('pong', () => {
        ws.isAlive = true;
      });

      ws.on('message', (raw) => {
        let msg;
        try {
          msg = JSON.parse(raw);
        } catch {
          return;
        }

        if (msg.type === 'join') {
          const name = String(msg.room || 'main').slice(0, 32).trim() || 'main';
          const existing = rooms.get(name);
          if (existing && existing.password && String(msg.password || '') !== existing.password) {
            ws.send(JSON.stringify({ type: 'join-error', error: 'Mot de passe incorrect' }));
            ws.close();
            return;
          }
          const room = existing || getOrCreateRoom(name, msg.password);
          const pseudo = String(msg.pseudo || 'Anonyme').slice(0, 24);
          const id = room.nextId++;
          room.clients.set(ws, { id, pseudo, muted: false, kicked: false });
          ws.roomName = name;
          if (room.hostId === null) room.hostId = id;
          ws.send(
            JSON.stringify({
              type: 'joined',
              selfId: id,
              hostId: room.hostId,
              settings: room.settings,
              roster: roster(room)
            })
          );
          broadcast(room, { type: 'roster', roster: roster(room), hostId: room.hostId }, ws);
          broadcast(room, { type: 'info', text: `${pseudo} a rejoint la room` }, ws);
          return;
        }

        const room = rooms.get(ws.roomName);
        const me = room && room.clients.get(ws);
        if (!me) return;

        switch (msg.type) {
          case 'chat':
            if (me.muted) return;
            if (room.settings.chatLocked && !isMod(room, me.id)) return;
            broadcast(room, {
              type: 'chat',
              from: me.pseudo,
              text: String(msg.text || '').slice(0, 500)
            });
            break;
          case 'typing':
            if (me.muted) return;
            if (room.settings.chatLocked && !isMod(room, me.id)) return;
            broadcast(room, { type: 'typing', from: me.pseudo }, ws);
            break;
          case 'reaction': {
            if (room.settings.reactionsLocked && !isMod(room, me.id)) return;
            const emoji = String(msg.emoji || '').slice(0, 8);
            if (emoji) broadcast(room, { type: 'reaction', emoji, from: me.pseudo }, ws);
            break;
          }
          case 'propose':
            broadcast(room, { type: 'propose', from: me.pseudo, url: String(msg.url || '') }, ws);
            break;
          case 'accepted':
            // Typé (pas un simple info) : le lanceur du stream déclenche une
            // re-synchronisation immédiate pour caler le nouvel arrivant.
            broadcast(room, { type: 'accepted', from: me.pseudo });
            break;
          case 'video':
            if (room.settings.hostOnlyControl && !isMod(room, me.id)) return;
            broadcast(
              room,
              { type: 'video', action: msg.action, time: msg.time, paused: msg.paused },
              ws
            );
            break;
          case 'settings':
            if (me.id !== room.hostId) return;
            room.settings.hostOnlyControl = !!msg.hostOnlyControl;
            room.settings.chatLocked = !!msg.chatLocked;
            room.settings.reactionsLocked = !!msg.reactionsLocked;
            broadcast(room, { type: 'settings', settings: room.settings });
            break;
          case 'kick': {
            if (!isMod(room, me.id)) return;
            for (const [w, c] of room.clients) {
              if (c.id !== msg.targetId || c.id === room.hostId) continue;
              // Un co-hôte ne peut pas expulser un autre co-hôte
              if (me.id !== room.hostId && room.coHosts.has(c.id)) continue;
              c.kicked = true;
              room.coHosts.delete(c.id);
              broadcast(room, { type: 'info', text: `${c.pseudo} a été expulsé par ${me.pseudo}` }, w);
              if (w.readyState === 1) w.send(JSON.stringify({ type: 'kicked' }));
              w.close();
            }
            break;
          }
          case 'mute': {
            if (!isMod(room, me.id)) return;
            for (const c of room.clients.values()) {
              if (c.id !== msg.targetId || c.id === room.hostId) continue;
              // Un co-hôte ne peut pas muter un autre co-hôte
              if (me.id !== room.hostId && room.coHosts.has(c.id)) continue;
              c.muted = !!msg.muted;
              broadcast(room, { type: 'roster', roster: roster(room), hostId: room.hostId });
              broadcast(room, {
                type: 'info',
                text: `${c.pseudo} a été ${c.muted ? 'muté' : 'démuté'} par ${me.pseudo}`
              });
            }
            break;
          }
          case 'cohost': {
            if (me.id !== room.hostId) return;
            for (const c of room.clients.values()) {
              if (c.id !== msg.targetId || c.id === room.hostId) continue;
              if (msg.value) room.coHosts.add(c.id);
              else room.coHosts.delete(c.id);
              broadcast(room, { type: 'roster', roster: roster(room), hostId: room.hostId });
              broadcast(room, {
                type: 'info',
                text: msg.value
                  ? `${c.pseudo} est maintenant co-hôte 🎖️`
                  : `${c.pseudo} n'est plus co-hôte`
              });
            }
            break;
          }
        }
      });

      ws.on('close', () => {
        const room = rooms.get(ws.roomName);
        if (!room) return;
        const me = room.clients.get(ws);
        if (!me) return;
        room.clients.delete(ws);
        room.coHosts.delete(me.id);
        if (room.clients.size === 0) {
          // Dernière personne partie : la room (et son mot de passe) disparaît
          rooms.delete(ws.roomName);
          return;
        }
        // L'hôte part → un co-hôte hérite en priorité, sinon le plus ancien
        if (me.id === room.hostId) {
          const heir =
            [...room.clients.values()].find((c) => room.coHosts.has(c.id)) ||
            [...room.clients.values()][0];
          room.hostId = heir.id;
          room.coHosts.delete(heir.id);
        }
        broadcast(room, { type: 'roster', roster: roster(room), hostId: room.hostId });
        if (!me.kicked) {
          broadcast(room, { type: 'info', text: `${me.pseudo} a quitté la room` });
        }
      });
    });

    // Keepalive : les proxys des hébergeurs coupent les connexions muettes.
    // Un ping toutes les 30 s les maintient en vie et détecte les morts.
    const pingLoop = setInterval(() => {
      for (const ws of wss.clients) {
        if (ws.isAlive === false) {
          ws.terminate();
          continue;
        }
        ws.isAlive = false;
        ws.ping();
      }
    }, 30000);
    wss.on('close', () => clearInterval(pingLoop));

    server.on('error', reject);
    server.listen(port, '0.0.0.0', () => resolve(server));
  });
}

module.exports = { startRoomServer };

if (require.main === module) {
  const port = Number(process.env.PORT) || Number(process.argv[2]) || 8765;
  startRoomServer(port)
    .then(() => console.log(`Room server en écoute sur le port ${port}`))
    .catch((err) => {
      console.error('Impossible de démarrer:', err.message);
      process.exit(1);
    });
}
