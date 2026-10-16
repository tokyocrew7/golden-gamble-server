// Golden Gamble — serveur de salon WebSocket (même API que la "room" du jeu)
// Lancer : npm i ws && node server.js   (port 8080 par défaut : PORT=xxxx node server.js)
// Dans le jeu : MULTIJOUEUR → champ serveur : ws://IP_DU_PC:8080
// Optimisations : la liste des joueurs (présences) est regroupée et diffusée au plus 20 fois/s,
// uniquement si quelque chose a changé ; messages compressés désactivés (latence) ; ping anti-déco.
const http=require('http'),{WebSocketServer}=require('ws');
const PORT=+process.env.PORT||8080,TICK=50;
const rooms=new Map();let nid=1;
const srv=http.createServer((q,r)=>{r.writeHead(200,{'content-type':'text/plain'});r.end('Golden Gamble room server OK — '+[...rooms.values()].reduce((a,r)=>a+r.peers.size,0)+' joueur(s)')});
const wss=new WebSocketServer({server:srv,perMessageDeflate:false,maxPayload:256*1024});
function room(code){let r=rooms.get(code);if(!r){r={peers:new Map(),dirty:false};rooms.set(code,r)}return r}
function send(ws,o){if(ws.readyState===1)ws.send(typeof o=='string'?o:JSON.stringify(o))}
wss.on('connection',ws=>{ws.id='p'+(nid++)+Math.random().toString(36).slice(2,6);ws.alive=true;ws.room=null;ws.presence=null;
 ws.on('pong',()=>ws.alive=true);
 ws.on('message',buf=>{let m;try{m=JSON.parse(buf)}catch(e){return}
  if(m.t=='join'&&!ws.room){const code=String(m.room||'tour').slice(0,40);ws.room=code;const r=room(code);r.peers.set(ws.id,ws);send(ws,{t:'hello',id:ws.id});r.dirty=true;return}
  const r=ws.room&&rooms.get(ws.room);if(!r)return;
  if(m.t=='pres'){ws.presence=m.p||null;r.dirty=true}
  else if(m.t=='emit'){const out=JSON.stringify({t:'emit',topic:String(m.topic||''),data:m.data,from:ws.id});r.peers.forEach(p=>send(p,out))}});
 ws.on('close',()=>{const r=ws.room&&rooms.get(ws.room);if(!r)return;r.peers.delete(ws.id);r.dirty=true;if(!r.peers.size)rooms.delete(ws.room)})});
setInterval(()=>{rooms.forEach(r=>{if(!r.dirty)return;r.dirty=false;const out=JSON.stringify({t:'peers',list:[...r.peers.values()].map(p=>({id:p.id,presence:p.presence}))});r.peers.forEach(p=>send(p,out))})},TICK);
setInterval(()=>{wss.clients.forEach(ws=>{if(!ws.alive)return ws.terminate();ws.alive=false;try{ws.ping()}catch(e){}})},15000);
srv.listen(PORT,()=>console.log('Golden Gamble : serveur de salon sur ws://localhost:'+PORT));
