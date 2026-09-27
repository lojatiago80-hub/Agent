require('dotenv').config();
const express=require('express');
const {fork}=require('child_process');
const path=require('path');
const PORT=Number(process.env.PORT||10000);
let BOT_ID=Number(process.env.BOT_ID||0);
const {MASTER_URL:CONFIG_MASTER_URL}=require('./master.config');
const MASTER_URL=String(process.env.MASTER_URL||CONFIG_MASTER_URL||'').replace(/\/$/,'');
let AGENT_SECRET=String(process.env.AGENT_SECRET||'');
const AGENT_KEY=String(process.env.AGENT_KEY||process.env.RENDER_SERVICE_ID||process.env.RENDER_INSTANCE_ID||('agent-'+require('os').hostname()));
const AGENT_URL=String(process.env.AGENT_PUBLIC_URL||(process.env.RENDER_EXTERNAL_HOSTNAME?`https://${process.env.RENDER_EXTERNAL_HOSTNAME}`:'')).replace(/\/$/,'');
let poolApproved=false;
if(!MASTER_URL||/SEU-MASTER/i.test(MASTER_URL))console.warn('⚠️ Edite master.config.js no GitHub e informe a URL do Master.');
const app=express();app.use(express.json({limit:'512kb'}));
let child=null,currentConfig=null,logs=[],pendingMasterLog='';
let desiredRunning=false;
let stopPromise=null;
let lastChildHeartbeat=0, recoveryCount=0, watchdogRestarts=0, lastRecoveryAt=null;
// Proteção contra loop interno da Event Queue do node-metaverse.
let eventQueueFailures=[];
let eventQueueRecoveryInProgress=false;
let eventQueueRecoveryLevel=0;
let stableOnlineTimer=null;
const EQ_WINDOW_MS=Math.max(30000,Number(process.env.EVENT_QUEUE_WINDOW_MS||60000));
const EQ_MAX_FAILURES=Math.max(3,Number(process.env.EVENT_QUEUE_MAX_FAILURES||5));
const EQ_BACKOFF_MS=[90000,120000,300000,600000,900000];
const WATCHDOG_INTERVAL_MS=Math.max(30000,Number(process.env.WATCHDOG_INTERVAL_MS||60000));
const WATCHDOG_STALE_MS=Math.max(90000,Number(process.env.WATCHDOG_STALE_MS||150000));
let status={reachable:true,running:false,online:false,phase:'offline',lastReason:'',startedAt:null,pid:null,lastChangeAt:new Date().toISOString(),recoveryCount:0,watchdogRestarts:0,lastRecoveryAt:null,region:'',connectedAt:null};
function addLog(x){const raw=String(x).trim();const line=`[${new Date().toISOString()}] ${raw}`;logs.push(line);while(logs.length>180)logs.shift();console.log(line);if(/Processo|Login|Conect|Deslig|Watchdog|Event Queue|Auto.Reconectar|erro|error|falh|offline|online/i.test(raw))pendingMasterLog=raw.slice(0,4000);inspectEventQueue(raw)}
function inspectEventQueue(raw){
 if(!/Event queue aborted/i.test(raw))return;
 const now=Date.now(); eventQueueFailures=eventQueueFailures.filter(t=>now-t<EQ_WINDOW_MS); eventQueueFailures.push(now);
 if(eventQueueFailures.length>=EQ_MAX_FAILURES&&!eventQueueRecoveryInProgress){
   eventQueueFailures=[]; recoverFromEventQueueLoop().catch(e=>addLog('Event Queue Recovery: '+e.message));
 }
}
async function recoverFromEventQueueLoop(){
 if(eventQueueRecoveryInProgress||!desiredRunning||!child)return;
 eventQueueRecoveryInProgress=true; recoveryCount++; lastRecoveryAt=new Date().toISOString();
 const level=Math.min(eventQueueRecoveryLevel,EQ_BACKOFF_MS.length-1); const wait=EQ_BACKOFF_MS[level]; eventQueueRecoveryLevel=Math.min(eventQueueRecoveryLevel+1,EQ_BACKOFF_MS.length-1);
 setStatus({phase:'waiting_region',online:false,lastReason:`Event Queue instável; nova sessão em ${Math.round(wait/1000)}s`,recoveryCount,lastRecoveryAt});
 addLog(`⚠️ Event Queue falhou ${EQ_MAX_FAILURES} vezes em ${Math.round(EQ_WINDOW_MS/1000)}s. Encerrando sessão e aguardando ${Math.round(wait/1000)}s.`);
 const cfg=currentConfig; await stop(); desiredRunning=true;
 setStatus({phase:'waiting_region',online:false,lastReason:`Aguardando ${Math.round(wait/1000)}s antes da recuperação da Event Queue`});
 setTimeout(()=>{ eventQueueRecoveryInProgress=false; if(!desiredRunning||child)return; try{start(cfg)}catch(e){addLog('Event Queue Auto Recovery: '+e.message)} },wait);
}
function auth(req,res,next){const got=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');if(got!==AGENT_SECRET)return res.status(403).json({ok:false,error:'Agent Secret inválido'});next()}
function setStatus(p){status={...status,...p,reachable:true,lastChangeAt:new Date().toISOString()}}
function childMessage(m={}){if(m.type==='BOT_HEARTBEAT'){lastChildHeartbeat=Date.now();setStatus({running:true,pid:child&&child.pid,region:m.payload?.region||status.region||''});return;}if(m.type==='SL_ONLINE'){lastChildHeartbeat=Date.now();setStatus({running:true,online:true,phase:'online',lastReason:'',region:m.payload?.region||status.region||'',connectedAt:status.connectedAt||new Date().toISOString()});if(stableOnlineTimer)clearTimeout(stableOnlineTimer);stableOnlineTimer=setTimeout(()=>{eventQueueRecoveryLevel=0;eventQueueFailures=[];},300000);if(stableOnlineTimer.unref)stableOnlineTimer.unref();}if(m.type==='SL_CONNECTING')setStatus({running:true,online:false,phase:m.payload?.phase||'connecting',lastReason:m.payload?.reason||'Conectando...'});if(m.type==='SL_OFFLINE')setStatus({running:!!child,online:false,phase:m.payload?.phase||'offline',lastReason:m.payload?.reason||'Offline',connectedAt:null});}
function start(config){
 desiredRunning=true;
 if(child)return {ok:true,message:'Bot já está ligado.'};
 if(config)currentConfig=config;
 if(!currentConfig)throw new Error('Configuração do bot não carregada.');
 const safe={...currentConfig};delete safe.agentSecret;delete safe.renderApiKey;
 const env={...process.env,BOT_ID:String(BOT_ID),BOT_CONFIG_B64:Buffer.from(JSON.stringify(safe)).toString('base64')};
 child=fork(path.join(__dirname,'botsl.js'),[],{silent:true,env});
 lastChildHeartbeat=Date.now();
 const thisChild=child;
 setStatus({running:true,online:false,phase:'starting',lastReason:'Processo iniciado',startedAt:new Date().toISOString(),pid:child.pid});
 addLog('🚀 Processo do bot iniciado.');
 child.stdout.on('data',d=>addLog(d));child.stderr.on('data',d=>addLog(d));child.on('message',childMessage);
 child.on('exit',(c,signal)=>{
   addLog(`🛑 Processo finalizado código ${c}${signal?` sinal ${signal}`:''}`);
   if(child===thisChild)child=null;
   setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:`Processo finalizado código ${c}${signal?` (${signal})`:''}`});
   // Só tenta religar se o painel ainda deseja o bot online.
   if(desiredRunning)setTimeout(()=>bootstrap(true).catch(e=>addLog('Bootstrap após queda: '+e.message)),8000);
 });
 return {ok:true,message:'Bot ligado.'};
}
async function stop(){
 desiredRunning=false;
 if(stopPromise)return stopPromise;
 if(!child){setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:'Desligado pelo painel'});return {ok:true,message:'Bot já está desligado.'}}
 const c=child;
 setStatus({running:true,online:false,phase:'stopping',lastReason:'Encerrando processo...'});
 addLog('⏹️ Desligamento solicitado pelo painel.');
 stopPromise=new Promise(resolve=>{
   let done=false;
   const finish=(forced=false)=>{if(done)return;done=true;clearTimeout(forceTimer);clearTimeout(finalTimer);if(child===c)child=null;setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:forced?'Processo encerrado à força':'Desligado pelo painel'});stopPromise=null;resolve({ok:true,message:forced?'Bot desligado (encerramento forçado).':'Bot desligado.'})};
   c.once('exit',()=>finish(false));
   try{c.kill('SIGTERM')}catch{finish(false);return}
   const forceTimer=setTimeout(()=>{if(done)return;addLog('⚠️ Processo não encerrou com SIGTERM; enviando SIGKILL.');try{c.kill('SIGKILL')}catch{}},8000);
   const finalTimer=setTimeout(()=>finish(true),12000);
 });
 return stopPromise;
}
async function restart(config){desiredRunning=false;await stop();if(config)currentConfig=config;return start(currentConfig)}
async function watchdog(){
 if(!desiredRunning||!child||stopPromise)return;
 const age=Date.now()-lastChildHeartbeat;
 if(age<WATCHDOG_STALE_MS)return;
 watchdogRestarts++; recoveryCount++; lastRecoveryAt=new Date().toISOString();
 setStatus({phase:'recovering',online:false,lastReason:`Watchdog: processo sem resposta por ${Math.round(age/1000)}s`,watchdogRestarts,recoveryCount,lastRecoveryAt});
 addLog(`⚠️ Watchdog detectou processo sem resposta por ${Math.round(age/1000)}s. Recuperando...`);
 const cfg=currentConfig; await stop(); desiredRunning=true; setTimeout(()=>{try{start(cfg)}catch(e){addLog('Auto Recovery: '+e.message)}},5000);
}
function send(type,payload={}){if(!child)return {ok:false,error:'Bot desligado.'};if(!status.online)return {ok:false,error:`Bot ainda não está online. Status: ${status.phase}`};child.send({type,payload});addLog('📨 '+type);return {ok:true,message:'Comando enviado.'}}
async function poolSync(autoStart=true){
 if(!MASTER_URL||/SEU-MASTER/i.test(MASTER_URL))return null;
 const r=await fetch(MASTER_URL+'/api/agents/register',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({agentKey:AGENT_KEY,agentUrl:AGENT_URL,status})});
 const d=await r.json(); if(!r.ok||!d.ok)throw new Error(d.error||`Master HTTP ${r.status}`);
 poolApproved=!!d.approved; if(!poolApproved){setStatus({phase:'waiting_approval',online:false,lastReason:'Aguardando aprovação no Master'});return d;}
 AGENT_SECRET=String(d.agentSecret||AGENT_SECRET||'');
 const assignedId=Number(d.botId||0);
 if(assignedId!==BOT_ID){ if(child)await stop(); BOT_ID=assignedId; currentConfig=null; desiredRunning=false; }
 if(!assignedId){ if(child)await stop(); setStatus({phase:'available',online:false,lastReason:'Servidor disponível'}); return d; }
 currentConfig=d.config||currentConfig; desiredRunning=!!d.desiredOnline;
 if(autoStart&&desiredRunning&&!child)start(currentConfig); if(!desiredRunning&&child)await stop(); return d;
}
async function bootstrap(autoStart=true){return poolSync(autoStart)}
async function heartbeat(log){
 if(!MASTER_URL)return; try{
   if(log===undefined){log=pendingMasterLog;pendingMasterLog='';}
   if(!poolApproved||!AGENT_SECRET||!BOT_ID){await poolSync(true);return;}
   const r=await fetch(MASTER_URL+'/api/agent/heartbeat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({botId:BOT_ID,agentKey:AGENT_KEY,agentSecret:AGENT_SECRET,status,log:log||''})});
   if(r.ok){const d=await r.json();desiredRunning=!!d.desiredOnline;if(!desiredRunning&&child)await stop();}
   else if(r.status===403){poolApproved=false;AGENT_SECRET='';}
 }catch{}
}
app.get('/',(_q,r)=>r.json({ok:true,agent:'SL Bot Agent',agentKey:AGENT_KEY,approved:poolApproved,botId:BOT_ID,status}));app.get('/health',(_q,r)=>r.send('ok'));
app.get('/status',auth,(_q,r)=>r.json({ok:true,status}));app.get('/logs',auth,(_q,r)=>r.json({ok:true,logs}));
app.post('/start',auth,(q,r)=>{try{r.json(start(q.body?.config))}catch(e){r.status(400).json({ok:false,error:e.message})}});
app.post('/stop',auth,async(_q,r)=>{try{r.json(await stop())}catch(e){r.status(500).json({ok:false,error:e.message})}});
app.post('/restart',auth,async(q,r)=>{try{r.json(await restart(q.body?.config))}catch(e){r.status(400).json({ok:false,error:e.message})}});
app.post('/home',auth,(_q,r)=>r.json(send('GO_HOME')));app.post('/invite',auth,(q,r)=>r.json(send('GROUP_INVITE',q.body||{})));
app.listen(PORT,'0.0.0.0',()=>{console.log(`✅ Agent ${AGENT_KEY} ativo na porta ${PORT}. Aguardando Master/atribuição.`);bootstrap(true).catch(e=>addLog('Bootstrap: '+e.message));setInterval(()=>heartbeat().catch(()=>{}),60000);setInterval(()=>watchdog().catch(e=>addLog('Watchdog: '+e.message)),WATCHDOG_INTERVAL_MS)});
process.on('SIGTERM',async()=>{desiredRunning=false;try{await stop()}catch{}process.exit(0)});
