import { DeviceStatusEnum, MeshDevice, Protobuf } from "@meshtastic/sdk";
import { TransportHTTP } from "@meshtastic/transport-http";
import { create } from "@bufbuild/protobuf";
import "./style.css";

type AnyRecord = Record<string, any>;
type Message = {ts:number; event:string; from:string; to:string; channel:number; rssi?:number; snr?:number; text:string; id?:number; source:string};
type AirEvent = {ts:number; kind:string; from:number; to:number; channel:number; rssi?:number; snr?:number; hops?:number; id?:number; viaMqtt?:boolean};
type AirtimeSample = {ts:number; channel:number; tx:number};

const $ = <T extends HTMLElement>(id:string) => document.getElementById(id) as T;
const nodes = new Map<number, AnyRecord>();
const channels = new Map<number, AnyRecord>();
const neighborInfos = new Map<number, AnyRecord>(JSON.parse(localStorage.getItem("meshtastic-neighbors")||"[]"));
const radioConfigs = new Map<string, AnyRecord>();
const moduleConfigs = new Map<string, AnyRecord>();
const defaultRadioConfigs = new Set<string>();
const defaultModuleConfigs = new Set<string>();
let messages: Message[] = [];
const MAX_BROWSER_MESSAGES=1000;
const unreadChannels=new Set<number>();
let selectedMessageChannel:number|"all"="all";
let archiveInitialized=false;
let airEvents: AirEvent[] = JSON.parse(localStorage.getItem("meshtastic-air-events")||"[]");
let airtime: AirtimeSample[] = JSON.parse(localStorage.getItem("meshtastic-airtime")||"[]");
let device: MeshDevice | undefined;
let selectedNode: number | undefined;
let directContext = "";
let myNode = 0;
let ownNodeNum = 0;
let mapRenderPending=false;
let nodeRenderTimer:number|undefined;
let nodeCacheTimer:number|undefined;
let initialNodeSync=true;
let ownFixedPosition: {lat:number;lon:number} | undefined;
let ownerConfig: AnyRecord | undefined;
let settingsEditing=false;
const fallbackOwner={longName:"BarbieNode 💅",shortName:"db8c"};
const mapRadii=[2.5,5,10,20,40,80];
let mapRadiusIndex=3;

const radioSchemas:Record<string,any>={device:Protobuf.Config.Config_DeviceConfigSchema,position:Protobuf.Config.Config_PositionConfigSchema,power:Protobuf.Config.Config_PowerConfigSchema,network:Protobuf.Config.Config_NetworkConfigSchema,display:Protobuf.Config.Config_DisplayConfigSchema,lora:Protobuf.Config.Config_LoRaConfigSchema,bluetooth:Protobuf.Config.Config_BluetoothConfigSchema,security:Protobuf.Config.Config_SecurityConfigSchema,sessionkey:Protobuf.Config.Config_SessionkeyConfigSchema};
const moduleSchemas:Record<string,any>={mqtt:Protobuf.ModuleConfig.ModuleConfig_MQTTConfigSchema,serial:Protobuf.ModuleConfig.ModuleConfig_SerialConfigSchema,externalNotification:Protobuf.ModuleConfig.ModuleConfig_ExternalNotificationConfigSchema,storeForward:Protobuf.ModuleConfig.ModuleConfig_StoreForwardConfigSchema,rangeTest:Protobuf.ModuleConfig.ModuleConfig_RangeTestConfigSchema,telemetry:Protobuf.ModuleConfig.ModuleConfig_TelemetryConfigSchema,cannedMessage:Protobuf.ModuleConfig.ModuleConfig_CannedMessageConfigSchema,audio:Protobuf.ModuleConfig.ModuleConfig_AudioConfigSchema,remoteHardware:Protobuf.ModuleConfig.ModuleConfig_RemoteHardwareConfigSchema,neighborInfo:Protobuf.ModuleConfig.ModuleConfig_NeighborInfoConfigSchema,ambientLighting:Protobuf.ModuleConfig.ModuleConfig_AmbientLightingConfigSchema,detectionSensor:Protobuf.ModuleConfig.ModuleConfig_DetectionSensorConfigSchema,paxcounter:Protobuf.ModuleConfig.ModuleConfig_PaxcounterConfigSchema,statusmessage:Protobuf.ModuleConfig.ModuleConfig_StatusMessageConfigSchema,trafficManagement:Protobuf.ModuleConfig.ModuleConfig_TrafficManagementConfigSchema,tak:Protobuf.ModuleConfig.ModuleConfig_TAKConfigSchema};
for(const [key,schema] of Object.entries(radioSchemas)){radioConfigs.set(key,create(schema));defaultRadioConfigs.add(key)}
for(const [key,schema] of Object.entries(moduleSchemas)){moduleConfigs.set(key,create(schema));defaultModuleConfigs.add(key)}
for(let index=0;index<8;index++)channels.set(index,create(Protobuf.Channel.ChannelSchema,{index,role:index===0?Protobuf.Channel.Channel_Role.PRIMARY:Protobuf.Channel.Channel_Role.DISABLED,settings:create(Protobuf.Channel.ChannelSettingsSchema,index===0?{psk:new Uint8Array([1])}:{})}));

const hex = (n:number) => `!${(n >>> 0).toString(16).padStart(8,"0")}`;
const short = (id:string) => id.replace("!","").slice(-4).toUpperCase();
const nodeName = (n:number|string) => {
  const num = typeof n === "number" ? n >>> 0 : Number.parseInt(n.replace("!",""),16) >>> 0;
  const info = nodes.get(num);
  return info?.user?.longName || info?.user?.shortName || `Meshtastic ${short(hex(num))}`;
};
function renderOwnIdentity(){
  const num=(myNode||ownNodeNum)>>>0,user=ownerConfig||(num?nodes.get(num)?.user:undefined)||fallbackOwner;
  const name=String(user?.longName||"");
  $("own-long-name").textContent=name||fallbackOwner.longName;
  $("own-short-name").textContent=user?.shortName||fallbackOwner.shortName;
  $("own-node-id").textContent=num?hex(num):"—";
  if(name){document.title=`${name} · Meshtastic`;const header=document.querySelector<HTMLElement>("header strong");if(header)header.textContent=name}
}
const fmtTime = (ts:number) => new Date(ts * 1000).toLocaleString("ru-RU", {day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit"});
const json = (v:unknown) => JSON.stringify(v, (_k,x) => typeof x === "bigint" ? x.toString() : x, 2);
const routingErrors:Record<number,string>={1:"Маршрут к ноде неизвестен",2:"Получен отказ от промежуточной ноды",3:"Истекло время ожидания",4:"Нет подходящего радио-интерфейса",5:"Исчерпаны повторные передачи",6:"Канал недоступен",7:"Пакет слишком большой",8:"Нода получила запрос, но её сервис не ответил",9:"Превышен допустимый эфирный цикл",32:"Удалённая нода отклонила запрос",33:"Удалённая нода не разрешила запрос",34:"Не удалось использовать PKI",35:"У принимающей ноды нет публичного ключа отправителя",36:"Сессия администрирования недействительна",37:"Публичный ключ не разрешён для администрирования",38:"Превышен лимит частоты пакетов",39:"Нет публичного ключа ноды-получателя"};
const routingActions:Record<number,string>={1:"Дождитесь свежего пакета от ноды и повторите позднее либо используйте общий канал.",2:"Не повторяйте сразу: промежуточная нода отказалась пересылать пакет.",3:"Проверьте доступность ноды и попробуйте позднее.",4:"Проверьте, что LoRa включена и регион настроен.",5:"Не повторяйте сразу: пакет мог выйти в эфир, но подтверждение не вернулось.",6:"Выберите включённый канал, общий с получателем.",7:"Сократите текст сообщения.",8:"Получатель доступен, но запрошенная функция у него не работает.",9:"Подождите освобождения лимита эфирного времени.",32:"Проверьте тип запроса и совместимость прошивки удалённой ноды.",33:"Используйте канал и ключ, разрешённые удалённой нодой.",34:"Обновите NodeInfo/ключи обеих нод и попробуйте позднее.",35:"Получатель должен сначала получить свежий NodeInfo вашей ноды.",36:"Повторно подключитесь к плате и создайте новую административную сессию.",37:"Этот ключ не входит в список администраторов удалённой ноды.",38:"Подождите перед следующей попыткой.",39:"Дождитесь свежего NodeInfo получателя либо вернитесь в общий канал."};
const errorText = (e:unknown) => {
  if(e&&typeof e==="object"&&"error" in e){const x=e as AnyRecord;return `${routingErrors[Number(x.error)]||`Ошибка маршрутизации ${x.error}`} (пакет #${x.id??"—"})`}
  return e instanceof Error ? e.message : typeof e === "string" ? e : json(e);
};
const statusPill = (text:string, cls:string) => { const el=$("connection"); el.textContent=text; el.className=`pill ${cls}`; };
const fallbackChannelNames:Record<number,string>={0:"Первичный",1:"Приватный",2:"Район",3:"Ping"};
const channelName=(index:number)=>channels.get(index)?.settings?.name||fallbackChannelNames[index]||`Канал ${index}`;
const messageKey=(m:Message)=>`${m.ts}|${m.from}|${m.to}|${m.channel}|${m.text}`;
const messagesViewActive=()=>$("messages").classList.contains("active");

type Addressing={label:string;kind:"direct"|"ours"|"other"|"broadcast"|"unknown";target?:string};
const normalizedNodeId=(value:string|undefined)=>/^![0-9a-f]{8}$/i.test(value||"")?value!.toLowerCase():undefined;
const ownNodeIds=()=>new Set([myNode,ownNodeNum].filter(Boolean).map(n=>hex(n).toLowerCase()));
const pongTarget=(text:string)=>text.match(/\bpong\b!?\s*(?:\[\s*)?(![0-9a-f]{8})(?:\s*\])?/i)?.[1]?.toLowerCase();
function messageAddressing(m:Message):Addressing{
  const ownIds=ownNodeIds(),from=normalizedNodeId(m.from),to=normalizedNodeId(m.to),target=pongTarget(m.text),isPong=/\bpong\b/i.test(m.text),isPing=m.text.trim().toLowerCase()==="ping"&&(channelName(m.channel).toLowerCase()==="ping"||m.channel===3);
  if(isPong){
    if(!target)return {label:"Pong без указанного ID",kind:"unknown"};
    if(m.event==="tx"||(from!==undefined&&ownIds.has(from)))return {label:`Наш ответ на Ping · ${short(target)}`,kind:"ours",target};
    if(ownIds.has(target))return {label:"Ответ на наш Ping",kind:"ours",target};
    return {label:`Ответ на чужой Ping · ${short(target)}`,kind:"other",target};
  }
  if(isPing)return m.event==="tx"?{label:"Наш Ping",kind:"ours"}:{label:`Ping от ${short(m.from)}`,kind:"other",target:normalizedNodeId(m.from)};
  if(to){
    if(m.event!=="tx"&&ownIds.has(to))return {label:"Лично вам",kind:"direct",target:to};
    return {label:`Лично → ${short(to)}`,kind:"direct",target:to};
  }
  return {label:"Всем в канале",kind:"broadcast"};
}

function updateUnreadIndicators(){
  $<HTMLElement>("messages-unread").hidden=unreadChannels.size===0;
  document.querySelectorAll<HTMLElement>("#message-channels [data-channel]").forEach(button=>{
    const value=button.dataset.channel!,dot=button.querySelector<HTMLElement>(".unread-dot");
    if(dot)dot.hidden=value==="all"?unreadChannels.size===0:!unreadChannels.has(Number(value));
  });
}
function markUnread(channel:number){
  if(!messagesViewActive()||(selectedMessageChannel!=="all"&&selectedMessageChannel!==channel))unreadChannels.add(channel);
  updateUnreadIndicators();
}
function selectMessageChannel(value:number|"all"){
  selectedMessageChannel=value;
  if(value==="all")unreadChannels.clear();else unreadChannels.delete(value);
  if(value!=="all"){
    const composerChannel=$<HTMLSelectElement>("channel"),option=String(value);
    if([...composerChannel.options].some(item=>item.value===option))composerChannel.value=option;
    setBroadcast();
  }
  renderMessageChannelTabs();renderMessages();updateUnreadIndicators();
  void acknowledgeViewedMessages();
}
function renderMessageChannelTabs(){
  const box=$("message-channels"),indexes=new Set<number>();
  for(const c of channels.values())if(c.index===0||c.role!==0)indexes.add(Number(c.index));
  for(const m of messages)indexes.add(m.channel);
  const entries:[number|"all",string][]=[["all","Все"],...[...indexes].sort((a,b)=>a-b).map(i=>[i,channelName(i)] as [number,string])];
  box.replaceChildren(...entries.map(([value,label])=>{
    const button=document.createElement("button");button.type="button";button.className=`channel-tab${selectedMessageChannel===value?" active":""}`;button.dataset.channel=String(value);button.setAttribute("role","tab");button.setAttribute("aria-selected",String(selectedMessageChannel===value));
    button.append(document.createTextNode(label+" "),Object.assign(document.createElement("span"),{className:"unread-dot",hidden:value==="all"?unreadChannels.size===0:!unreadChannels.has(Number(value))}));
    button.addEventListener("click",()=>selectMessageChannel(value));return button;
  }));
  $("message-filter-label").textContent=selectedMessageChannel==="all"?"Показаны все каналы":`Показан канал: ${channelName(selectedMessageChannel)}`;
}

function addNode(num:number, patch:AnyRecord={}) {
  num >>>= 0;
  const current = nodes.get(num) || {num};
  nodes.set(num, {...current, ...patch, user:{...(current.user||{}), ...(patch.user||{})}});
}

function scheduleNodeRender(){
  if(nodeRenderTimer!==undefined)return;
  nodeRenderTimer=window.setTimeout(()=>{nodeRenderTimer=undefined;renderNodes();scheduleMap()},120);
}

function cachedNode(node:AnyRecord){
  return {
    num:Number(node.num)>>>0,
    user:node.user?{id:node.user.id,longName:node.user.longName,shortName:node.user.shortName,hwModel:node.user.hwModel,role:node.user.role,isLicensed:node.user.isLicensed,isUnmessagable:node.user.isUnmessagable}:undefined,
    position:node.position?{latitudeI:node.position.latitudeI,longitudeI:node.position.longitudeI,altitude:node.position.altitude,time:node.position.time,locationSource:node.position.locationSource}:undefined,
    lastHeard:node.lastHeard,lastRssi:node.lastRssi,lastSnr:node.lastSnr,lastHops:node.lastHops,signalAt:node.signalAt,snr:node.snr,hopsAway:node.hopsAway,channel:node.channel,viaMqtt:node.viaMqtt
  };
}

async function loadNodeCache(){
  try{
    const payload=await fetchJson("/node-cache.json");
    if(!Array.isArray(payload.nodes))return;
    for(const node of payload.nodes){const num=Number(node?.num)>>>0;if(num)addNode(num,cachedNode(node))}
    renderOwnIdentity();renderNodes();scheduleMap();
  }catch{}
}

async function saveNodeCache(){
  const snapshot={savedAt:Math.floor(Date.now()/1000),nodes:[...nodes.values()].filter(node=>Number(node.num)).map(cachedNode)};
  try{await fetch("/node-cache.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(snapshot)})}catch{}
}

function scheduleNodeCacheSave(){
  if(nodeCacheTimer!==undefined)window.clearTimeout(nodeCacheTimer);
  nodeCacheTimer=window.setTimeout(()=>{nodeCacheTimer=undefined;void saveNodeCache()},1500);
}

function hasUsablePublicKey(num:number){
  const value=nodes.get(num>>>0)?.user?.publicKey;
  const bytes=value instanceof Uint8Array?value:Array.isArray(value)?Uint8Array.from(value):undefined;
  return !!bytes&&bytes.length===32&&bytes.some(byte=>byte!==0);
}

function sendFailureText(error:unknown,target?:number){
  const packet=error&&typeof error==="object"&&"id" in error?`Пакет #${(error as AnyRecord).id??"—"}. `:"";
  const code=error&&typeof error==="object"&&"error" in error?Number((error as AnyRecord).error):undefined;
  if(code===39)return `${packet}Личное сообщение НЕ передано в эфир: у платы нет публичного ключа ${target?nodeName(target):"получателя"}.

Почему: запись о ноде могла прийти из старого ESP-архива или из пакета без NodeInfo. Одного короткого ID для зашифрованной личной отправки недостаточно.

Что делать: не повторяйте отправку; дождитесь свежего NodeInfo этой ноды либо нажмите «Вернуться в общий» и отправьте короткое публичное сообщение с ID адресата.`;
  if(code===5)return `${packet}${routingErrors[5]}. Плата пыталась доставить сообщение, но подтверждение не пришло. Не повторяйте сразу: пакет мог выйти в эфир, а обратный маршрут мог не сработать.`;
  if(code!==undefined)return `${packet}Ошибка маршрутизации ${code}: ${routingErrors[code]||"неизвестная ошибка"}.\n\nЧто делать: ${routingActions[code]||"Не повторяйте отправку сразу; откройте полные данные пакета и проверьте состояние ноды."}`;
  return `Ошибка отправки: ${errorText(error)}`;
}

const isOwnNode=(num:number)=>num===myNode||num===ownNodeNum;
function nodeCoordinates(n:AnyRecord){const p=n.position;if(p){const lat=Number(p.latitudeI||0)/1e7,lon=Number(p.longitudeI||0)/1e7;if(Number.isFinite(lat)&&Number.isFinite(lon)&&(lat||lon))return {lat,lon,source:"mesh"}}if(isOwnNode(n.num)&&ownFixedPosition)return {...ownFixedPosition,source:"local-fixed"};}
function scheduleMap(){if(mapRenderPending)return;mapRenderPending=true;requestAnimationFrame(()=>{mapRenderPending=false;renderMap()})}

async function loadOwnLocation(){try{const p=await fetchJson("/node-location.json"),lat=Number(p.latitude),lon=Number(p.longitude),nodeId=Number(p.nodeId)>>>0;if(Number.isFinite(lat)&&lat>=-90&&lat<=90&&Number.isFinite(lon)&&lon>=-180&&lon<=180&&nodeId){ownFixedPosition={lat,lon};ownNodeNum=nodeId;if(!nodes.get(ownNodeNum)?.user)addNode(ownNodeNum,{user:fallbackOwner});renderOwnIdentity();renderMessages();renderNodes();scheduleMap()}}catch{}}

function renderMap(){
  const stage=$("map-stage");if(!stage)return;
  const recentOnly=$<HTMLInputElement>("map-active")?.checked??false,showLinks=$<HTMLInputElement>("map-links")?.checked??true,now=Date.now()/1000;
  const positioned=[...nodes.values()].map(n=>({n,pos:nodeCoordinates(n)})).filter((x):x is {n:AnyRecord,pos:{lat:number,lon:number,source:string}}=>!!x.pos&&(!recentOnly||isOwnNode(x.n.num)||now-(x.n.lastHeard||0)<7200));
  stage.replaceChildren();
  if(!positioned.length){$("map-count").textContent="(0)";stage.append(Object.assign(document.createElement("div"),{className:"map-empty muted",textContent:"В базе пока нет нод с координатами"}));$("map-caption").textContent="0 нод";return}
  const own=positioned.find(x=>isOwnNode(x.n.num)),sortedLat=positioned.map(x=>x.pos.lat).sort((a,b)=>a-b),sortedLon=positioned.map(x=>x.pos.lon).sort((a,b)=>a-b),center=own?.pos||{lat:sortedLat[Math.floor(sortedLat.length/2)],lon:sortedLon[Math.floor(sortedLon.length/2)]};
  const radius=mapRadii[mapRadiusIndex],lonKm=111.32*Math.cos(center.lat*Math.PI/180),rect=stage.getBoundingClientRect(),xScale=45*Math.min(1,rect.height/Math.max(1,rect.width)),yScale=45*Math.min(1,rect.width/Math.max(1,rect.height)),project=(lat:number,lon:number)=>{const dx=(lon-center.lon)*lonKm,dy=(lat-center.lat)*110.574;return {dx,dy,distance:Math.hypot(dx,dy),x:50+xScale*dx/radius,y:50-yScale*dy/radius}};
  const visible=positioned.map(x=>({...x,base:project(x.pos.lat,x.pos.lon)})).filter(x=>isOwnNode(x.n.num)||x.base.distance<=radius);
  $("map-count").textContent=`(${visible.length}/${positioned.length})`;
  $("map-radius").textContent=`${radius} км`;$<HTMLButtonElement>("map-zoom-in").disabled=mapRadiusIndex===0;$<HTMLButtonElement>("map-zoom-out").disabled=mapRadiusIndex===mapRadii.length-1;
  const occupied:{x:number;y:number}[]=[],layout=new Map<number,{x:number;y:number}>();
  for(const item of visible.slice().sort((a,b)=>Number(isOwnNode(b.n.num))-Number(isOwnNode(a.n.num)))){let best={x:item.base.x,y:item.base.y};if(!isOwnNode(item.n.num)){outer:for(let ring=0;ring<7;ring++){const count=ring?12:1;for(let i=0;i<count;i++){const distance=ring*3.4,angle=2*Math.PI*i/count,candidate={x:Math.max(3,Math.min(97,item.base.x+Math.cos(angle)*distance)),y:Math.max(3,Math.min(97,item.base.y+Math.sin(angle)*distance))};if(occupied.every(p=>Math.hypot(p.x-candidate.x,p.y-candidate.y)>4)){best=candidate;break outer}}}}occupied.push(best);layout.set(item.n.num>>>0,best)}
  const byNum=new Map(visible.map(x=>[x.n.num>>>0,x])),svg=document.createElementNS("http://www.w3.org/2000/svg","svg");svg.setAttribute("class","map-links");svg.setAttribute("viewBox","0 0 100 100");svg.setAttribute("preserveAspectRatio","none");
  for(const factor of [.5,1]){const ellipse=document.createElementNS(svg.namespaceURI,"ellipse");ellipse.setAttribute("cx","50");ellipse.setAttribute("cy","50");ellipse.setAttribute("rx",String(xScale*factor));ellipse.setAttribute("ry",String(yScale*factor));ellipse.setAttribute("class","map-range");svg.append(ellipse)}
  for(const [x1,y1,x2,y2] of [[50-xScale,50,50+xScale,50],[50,50-yScale,50,50+yScale]]){const line=document.createElementNS(svg.namespaceURI,"line");line.setAttribute("x1",String(x1));line.setAttribute("y1",String(y1));line.setAttribute("x2",String(x2));line.setAttribute("y2",String(y2));line.setAttribute("class","map-axis");svg.append(line)}
  let links=0;
  if(showLinks){for(const [source,info] of neighborInfos){if(!byNum.has(source))continue;for(const neighbor of info.neighbors||[]){const target=Number(neighbor.nodeId)>>>0;if(!byNum.has(target))continue;const ap=layout.get(source),bp=layout.get(target);if(!ap||!bp)continue;const line=document.createElementNS(svg.namespaceURI,"line");line.setAttribute("x1",String(ap.x));line.setAttribute("y1",String(ap.y));line.setAttribute("x2",String(bp.x));line.setAttribute("y2",String(bp.y));line.setAttribute("class",`map-link${Number(neighbor.snr)<-10?" weak":""}`);svg.append(line);links++}}}stage.append(svg);
  const north=Object.assign(document.createElement("span"),{className:"map-north",textContent:"С"}),distance=Object.assign(document.createElement("span"),{className:"map-distance",textContent:`${radius/2} км`});north.style.top=`${50-yScale-3}%`;distance.style.top=`${50-yScale/2}%`;stage.append(north,distance);
  for(const {n,pos} of visible){const p=layout.get(n.num>>>0)!,button=document.createElement("button"),a=activity(n.lastHeard||0),self=isOwnNode(n.num);button.className=`map-node ${self?"self":a.cls==="activity-live"?"":a.cls==="activity-recent"?"recent":"old"}`;button.style.left=`${p.x}%`;button.style.top=`${p.y}%`;button.textContent=self?"МЫ":short(hex(n.num));button.title=`${nodeName(n.num)}${self?pos.source==="local-fixed"?" · наша локальная точка":" · позиция с устройства":""}\n${pos.lat.toFixed(6)}, ${pos.lon.toFixed(6)} · ${project(pos.lat,pos.lon).distance.toFixed(1)} км от центра${n.snr!==undefined?`\nSNR ${Number(n.snr).toFixed(1)} dB`:""}`;button.addEventListener("click",()=>openNode(n.num));stage.append(button)}
  $("map-caption").textContent=`${visible.length} из ${positioned.length} нод · радиус ${radius} км · ${links} связей${own?` · МЫ: ${own.pos.source==="local-fixed"?"локальная точка":"позиция телефона/ноды"}`:""}`;
}

function activity(lastHeard=0){const age=Date.now()/1000-lastHeard;if(age<900)return {text:"активна",cls:"activity-live"};if(age<7200)return {text:"недавно",cls:"activity-recent"};return {text:"давно",cls:"activity-old"}}

function recordPacket(packet:AnyRecord){
  const decoded=packet.payloadVariant?.case==="decoded"?packet.payloadVariant.value:undefined;
  const port=decoded?.portnum;
  const kind=port===undefined?(packet.payloadVariant?.case||"UNKNOWN"):(Protobuf.Portnums.PortNum as AnyRecord)[port]||`PORT_${port}`;
  const hops=packet.hopStart>0?Math.max(0,Number(packet.hopStart)-Number(packet.hopLimit||0)):undefined;
  const event:AirEvent={ts:Math.floor(Date.now()/1000),kind,from:Number(packet.from)>>>0,to:Number(packet.to)>>>0,channel:Number(packet.channel)||0,rssi:packet.rxRssi||undefined,snr:packet.rxSnr||undefined,hops,id:packet.id,viaMqtt:packet.viaMqtt};
  airEvents.push(event);airEvents=airEvents.slice(-400);localStorage.setItem("meshtastic-air-events",JSON.stringify(airEvents));
  if(event.from){const current=nodes.get(event.from)||{},history=[...(current.signalHistory||[]),{ts:event.ts,rssi:event.rssi,snr:event.snr,hops:event.hops}].slice(-50);addNode(event.from,{lastHeard:event.ts,lastRssi:event.rssi,lastSnr:event.snr,lastHops:event.hops,signalHistory:history})}
  renderAir();renderNodes();
}

async function fetchJson(url:string) {
  const response = await fetch(url, {cache:"no-store"});
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

const notificationLabels:Record<string,string>={green:"LED: зелёный · всё прочитано",blue:"LED: синий · есть общие сообщения",red:"LED: красный · есть личные сообщения",white:"LED: белый · есть общие и личные"};
async function loadNotificationStatus(){
  try{
    const state=await fetchJson("/notifications/status"),color=String(state.color||"");
    $("notification-state").textContent=notificationLabels[color]||`LED: ${color||"неизвестно"}`;
    $("notification-state").className=`notification-state notification-${color||"unknown"}`;
  }catch{
    $("notification-state").textContent="LED: требуется новая прошивка";
    $("notification-state").className="notification-state muted";
  }
}
async function acknowledgeViewedMessages(){
  if(!messagesViewActive())return;
  const scope=selectedMessageChannel==="all"?"all":String(selectedMessageChannel);
  try{
    const response=await fetch("/notifications/read",{method:"POST",headers:{"Content-Type":"text/plain"},body:scope});
    if(!response.ok)throw new Error(`${response.status}`);
    if(selectedMessageChannel==="all")unreadChannels.clear();else unreadChannels.delete(selectedMessageChannel);
    updateUnreadIndicators();await loadNotificationStatus();
  }catch{}
}

type SettingsEntry={id:string;label:string;kind:"radio"|"module"|"channel"|"owner";key?:string;index?:number;value:AnyRecord};
const bytesToBase64=(bytes:Uint8Array)=>{let s="";for(const b of bytes)s+=String.fromCharCode(b);return btoa(s)};
const settingsStringify=(value:unknown)=>JSON.stringify(value,(_key,item)=>typeof item==="bigint"?{$bigint:item.toString()}:item instanceof Uint8Array?{$bytes:bytesToBase64(item)}:item,2);
const settingsParse=(text:string)=>JSON.parse(text,(_key,item)=>{if(item&&typeof item==="object"&&Object.keys(item).length===1){if(typeof item.$bigint==="string")return BigInt(item.$bigint);if(typeof item.$bytes==="string"){const raw=atob(item.$bytes),bytes=new Uint8Array(raw.length);for(let i=0;i<raw.length;i++)bytes[i]=raw.charCodeAt(i);return bytes}}return item});
function settingsEntries():SettingsEntry[]{
  const entries:SettingsEntry[]=[];
  for(const [key,value] of [...radioConfigs].sort())entries.push({id:`radio:${key}`,label:`Система · ${key}${defaultRadioConfigs.has(key)?" · значения по умолчанию":""}`,kind:"radio",key,value});
  for(const [key,value] of [...moduleConfigs].sort())entries.push({id:`module:${key}`,label:`Модуль · ${key}${defaultModuleConfigs.has(key)?" · значения по умолчанию":""}`,kind:"module",key,value});
  for(const channel of [...channels.values()].filter(c=>c.$typeName).sort((a,b)=>a.index-b.index))entries.push({id:`channel:${channel.index}`,label:`Канал ${channel.index} · ${channel.settings?.name||(channel.role===Protobuf.Channel.Channel_Role.DISABLED?"свободен":channel.index===0?"Первичный":"без имени")}`,kind:"channel",index:channel.index,value:channel});
  if(ownerConfig)entries.push({id:"owner",label:"Владелец ноды",kind:"owner",value:ownerConfig});
  return entries;
}
function selectedSettingsEntry(){const id=$<HTMLSelectElement>("settings-section").value;return settingsEntries().find(x=>x.id===id)}
function loadSelectedSettings(){const entry=selectedSettingsEntry(),area=$<HTMLTextAreaElement>("settings-json");if(!entry){area.value="";$<HTMLButtonElement>("settings-apply").disabled=true;return}area.value=settingsStringify(entry.value);settingsEditing=false;$<HTMLButtonElement>("settings-apply").disabled=false;$("settings-state").textContent=`Загружен раздел «${entry.label}».`}
function refreshSettingsOptions(){const select=$<HTMLSelectElement>("settings-section"),current=select.value,entries=settingsEntries();select.replaceChildren(...entries.map(entry=>Object.assign(document.createElement("option"),{value:entry.id,textContent:entry.label})));if(entries.some(x=>x.id===current))select.value=current;else if(entries.length)select.value=entries[0].id;if(!settingsEditing)loadSelectedSettings()}
function exportSettings(){const backup={exportedAt:new Date().toISOString(),node:ownNodeNum?hex(ownNodeNum):undefined,radio:Object.fromEntries(radioConfigs),modules:Object.fromEntries(moduleConfigs),channels:[...channels.values()].filter(c=>c.$typeName),owner:ownerConfig},blob=new Blob([settingsStringify(backup)],{type:"application/json"}),link=document.createElement("a");link.href=URL.createObjectURL(blob);link.download=`barbienode-config-${new Date().toISOString().slice(0,10)}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(link.href),1000);$("settings-state").textContent="Резервная копия скачана. Она может содержать пароли и ключи каналов — храните её безопасно."}
function hydrateSettingsFromEditor(){if(!device)return;const editor=device.meshClient.config.editor as AnyRecord,radio=editor.radio?.value??editor.radio?.peek?.(),modules=editor.modules?.value??editor.modules?.peek?.(),editorChannels=editor.channels?.value??editor.channels?.peek?.();if(radio)for(const [key,value] of Object.entries(radio))if(value){radioConfigs.set(key,value as AnyRecord);defaultRadioConfigs.delete(key)}if(modules)for(const [key,value] of Object.entries(modules))if(value){moduleConfigs.set(key,value as AnyRecord);defaultModuleConfigs.delete(key)}if(editorChannels instanceof Map)for(const [index,value] of editorChannels)channels.set(Number(index),value);renderChannels();refreshSettingsOptions()}
async function applySettings(){
  const entry=selectedSettingsEntry();if(!entry||!device)return;
  let value:AnyRecord;try{value=settingsParse($<HTMLTextAreaElement>("settings-json").value)}catch(e){$("settings-state").textContent=`Ошибка JSON: ${errorText(e)}`;return}
  if(!confirm(`Применить раздел «${entry.label}»? Плата может перезагрузиться или отключиться от Wi‑Fi.`))return;
  const button=$<HTMLButtonElement>("settings-apply");button.disabled=true;$("settings-state").textContent="Отправляю конфигурацию на плату…";
  try{const editor=device.meshClient.config.editor as AnyRecord;if(entry.kind==="radio")editor.setRadioSection(entry.key,value);else if(entry.kind==="module")editor.setModuleSection(entry.key,value);else if(entry.kind==="channel")editor.setChannel(value);else editor.setOwner(value);const committed=await editor.commit();if(committed.status==="error")throw committed.error;if(entry.kind==="radio")radioConfigs.set(entry.key!,value);else if(entry.kind==="module")moduleConfigs.set(entry.key!,value);else if(entry.kind==="channel")channels.set(entry.index!,value);else{ownerConfig=value;renderOwnIdentity()}settingsEditing=false;$("settings-state").textContent="Настройки применены. Если раздел требует перезагрузки, соединение восстановится автоматически.";refreshSettingsOptions()}catch(e){$("settings-state").textContent=`Ошибка применения: ${errorText(e)}`}finally{button.disabled=false}
}

async function loadArchive() {
  const result:Message[]=[];
  const known=new Set(messages.map(messageKey));
  for (const path of ["/nightbot.previous.jsonl","/nightbot.jsonl","/nightbot.sent.jsonl"]) {
    try {
      const response=await fetch(path,{cache:"no-store"});
      if(!response.ok) continue;
      for(const line of (await response.text()).split(/\r?\n/)) {
        try {
          const row=JSON.parse(line);
          if(row && typeof row.ts==="number" && typeof row.text==="string") {
            result.push({...row,channel:Number(row.channel)||0,source:path.includes("sent")?"исходящие ESP":"архив ESP"});
            if(typeof row.from==="string" && row.from.startsWith("!")){const num=Number.parseInt(row.from.slice(1),16),known=nodes.get(num)||{};addNode(num,{lastHeard:Math.max(known.lastHeard||0,row.ts),lastRssi:known.lastRssi??row.rssi,lastSnr:known.lastSnr??row.snr,signalAt:known.signalAt??row.ts})}
          }
        } catch {}
      }
    } catch {}
  }
  const sent:Message[]=JSON.parse(localStorage.getItem("meshtastic-esp-sent")||"[]");
  if(archiveInitialized)for(const m of result)if(m.event==="rx"&&!known.has(messageKey(m)))markUnread(m.channel);
  messages=Array.from(new Map([...result,...sent,...messages].map(m=>[messageKey(m),m])).values()).sort((a,b)=>a.ts-b.ts).slice(-MAX_BROWSER_MESSAGES);
  archiveInitialized=true;renderMessageChannelTabs();renderMessages();renderNodes();
  if(messagesViewActive())void acknowledgeViewedMessages();
}

async function saveSentArchive(){
  const rows=messages.filter(m=>m.event==="tx").slice(-200).map(m=>JSON.stringify({...m,source:undefined})).join("\n")+"\n";
  const form=new FormData();form.append("file",new Blob([rows],{type:"application/x-ndjson"}),"nightbot.sent.jsonl");
  const response=await fetch("/upload",{method:"POST",body:form});if(!response.ok)throw new Error(`${response.status} ${response.statusText}`);
}

function renderMessages() {
  const list=$("message-list"); list.replaceChildren();
  const visible=messages.filter(m=>selectedMessageChannel==="all"||m.channel===selectedMessageChannel).slice().sort((a,b)=>b.ts-a.ts);
  if(!visible.length){list.innerHTML='<p class="muted">В этом потоке сообщений пока нет.</p>';return;}
  for(const m of visible) {
    const fromNum=m.from?.startsWith("!")?Number.parseInt(m.from.slice(1),16)>>>0:0;
    const addressing=messageAddressing(m);
    const directForUs=m.event!=="tx"&&addressing.label==="Лично вам",pingReplyForUs=addressing.label==="Ответ на наш Ping";
    const card=document.createElement("article"); card.className=`card${directForUs?" message-direct":pingReplyForUs?" message-ping-reply":""}`;
    const head=document.createElement("div"); head.className="card-head";
    const who=document.createElement(fromNum?"button":"strong"); who.className=fromNum?"node-button":""; who.textContent=m.event==="tx"?"Вы":nodeName(fromNum);
    if(fromNum) who.addEventListener("click",()=>openNode(fromNum));
    const channel=document.createElement("button");channel.type="button";channel.className="channel-badge";channel.textContent=channelName(m.channel);channel.title=`Показать только канал ${channelName(m.channel)}`;channel.addEventListener("click",()=>selectMessageChannel(m.channel));
    const addressingBadge=Object.assign(document.createElement("span"),{className:`addressing-badge ${addressing.kind}`,textContent:addressing.label});
    head.append(who, Object.assign(document.createElement("span"),{className:"meta",textContent:fmtTime(m.ts)}),channel,addressingBadge,Object.assign(document.createElement("span"),{className:"badge",textContent:m.source}));
    const text=document.createElement("div"); text.className="text"; text.textContent=m.text;
    const actions=document.createElement("div");actions.className="message-actions";
    if(m.event!=="tx"){const direct=addressing.kind==="direct"&&fromNum>0,reply=document.createElement("button");reply.type="button";reply.className="secondary";reply.textContent=direct?`Ответить лично ${short(m.from)}`:`Ответить в ${channelName(m.channel)}`;reply.addEventListener("click",()=>{const foreignDirect=direct&&addressing.label!=="Лично вам",context=[m.source==="архив ESP"?"Это архивная запись: данные о ноде и её ключ могли устареть.":"",foreignDirect?`Исходное сообщение было адресовано ${addressing.target?short(addressing.target):"другой ноде"}, а не вашей ноде.`:""] .filter(Boolean).join(" ");direct?setDirect(fromNum,context):setBroadcast();$<HTMLSelectElement>("channel").value=String(m.channel);updateDestination();$<HTMLTextAreaElement>("message").focus()});actions.append(reply)}
    if(m.rssi!==undefined||m.snr!==undefined){const signal=document.createElement("span");signal.className="meta signal";signal.title="RSSI: ближе к 0 — сильнее. SNR: выше — чище.";signal.textContent=[m.rssi!==undefined?`RSSI ${m.rssi} dBm`:"",m.snr!==undefined?`SNR ${Number(m.snr).toFixed(2)} dB`:""].filter(Boolean).join(" · ");actions.append(signal)}
    const details=document.createElement("details"); const summary=document.createElement("summary"); summary.textContent="Полные данные";
    const pre=document.createElement("pre"); pre.textContent=json({...m,addressing,fromName:fromNum?nodeName(fromNum):undefined,fromDecimal:fromNum||undefined,fromHex:fromNum?hex(fromNum):m.from});
    details.append(summary,pre); card.append(head,text,actions,details); list.append(card);
  }
}

function nodeSearchText(n:AnyRecord) { return `${n.num} ${hex(n.num)} ${short(hex(n.num))} ${n.user?.longName||""} ${n.user?.shortName||""}`.toLowerCase(); }
function renderNodes() {
  const q=($("node-search") as HTMLInputElement).value.trim().toLowerCase().replace(/^0x/,"");
  const list=$("node-list"); list.replaceChildren();
  const values=[...nodes.values()].filter(n=>!q||nodeSearchText(n).includes(q)).sort((a,b)=>(b.lastHeard||0)-(a.lastHeard||0));
  $("node-count").textContent=`(${nodes.size})`;
  for(const n of values) {
    const card=document.createElement("article"); card.className="card node-card"; card.tabIndex=0;
    const head=document.createElement("div"); head.className="card-head";
    head.append(Object.assign(document.createElement("strong"),{textContent:nodeName(n.num)}),Object.assign(document.createElement("span"),{className:"badge",textContent:short(hex(n.num))}));
    if(n.lastHeard){const a=activity(n.lastHeard);head.append(Object.assign(document.createElement("span"),{className:`badge ${a.cls}`,textContent:a.text}))}
    const meta=document.createElement("div"); meta.className="meta"; meta.textContent=`${hex(n.num)} · ${n.num}${n.lastHeard?` · ${fmtTime(n.lastHeard)}`:""}`;
    const signal=document.createElement("div");signal.className="meta signal";signal.textContent=[n.lastRssi!==undefined?`RSSI ${n.lastRssi} dBm`:"",n.lastSnr!==undefined?`SNR ${Number(n.lastSnr).toFixed(1)} dB`:"",n.lastHops!==undefined?`${n.lastHops} пер.`:""].filter(Boolean).join(" · ");
    card.append(head,meta);if(signal.textContent)card.append(signal);
    card.addEventListener("click",()=>openNode(n.num)); list.append(card);
  }
  if(!values.length) list.innerHTML='<p class="muted">Совпадений нет. Ноды появляются после их пакетов или при подключении к плате.</p>';
}

function renderAir(){
  const q=$<HTMLInputElement>("air-search")?.value.trim().toLowerCase()||"",kind=$<HTMLSelectElement>("air-kind")?.value||"";
  const filtered=airEvents.filter(e=>(!kind||e.kind===kind)&&(!q||`${e.kind} ${e.id} ${hex(e.from)} ${short(hex(e.from))} ${nodeName(e.from)}`.toLowerCase().includes(q))).slice().reverse();
  $("air-count").textContent=`(${airEvents.length})`;
  const heard=new Set(airEvents.map(e=>e.from).filter(Boolean)).size,withSignal=airEvents.filter(e=>e.rssi!==undefined&&e.rssi!==0),avg=withSignal.length?withSignal.reduce((s,e)=>s+Number(e.rssi),0)/withSignal.length:undefined;
  const summary=$("air-summary");summary.replaceChildren(...[["Пакетов",airEvents.length],["Нод",heard],["Средний RSSI",avg===undefined?"—":`${avg.toFixed(1)} dBm`],["Последний",airEvents.length?fmtTime(airEvents.at(-1)!.ts):"—"]].map(([a,b])=>{const el=document.createElement("div");el.className="metric";el.append(Object.assign(document.createElement("span"),{textContent:String(a)}),Object.assign(document.createElement("b"),{textContent:String(b)}));return el}));
  const list=$("air-list");list.replaceChildren();
  for(const e of filtered.slice(0,200)){
    const card=document.createElement("article");card.className="card air-event";
    const left=document.createElement("div");left.append(Object.assign(document.createElement("strong"),{textContent:e.kind}),document.createElement("br"),Object.assign(document.createElement("span"),{className:"meta",textContent:`${fmtTime(e.ts)} · #${e.id??"—"}`}));
    const right=document.createElement("div");const from=document.createElement("button");from.className="node-button";from.textContent=`${nodeName(e.from)} (${short(hex(e.from))})`;from.addEventListener("click",()=>openNode(e.from));right.append(from,document.createElement("br"),Object.assign(document.createElement("span"),{className:"meta signal",textContent:[`канал ${e.channel}`,e.rssi?`RSSI ${e.rssi} dBm`:"",e.snr?`SNR ${Number(e.snr).toFixed(1)} dB`:"",e.hops!==undefined?`${e.hops} переходов`:"",e.viaMqtt?"MQTT":e.from===myNode?"локально":"LoRa"].filter(Boolean).join(" · ")}));
    card.append(left,right);list.append(card);
  }
  if(!filtered.length)list.innerHTML='<p class="muted">Подходящих пакетов пока нет.</p>';
}

function drawAirtime(){
  const canvas=$<HTMLCanvasElement>("air-chart"),rect=canvas.getBoundingClientRect(),scale=devicePixelRatio||1;canvas.width=Math.max(300,rect.width*scale);canvas.height=180*scale;const c=canvas.getContext("2d");if(!c)return;c.scale(scale,scale);const w=canvas.width/scale,h=180;c.clearRect(0,0,w,h);c.strokeStyle="#26344c";for(let y=0;y<=4;y++){c.beginPath();c.moveTo(0,y*h/4);c.lineTo(w,y*h/4);c.stroke()}if(airtime.length<2)return;const draw=(key:"channel"|"tx",color:string)=>{c.strokeStyle=color;c.lineWidth=2;c.beginPath();airtime.forEach((s,i)=>{const x=i*w/(airtime.length-1),y=h-Math.min(100,s[key])*h/100;i?c.lineTo(x,y):c.moveTo(x,y)});c.stroke()};draw("channel","#61e7a5");draw("tx","#ffb454");
}

function renderChannels(){
  const select=$<HTMLSelectElement>("channel"),current=select.value; select.replaceChildren();
  for(const c of [...channels.values()].sort((a,b)=>a.index-b.index)){
    if(c.role===0&&c.index!==0)continue;
    const option=document.createElement("option"); option.value=String(c.index); option.textContent=`${c.index} · ${c.settings?.name||(c.index===0?"Первичный":`Канал ${c.index}`)}`; select.append(option);
  }
  if([...select.options].some(o=>o.value===current))select.value=current;
  renderMessageChannelTabs();updateDestination();
}

function positionText(p:AnyRecord) {
  const lat=(p.latitudeI??0)/1e7, lon=(p.longitudeI??0)/1e7;
  return lat||lon ? `${lat.toFixed(6)}, ${lon.toFixed(6)}\nhttps://www.openstreetmap.org/?mlat=${lat}&mlon=${lon}#map=15/${lat}/${lon}` : "Позиция скрыта или координаты отсутствуют";
}
function openNode(num:number) {
  selectedNode=num>>>0; const n=nodes.get(selectedNode)||{num:selectedNode};
  const box=$("node-details"); box.replaceChildren();
  const title=document.createElement("h2"); title.textContent=nodeName(selectedNode);
  const pre=document.createElement("pre"); pre.className="result"; pre.textContent=json({...n,num:selectedNode,hex:hex(selectedNode),shortId:short(hex(selectedNode))});
  const actions=document.createElement("div"); actions.className="actions";
  for(const [label,action] of [["Личное сообщение",()=>setDirect(selectedNode)],["Запросить позицию",()=>requestPosition(selectedNode)],["Трассировка",()=>traceRoute(selectedNode)]] as const) {
    const b=document.createElement("button"); b.textContent=label; b.addEventListener("click",action); actions.append(b);
  }
  const result=document.createElement("div"); result.id="node-action-result"; result.className="result muted"; result.textContent="Ответы на запросы появятся здесь.";
  box.append(title,pre,actions,result); ($<HTMLDialogElement>("node-dialog")).showModal();
}
function updateDestination(){
  const channel=Number($<HTMLSelectElement>("channel").value)||0,guidance=$("send-guidance"),send=$<HTMLButtonElement>("send-button");
  if(selectedNode===undefined){
    $("destination").textContent=`Широковещательно · ${channelName(channel)}`;guidance.hidden=true;guidance.textContent="";guidance.className="send-guidance";send.disabled=false;send.textContent="Отправить";return;
  }
  const keyKnown=hasUsablePublicKey(selectedNode),name=`${nodeName(selectedNode)} (${short(hex(selectedNode))})`;
  $("destination").textContent=`Лично: ${name} · ${channelName(channel)}`;
  guidance.hidden=false;
  guidance.className=`send-guidance ${keyKnown?directContext?"warn":"ok":"bad"}`;
  guidance.textContent=[directContext,keyKnown?"Публичный ключ получателя известен: личная отправка доступна.":`Личная отправка заблокирована: у платы нет 32-байтного публичного ключа ${name}. Без него возникнет ошибка 39, а пакет не выйдет в эфир. Дождитесь свежего NodeInfo или вернитесь в общий канал.`].filter(Boolean).join("\n");
  send.disabled=!keyKnown;send.textContent=keyKnown?"Отправить лично":"Нет публичного ключа";
}
function setDirect(num:number,context="") { selectedNode=num;directContext=context;updateDestination();$<HTMLButtonElement>("broadcast").hidden=false;($<HTMLDialogElement>("node-dialog")).close();$<HTMLTextAreaElement>("message").focus()}
function setBroadcast(){selectedNode=undefined;directContext="";updateDestination();$<HTMLButtonElement>("broadcast").hidden=true}
function setAction(text:string){ const e=document.getElementById("node-action-result"); if(e)e.textContent=text; }
async function requestPosition(num:number){if(!device)return setAction("Плата ещё не подключена");setAction(`Запрос позиции отправляется ${nodeName(num)}…`);try{const id=await device.requestPosition(num);setAction(`Запрос #${id} отправлен. Ждём ответ по LoRa; это может занять минуты. Ответ появится здесь.`)}catch(e){setAction(`Ошибка: ${errorText(e)}`)}}
async function traceRoute(num:number){if(!device)return setAction("Плата ещё не подключена");setAction(`Запрос трассировки отправляется ${nodeName(num)}…`);try{const id=await device.traceRoute(num);setAction(`Запрос #${id} отправлен. Ждём маршрут по LoRa; если нода недоступна, ответа может не быть.`)}catch(e){setAction(`Ошибка: ${errorText(e)}`)}}

async function loadStatus(){
  try{
    const r=await fetchJson("/json/report"),d=r.data||r,channel=Number(d.airtime?.channel_utilization)||0,tx=Number(d.airtime?.utilization_tx)||0;
    const metrics=[["Wi‑Fi",`${d.wifi?.rssi??"—"} dBm`],["LoRa",`${d.radio?.frequency?.toFixed?.(3)??"—"} MHz`],["Эфир занят",`${channel.toFixed(1)}%`],["Передача",`${tx.toFixed(2)}%`],["Работает",`${Math.floor((d.airtime?.seconds_since_boot||0)/3600)} ч`],["Архив свободно",`${Math.round((d.memory?.fs_free||0)/1024)} КБ`]];
    const grid=$("status-grid");grid.replaceChildren(...metrics.map(([a,b])=>{const e=document.createElement("div"),label=document.createElement("span"),value=document.createElement("b");e.className="metric";label.textContent=String(a);value.textContent=String(b);e.append(label,value);return e}));
    const last=airtime.at(-1);if(!last||Date.now()/1000-last.ts>20){airtime.push({ts:Math.floor(Date.now()/1000),channel,tx});airtime=airtime.slice(-120);localStorage.setItem("meshtastic-airtime",JSON.stringify(airtime))}drawAirtime();
  }catch(e){$("status-grid").textContent=`Нет данных: ${errorText(e)}`}
}

async function loadDualBootStatus(){
  const box=$("dualboot-status"),button=$<HTMLButtonElement>("boot-rnode"),portable=$<HTMLElement>("portable-controls"),home=$<HTMLButtonElement>("boot-home");
  try{
    const status=await fetchJson("/dualboot/status");
    if(status.portable_ap){
      box.textContent=`Портативный Meshtastic AP включён: ${status.portable_ssid} · ${status.portable_ip}`;
      portable.hidden=true;home.hidden=false;
    }else{
      box.textContent="Meshtastic подключён к домашней Wi‑Fi сети.";
      portable.hidden=false;home.hidden=true;
    }
    if(status.rnode_installed){
      box.textContent+=` RNode готов: ${status.version||"образ найден"}, раздел ${status.partition||"app1"}.`;
      box.className="result";button.disabled=Boolean(status.portable_ap);
    }else{
      box.textContent+=" Образ RNode во втором разделе не найден; загрузите его по Wi‑Fi ниже.";
      box.className="result muted";button.disabled=true;
    }
  }catch(e){
    box.textContent=`Эта версия Meshtastic ещё не поддерживает dual‑boot: ${errorText(e)}`;
    box.className="result muted";button.disabled=true;
  }
}

async function bootPortable(){
  const password=$<HTMLInputElement>("portable-password").value,result=$("dualboot-result"),button=$<HTMLButtonElement>("boot-portable");
  if(password.length<8||password.length>63)return void(result.textContent="Пароль должен содержать от 8 до 63 символов.");
  if(!/^[\x20-\x7e]+$/.test(password))return void(result.textContent="Используйте латинские буквы, цифры и обычные печатные знаки.");
  if(!confirm("Отключить плату от домашней сети и включить портативную точку BarbieNode-Portable?"))return;
  button.disabled=true;result.textContent="Сохраняю пароль и включаю точку доступа…";
  try{
    const response=await fetch("/dualboot/ap",{method:"POST",headers:{"Content-Type":"text/plain"},body:password}),text=await response.text();
    if(!response.ok)throw new Error(text||`${response.status}`);
    result.textContent=text;
  }catch(e){result.textContent=`Ошибка: ${errorText(e)}`;button.disabled=false}
}

async function bootHome(){
  if(!confirm("Отключить портативную точку и вернуться в сохранённую домашнюю Wi‑Fi сеть?"))return;
  const button=$<HTMLButtonElement>("boot-home"),result=$("dualboot-result");button.disabled=true;result.textContent="Возвращаю домашний Wi‑Fi…";
  try{
    const response=await fetch("/dualboot/home",{method:"POST"}),text=await response.text();
    if(!response.ok)throw new Error(text||`${response.status}`);
    result.textContent=text;
  }catch(e){result.textContent=`Ошибка: ${errorText(e)}`;button.disabled=false}
}

async function bootRNode(){
  if(!confirm("Остановить Meshtastic и перезагрузить плату в режим RNode / Reticulum?"))return;
  const button=$<HTMLButtonElement>("boot-rnode"),result=$("dualboot-result");
  button.disabled=true;result.textContent="Сохраняю Wi‑Fi и переключаю раздел…";
  try{
    const response=await fetch("/dualboot/rnode",{method:"POST"}),text=await response.text();
    if(!response.ok)throw new Error(text||`${response.status}`);
    result.textContent=text||"Плата перезагружается. Откройте этот же адрес через 10–20 секунд.";
  }catch(e){result.textContent=`Ошибка: ${errorText(e)}`;button.disabled=false}
}

function sha256Hex(input:ArrayBuffer){
  const source=new Uint8Array(input),paddedLength=Math.ceil((source.length+9)/64)*64,padded=new Uint8Array(paddedLength),view=new DataView(padded.buffer);
  padded.set(source);padded[source.length]=0x80;const bits=source.length*8;view.setUint32(paddedLength-8,Math.floor(bits/0x100000000));view.setUint32(paddedLength-4,bits>>>0);
  const constants=new Uint32Array([0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2]),words=new Uint32Array(64),hash=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);
  const rotate=(value:number,count:number)=>(value>>>count)|(value<<(32-count));
  for(let offset=0;offset<paddedLength;offset+=64){for(let i=0;i<16;i++)words[i]=view.getUint32(offset+i*4);for(let i=16;i<64;i++){const a=words[i-15],b=words[i-2],s0=rotate(a,7)^rotate(a,18)^(a>>>3),s1=rotate(b,17)^rotate(b,19)^(b>>>10);words[i]=(words[i-16]+s0+words[i-7]+s1)>>>0}let [a,b,c,d,e,f,g,h]=hash;for(let i=0;i<64;i++){const s1=rotate(e,6)^rotate(e,11)^rotate(e,25),choice=(e&f)^(~e&g),t1=(h+s1+choice+constants[i]+words[i])>>>0,s0=rotate(a,2)^rotate(a,13)^rotate(a,22),majority=(a&b)^(a&c)^(b&c),t2=(s0+majority)>>>0;h=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0}hash[0]=(hash[0]+a)>>>0;hash[1]=(hash[1]+b)>>>0;hash[2]=(hash[2]+c)>>>0;hash[3]=(hash[3]+d)>>>0;hash[4]=(hash[4]+e)>>>0;hash[5]=(hash[5]+f)>>>0;hash[6]=(hash[6]+g)>>>0;hash[7]=(hash[7]+h)>>>0}
  return [...hash].map(value=>value.toString(16).padStart(8,"0")).join("");
}

async function uploadRNodeFirmware(){
  const input=$<HTMLInputElement>("rnode-firmware"),button=$<HTMLButtonElement>("upload-rnode"),result=$("firmware-result"),file=input.files?.[0];
  if(!file)return void(result.textContent="Выберите .bin файл RNode.");
  if(!confirm("Записать выбранный образ RNode в неактивный раздел app1? Meshtastic останется активным."))return;
  button.disabled=true;result.textContent="Считаю SHA‑256…";
  try{
    const sha=sha256Hex(await file.arrayBuffer());
    result.textContent="Загружаю и проверяю образ…";
    const response=await fetch("/dualboot/update/rnode",{method:"POST",headers:{"Content-Type":"application/octet-stream","X-Firmware-SHA256":sha,"X-Firmware-Target":"rnode-app1"},body:file});
    const message=await response.text();
    if(!response.ok)throw new Error(message||`${response.status}`);
    result.textContent=message;await loadDualBootStatus();
  }catch(error){result.textContent=`Ошибка: ${errorText(error)}`}
  button.disabled=false;
}

async function connect(){
  try{
    const transport=await TransportHTTP.create(location.host,location.protocol==="https:");
    device=new MeshDevice(transport,Math.floor(Math.random()*0xffffffff));
    device.events.onDeviceStatus.subscribe(s=>statusPill(s===DeviceStatusEnum.DeviceConfigured?"плата подключена":"подключение…",s===DeviceStatusEnum.DeviceConfigured?"ok":"warn"));
    device.events.onMyNodeInfo.subscribe(info=>{statusPill("плата подключена","ok");myNode=info.myNodeNum>>>0;if(!nodes.get(myNode)?.user)addNode(myNode,{user:fallbackOwner});renderOwnIdentity();renderMessages();renderNodes();scheduleMap()});
    device.events.onNodeInfoPacket.subscribe(info=>{const n=info as AnyRecord;const num=(n.num??n.nodeNum)>>>0;if(num){addNode(num,n);if(selectedNode===num)updateDestination();if(isOwnNode(num)&&n.user){ownerConfig=n.user;device!.meshClient.config.editor.setBaselineOwner(n.user);renderOwnIdentity();refreshSettingsOptions()}if(!initialNodeSync){scheduleNodeRender();scheduleNodeCacheSave()}}});
    device.events.onChannelPacket.subscribe(info=>{const c=info as AnyRecord;channels.set(Number(c.index),c);renderChannels();refreshSettingsOptions()});
    device.events.onConfigPacket.subscribe(info=>{const c=info as AnyRecord,key=c.payloadVariant?.case,value=c.payloadVariant?.value;if(key&&value){radioConfigs.set(key,value);defaultRadioConfigs.delete(key);refreshSettingsOptions()}});
    device.events.onModuleConfigPacket.subscribe(info=>{const c=info as AnyRecord,key=c.payloadVariant?.case,value=c.payloadVariant?.value;if(key&&value){moduleConfigs.set(key,value);defaultModuleConfigs.delete(key);refreshSettingsOptions()}});
    device.events.onMeshPacket.subscribe(packet=>recordPacket(packet as AnyRecord));
    device.events.onMessagePacket.subscribe(packet=>{const p=packet as AnyRecord,m:Message={ts:Math.floor(new Date(p.rxTime).getTime()/1000)||Math.floor(Date.now()/1000),event:p.from===myNode?"tx":"rx",from:hex(p.from),to:p.type==="broadcast"?"^all":hex(p.to),channel:Number(p.channel)||0,text:String(p.data),id:p.id,source:"эфир"};messages.push(m);messages=messages.slice(-MAX_BROWSER_MESSAGES);if(m.event==="rx")markUnread(m.channel);addNode(p.from,{lastHeard:Math.floor(Date.now()/1000)});renderMessageChannelTabs();renderMessages();renderNodes();if(messagesViewActive()&&(selectedMessageChannel==="all"||selectedMessageChannel===m.channel))void acknowledgeViewedMessages()});
    device.events.onPositionPacket.subscribe(packet=>{const p=packet as AnyRecord;addNode(p.from,{position:p.data,lastHeard:Math.floor(Date.now()/1000)});if(selectedNode===p.from)setAction(`Позиция получена:\n${positionText(p.data)}`);renderNodes();scheduleMap()});
    device.events.onNeighborInfoPacket.subscribe(packet=>{const p=packet as AnyRecord,source=Number(p.data?.nodeId||p.from)>>>0;neighborInfos.set(source,p.data);localStorage.setItem("meshtastic-neighbors",JSON.stringify([...neighborInfos]));scheduleMap()});
    device.events.onTraceRoutePacket.subscribe(packet=>{const p=packet as AnyRecord;if(selectedNode===p.from){const route=(p.data?.route||[]).map((n:number,i:number)=>`${i+1}. ${nodeName(n)} (${short(hex(n))})`).join("\n");setAction(`Трассировка получена:\n${route||"Прямое соединение без промежуточных нод"}\n\nПолные данные:\n${json(p.data)}`)}});
    await device.configure();initialNodeSync=false;
    myNode=device.meshClient.myNodeNum>>>0;
    if(myNode){if(!nodes.get(myNode)?.user)addNode(myNode,{user:fallbackOwner});const owner=nodes.get(myNode)?.user;if(owner){ownerConfig=owner;device.meshClient.config.editor.setBaselineOwner(owner)}renderOwnIdentity();renderMessages();renderNodes();scheduleMap();refreshSettingsOptions();void saveNodeCache()}
    hydrateSettingsFromEditor();
    statusPill("плата подключена","ok");
  }catch(e){initialNodeSync=false;statusPill("нет API платы","bad");$("send-result").textContent=`Подключение: ${errorText(e)}`}
}

async function sendMessage(event:SubmitEvent){
  event.preventDefault();const field=$<HTMLTextAreaElement>("message"),text=field.value.trim();if(!text||!device)return;
  const result=$("send-result"),direct=selectedNode!==undefined;
  if(direct&&!hasUsablePublicKey(selectedNode!)){updateDestination();result.className="send-feedback bad";result.textContent="Отправка остановлена до эфира: публичный ключ получателя неизвестен. Выберите «Вернуться в общий» или дождитесь свежего NodeInfo.";return}
  result.className="send-feedback muted";result.textContent="Отправка…";
  try{
    const target=selectedNode,id=await device.sendText(text,direct?target:"broadcast",direct,Number(($<HTMLSelectElement>("channel")).value));
    const m:Message={ts:Math.floor(Date.now()/1000),event:"tx",from:myNode?hex(myNode):"self",to:direct?hex(selectedNode!):"^all",channel:Number(($<HTMLSelectElement>("channel")).value),text,id,source:"этот браузер"};
    messages.push(m);const sent=messages.filter(x=>x.event==="tx").slice(-200);localStorage.setItem("meshtastic-esp-sent",JSON.stringify(sent));renderMessages();field.value="";$("chars").textContent="0/200";
    let saved=true;try{await saveSentArchive()}catch{saved=false}
    result.className="send-feedback ok";result.textContent=(direct?`Пакет #${id} передан плате для личной доставки; ждём подтверждение ноды.`:`Пакет #${id} принят вашей платой. Для общего канала доставка получателям не подтверждается.`)+(saved?" Сохранено на ESP.":" Сохранено только в этом браузере.");
  }catch(e){result.className="send-feedback bad";result.textContent=sendFailureText(e,selectedNode)}
}

document.querySelectorAll<HTMLButtonElement>(".tab").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll(".tab,.view").forEach(x=>x.classList.remove("active"));b.classList.add("active");$(b.dataset.tab!).classList.add("active");if(b.dataset.tab==="messages"){if(selectedMessageChannel==="all")unreadChannels.clear();else unreadChannels.delete(selectedMessageChannel);updateUnreadIndicators();void acknowledgeViewedMessages()}if(b.dataset.tab==="status")requestAnimationFrame(drawAirtime);if(b.dataset.tab==="map")scheduleMap();if(b.dataset.tab==="settings")refreshSettingsOptions()}));
$("refresh").addEventListener("click",()=>void Promise.all([loadArchive(),loadStatus(),loadNotificationStatus()]));
$("mark-read").addEventListener("click",()=>void acknowledgeViewedMessages());
$("node-search").addEventListener("input",renderNodes);
$("map-active").addEventListener("change",renderMap);
$("map-links").addEventListener("change",renderMap);
$("map-zoom-in").addEventListener("click",()=>{mapRadiusIndex=Math.max(0,mapRadiusIndex-1);renderMap()});
$("map-zoom-out").addEventListener("click",()=>{mapRadiusIndex=Math.min(mapRadii.length-1,mapRadiusIndex+1);renderMap()});
$("air-search").addEventListener("input",renderAir);
$("air-kind").addEventListener("change",renderAir);
$("clear-air").addEventListener("click",()=>{airEvents=[];localStorage.removeItem("meshtastic-air-events");renderAir()});
$("broadcast").addEventListener("click",setBroadcast);
$("channel").addEventListener("change",setBroadcast);
$("close-dialog").addEventListener("click",()=>($<HTMLDialogElement>("node-dialog")).close());
$("message").addEventListener("input",e=>$("chars").textContent=`${(e.target as HTMLTextAreaElement).value.length}/200`);
$("emoji-bar").addEventListener("click",event=>{
  const button=(event.target as HTMLElement).closest<HTMLButtonElement>("button[data-emoji]");if(!button)return;
  const field=$<HTMLTextAreaElement>("message"),emoji=button.dataset.emoji||"",start=field.selectionStart??field.value.length,end=field.selectionEnd??start;
  if(field.value.length-(end-start)+emoji.length>field.maxLength){$("send-result").textContent="Достигнут лимит 200 символов.";field.focus();return}
  field.setRangeText(emoji,start,end,"end");field.dispatchEvent(new Event("input",{bubbles:true}));field.focus();
});
$("send-form").addEventListener("submit",sendMessage);
$("settings-section").addEventListener("change",()=>{settingsEditing=false;loadSelectedSettings()});
$("settings-json").addEventListener("input",()=>{settingsEditing=true;$("settings-state").textContent="Есть несохранённые изменения."});
$("settings-reload").addEventListener("click",()=>{settingsEditing=false;loadSelectedSettings()});
$("settings-export").addEventListener("click",exportSettings);
$("settings-apply").addEventListener("click",()=>void applySettings());
$("boot-rnode").addEventListener("click",()=>void bootRNode());
$("boot-portable").addEventListener("click",()=>void bootPortable());
$("boot-home").addEventListener("click",()=>void bootHome());
$("upload-rnode").addEventListener("click",()=>void uploadRNodeFirmware());

const composer=$("send-form");
const reserveComposerSpace=()=>document.documentElement.style.setProperty("--composer-space",`${composer.getBoundingClientRect().height+28}px`);
new ResizeObserver(reserveComposerSpace).observe(composer);
reserveComposerSpace();
addEventListener("resize",()=>{drawAirtime();scheduleMap()});

renderAir();drawAirtime();renderMap();
void Promise.all([loadNodeCache(),loadArchive(),loadStatus(),loadDualBootStatus(),loadNotificationStatus(),loadOwnLocation(),connect()]);
setInterval(loadArchive,30000);setInterval(loadStatus,30000);setInterval(loadNotificationStatus,30000);
