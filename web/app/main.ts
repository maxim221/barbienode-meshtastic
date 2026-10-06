import { DeviceStatusEnum, MeshDevice, Protobuf } from "@meshtastic/sdk";
import { TransportHTTP } from "@meshtastic/transport-http";
import { create, toBinary } from "@bufbuild/protobuf";
import "./style.css";

type AnyRecord = Record<string, any>;
type Message = {ts:number; event:string; from:string; to:string; channel:number; rssi?:number; snr?:number; text:string; id?:number; source:string};
type DeliveryState = {ts:number;packetId:number;destination?:string;status:"accepted"|"delivered"|"failed";errorCode?:number;errorReason?:string;evidence?:string;from?:string};
type AirEvent = {ts:number; kind:string; from:number; to:number; channel:number; rssi?:number; snr?:number; hops?:number; hopStart?:number; hopLimit?:number; relayNode?:number; id?:number; viaMqtt?:boolean; wantAck?:boolean; wantResponse?:boolean};
type AirtimeSample = {ts:number; channel:number; tx:number};
type LinkQualityBin = {ts:number;packets:number;direct:number;nodes:number;rssi:number|null;snr:number|null;directRssi:number|null;directSnr:number|null};
type AimSample = {ts:number; heading:number; from:number; rssi:number; snr?:number};
type AimTrial = {id:number;start:number;end?:number;heading:number;packetCount:number;directCount:number;relayedCount:number;unknownCount:number;nodes:number[];rssis:number[];snrs:number[];pingSentAt?:number;pingPacketId?:number;pingReplies:number[]};
type MqttMessage = {id:string;ts:number;direction:"rx"|"tx";sender:string;senderId?:string;text:string;topic:string;source:"broker"|"meshtastic"|"onemesh-api"};
type RuntimeMode = "unknown"|"orangepi"|"board";

const $ = <T extends HTMLElement>(id:string) => document.getElementById(id) as T;
const nodes = new Map<number, AnyRecord>();
const channels = new Map<number, AnyRecord>();
const neighborInfos = new Map<number, AnyRecord>(JSON.parse(localStorage.getItem("meshtastic-neighbors")||"[]"));
const radioConfigs = new Map<string, AnyRecord>();
const moduleConfigs = new Map<string, AnyRecord>();
const defaultRadioConfigs = new Set<string>();
const defaultModuleConfigs = new Set<string>();
let messages: Message[] = [];
const deliveryStates=new Map<number,DeliveryState>();
const MAX_BROWSER_MESSAGES=1000;
const unreadChannels=new Set<number>();
const unreadDirectPeers=new Set<number>();
const archiveSeenKeys=new Set<string>();
const MAX_ARCHIVE_SEEN_KEYS=4000;
let selectedMessageChannel:number|"all"|"direct"="all";
let selectedDirectPeer:number|"all"="all";
let archiveInitialized=false;
let archiveClockAheadSeconds=0;
let archiveFutureTimestamps=0;
let clockSyncInFlight=false;
let clockSyncCompleted=false;
let clockSyncError="";
let lastClockSyncAt=0;
let clockArchiveCorrected=0;
let listenBeforeTalkEnabled:boolean|undefined;
let listenBeforeTalkAvailable=false;
let listenBeforeTalkSaving=false;
const AIR_JOURNAL_VERSION="3";
const AIM_MEASUREMENTS_VERSION=2;
const airJournalIsCurrent=localStorage.getItem("meshtastic-air-events-version")===AIR_JOURNAL_VERSION;
let airEvents: AirEvent[] = airJournalIsCurrent?dedupeAirEvents(JSON.parse(localStorage.getItem("meshtastic-air-events")||"[]")):[];
if(!airJournalIsCurrent){localStorage.removeItem("meshtastic-air-events");localStorage.setItem("meshtastic-air-events-version",AIR_JOURNAL_VERSION)}
const seenAirPacketIds=new Set(airEvents.map(airPacketIdentity).filter(Boolean));
const mqttPacketIds=new Set<string>();
let airtime: AirtimeSample[] = JSON.parse(localStorage.getItem("meshtastic-airtime")||"[]");
let linkQualityBins:LinkQualityBin[]=[];
let device: MeshDevice | undefined;
let selectedNode: number | undefined;
type NodeActionKind="position"|"trace";
const nodeActionState:Record<NodeActionKind,Map<number,string>>={position:new Map(),trace:new Map()};
let directContext = "";
let sendInFlight=false;
let myNode = 0;
let ownNodeNum = 0;
let mapRenderPending=false;
let nodeRenderTimer:number|undefined;
let nodeCacheTimer:number|undefined;
let initialNodeSync=true;
let livePacketCapture=false;
let livePacketCaptureTimer:number|undefined;
let ownFixedPosition: {lat:number;lon:number} | undefined;
let ownerConfig: AnyRecord | undefined;
let settingsEditing=false;
let connectionConfigured=false;
let connectionWarningTimer:number|undefined;
let aimTarget=Number(localStorage.getItem("meshtastic-aim-target"))>>>0||0;
let aimHeading=Number(localStorage.getItem("meshtastic-aim-heading"))||0;
let aimStartedAt=Math.floor(Date.now()/1000)-7200;
let compassActive=false;
const aimMeasurementsAreCurrent=Number(localStorage.getItem("meshtastic-aim-samples-version"))===AIM_MEASUREMENTS_VERSION;
let aimSamples:AimSample[]=aimMeasurementsAreCurrent?parseAimSamples(localStorage.getItem("meshtastic-aim-samples")):[];
if(!aimMeasurementsAreCurrent){localStorage.removeItem("meshtastic-aim-samples");localStorage.setItem("meshtastic-aim-samples-version",String(AIM_MEASUREMENTS_VERSION))}
let aimSamplesSaveTimer:number|undefined;
let aimTrials:AimTrial[]=parseAimTrials(localStorage.getItem("meshtastic-aim-trials"));
const fallbackOwner={longName:"BarbieNode 💅",shortName:"db8c"};
// Verified in this board's 2026-09-30 configuration backup. Used only when
// firmware omits DEVICE_CONFIG from the HTTP configuration stream.
const PROJECT_NODE_INFO_BROADCAST_SECS=10800;
function projectPositionConfig(fixedPosition:boolean){return create(Protobuf.Config.Config_PositionConfigSchema,{broadcastSmartMinimumDistance:100,broadcastSmartMinimumIntervalSecs:300,fixedPosition,gpsMode:Protobuf.Config.Config_PositionConfig_GpsMode.NOT_PRESENT,gpsUpdateInterval:120,positionBroadcastSecs:3600,positionBroadcastSmartEnabled:true,positionFlags:811})}
const mapRadii=[2.5,5,10,20,40,80];
let mapRadiusIndex=3;
let mqttMeshMessages:MqttMessage[]=(()=>{try{const value=JSON.parse(localStorage.getItem("barbienode-mqtt-mesh")||"[]");return Array.isArray(value)?value.slice(-300):[]}catch{return[]}})();
let mqttMessages:MqttMessage[]=[...mqttMeshMessages];
let mqttConnected=false;
let mqttLastSeenTs=Number(localStorage.getItem("barbienode-mqtt-last-seen"))||0;
let scheduledPings:{ts:number;id?:number}[]=[];
let runtimeMode:RuntimeMode="unknown";
const NODE_CACHE_STORAGE_KEY="barbienode-node-cache";
const OWN_LOCATION_STORAGE_KEY="barbienode-own-location";
const SAVED_FIXED_POSITION_STORAGE_KEY="barbienode-saved-fixed-position";
const hasBackend=()=>runtimeMode==="orangepi";

const MQTT_PROFILES:Record<string,{label:string;host:string;port:number;topic:string;username:string;password:string;tls:boolean;note:string}>={
  "onemesh-monitor":{label:"Москва · ONEmesh · без downlink",host:"mqtt.onemesh.ru",port:8883,topic:"msh/RU/MSK/2/json/MediumFast",username:"onemesh",password:"onecat",tls:true,note:"Основной режим ONEmesh. Сервер не выдаёт downlink этому логину."},
  "onemesh-zero":{label:"Москва · ONEmesh · zero-hop downlink",host:"mqtt.onemesh.ru",port:8883,topic:"msh/RU/MSK/2/json/MediumFast",username:"onemeshz",password:"onecat",tls:true,note:"Downlink разрешён с zero-hop policy: принятые через интернет пакеты не должны ретранслироваться по радио."},
  "onemesh-full":{label:"Москва · ONEmesh · полный downlink",host:"mqtt.onemesh.ru",port:8883,topic:"msh/RU/MSK/2/json/MediumFast",username:"onemeshd",password:"onecat",tls:true,note:"Полный downlink ONEmesh. Используйте осознанно: шлюзы с радиопередачей могут заметно загружать эфир."},
  "meshtastic-public":{label:"Официальный публичный Meshtastic",host:"mqtt.meshtastic.org",port:8883,topic:"msh/RU/2/json/MediumFast",username:"meshdev",password:"large4cats",tls:true,note:"Публичный брокер Meshtastic. Московское разделение ONEmesh здесь не применяется."},
};

const radioSchemas:Record<string,any>={device:Protobuf.Config.Config_DeviceConfigSchema,position:Protobuf.Config.Config_PositionConfigSchema,power:Protobuf.Config.Config_PowerConfigSchema,network:Protobuf.Config.Config_NetworkConfigSchema,display:Protobuf.Config.Config_DisplayConfigSchema,lora:Protobuf.Config.Config_LoRaConfigSchema,bluetooth:Protobuf.Config.Config_BluetoothConfigSchema,security:Protobuf.Config.Config_SecurityConfigSchema,sessionkey:Protobuf.Config.Config_SessionkeyConfigSchema};
const moduleSchemas:Record<string,any>={mqtt:Protobuf.ModuleConfig.ModuleConfig_MQTTConfigSchema,serial:Protobuf.ModuleConfig.ModuleConfig_SerialConfigSchema,externalNotification:Protobuf.ModuleConfig.ModuleConfig_ExternalNotificationConfigSchema,storeForward:Protobuf.ModuleConfig.ModuleConfig_StoreForwardConfigSchema,rangeTest:Protobuf.ModuleConfig.ModuleConfig_RangeTestConfigSchema,telemetry:Protobuf.ModuleConfig.ModuleConfig_TelemetryConfigSchema,cannedMessage:Protobuf.ModuleConfig.ModuleConfig_CannedMessageConfigSchema,audio:Protobuf.ModuleConfig.ModuleConfig_AudioConfigSchema,remoteHardware:Protobuf.ModuleConfig.ModuleConfig_RemoteHardwareConfigSchema,neighborInfo:Protobuf.ModuleConfig.ModuleConfig_NeighborInfoConfigSchema,ambientLighting:Protobuf.ModuleConfig.ModuleConfig_AmbientLightingConfigSchema,detectionSensor:Protobuf.ModuleConfig.ModuleConfig_DetectionSensorConfigSchema,paxcounter:Protobuf.ModuleConfig.ModuleConfig_PaxcounterConfigSchema,statusmessage:Protobuf.ModuleConfig.ModuleConfig_StatusMessageConfigSchema,trafficManagement:Protobuf.ModuleConfig.ModuleConfig_TrafficManagementConfigSchema,tak:Protobuf.ModuleConfig.ModuleConfig_TAKConfigSchema};
for(const [key,schema] of Object.entries(radioSchemas)){radioConfigs.set(key,create(schema));defaultRadioConfigs.add(key)}
for(const [key,schema] of Object.entries(moduleSchemas)){moduleConfigs.set(key,create(schema));defaultModuleConfigs.add(key)}
for(let index=0;index<8;index++)channels.set(index,create(Protobuf.Channel.ChannelSchema,{index,role:index===0?Protobuf.Channel.Channel_Role.PRIMARY:Protobuf.Channel.Channel_Role.DISABLED,settings:create(Protobuf.Channel.ChannelSettingsSchema,index===0?{psk:new Uint8Array([1])}:{})}));

const hex = (n:number) => `!${(n >>> 0).toString(16).padStart(8,"0")}`;
function airPacketIdentity(event:Pick<AirEvent,"from"|"id">){
  const id=Number(event.id),from=Number(event.from)>>>0;
  return from&&Number.isFinite(id)&&id!==0?`${from}|${id>>>0}`:"";
}
function dedupeAirEvents(events:unknown):AirEvent[]{
  if(!Array.isArray(events))return [];
  const seen=new Set<string>();
  return events.filter((event):event is AirEvent=>{
    if(!event||typeof event!=="object")return false;
    const key=airPacketIdentity(event as AirEvent);
    if(!key)return true;
    if(seen.has(key))return false;
    seen.add(key);return true;
  }).slice(-400);
}
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
const MESSAGE_TIME_FLOOR=946684800; // 2000-01-01; the board reports zero until its clock becomes valid after boot.
const hasValidMessageTime=(ts:number)=>Number.isFinite(ts)&&ts>=MESSAGE_TIME_FLOOR;
const fmtTime = (ts:number) => hasValidMessageTime(ts)?new Date(ts * 1000).toLocaleString("ru-RU", {day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit"}):"время неизвестно";
const json = (v:unknown) => JSON.stringify(v, (_k,x) => typeof x === "bigint" ? x.toString() : x, 2);
const routingErrors:Record<number,string>={1:"Маршрут к ноде неизвестен",2:"Получен отказ от промежуточной ноды",3:"Истекло время ожидания",4:"Нет подходящего радио-интерфейса",5:"Исчерпаны повторные передачи",6:"Канал недоступен",7:"Пакет слишком большой",8:"Нода получила запрос, но её сервис не ответил",9:"Превышен допустимый эфирный цикл",32:"Удалённая нода отклонила запрос",33:"Удалённая нода не разрешила запрос",34:"Не удалось использовать PKI",35:"У принимающей ноды нет публичного ключа отправителя",36:"Сессия администрирования недействительна",37:"Публичный ключ не разрешён для администрирования",38:"Превышен лимит частоты пакетов",39:"Нет публичного ключа ноды-получателя"};
const routingActions:Record<number,string>={1:"Дождитесь свежего пакета от ноды и повторите позднее либо используйте общий канал.",2:"Не повторяйте сразу: промежуточная нода отказалась пересылать пакет.",3:"Проверьте доступность ноды и попробуйте позднее.",4:"Проверьте, что LoRa включена и регион настроен.",5:"Не повторяйте сразу: пакет мог выйти в эфир, но подтверждение не вернулось.",6:"Выберите включённый канал, общий с получателем.",7:"Сократите текст сообщения.",8:"Получатель доступен, но запрошенная функция у него не работает.",9:"Подождите освобождения лимита эфирного времени.",32:"Проверьте тип запроса и совместимость прошивки удалённой ноды.",33:"Используйте канал и ключ, разрешённые удалённой нодой.",34:"Обновите NodeInfo/ключи обеих нод и попробуйте позднее.",35:"Получатель должен сначала получить свежий NodeInfo вашей ноды.",36:"Повторно подключитесь к плате и создайте новую административную сессию.",37:"Этот ключ не входит в список администраторов удалённой ноды.",38:"Подождите перед следующей попыткой.",39:"Дождитесь свежего NodeInfo получателя либо вернитесь в общий канал."};
const errorText = (e:unknown) => {
  if(e&&typeof e==="object"&&"error" in e){const x=e as AnyRecord;return `${routingErrors[Number(x.error)]||`Ошибка маршрутизации ${x.error}`} (пакет #${x.id??"—"})`}
  return e instanceof Error ? e.message : typeof e === "string" ? e : json(e);
};
const statusPill = (text:string, cls:string) => { const el=$("connection"); el.textContent=text; el.className=`pill ${cls}`; };
function clearConnectionWarning(){if(connectionWarningTimer!==undefined){window.clearTimeout(connectionWarningTimer);connectionWarningTimer=undefined}}
function markConnectionConfigured(){connectionConfigured=true;clearConnectionWarning();statusPill("плата подключена","ok")}
function updateConnectionStatus(status:DeviceStatusEnum){
  if(status===DeviceStatusEnum.DeviceConfigured){markConnectionConfigured();return}
  if(status===DeviceStatusEnum.DeviceConnected){clearConnectionWarning();statusPill(connectionConfigured?"плата подключена":"подключение…",connectionConfigured?"ok":"warn");return}
  if(status===DeviceStatusEnum.DeviceRestarting){connectionConfigured=false;clearConnectionWarning();statusPill("плата перезагружается…","warn");return}
  if(status===DeviceStatusEnum.DeviceError){connectionConfigured=false;clearConnectionWarning();statusPill("ошибка подключения","bad");return}
  if(status===DeviceStatusEnum.DeviceDisconnected){
    clearConnectionWarning();
    connectionWarningTimer=window.setTimeout(()=>statusPill(connectionConfigured?"связь с платой потеряна":"нет подключения к плате","bad"),15000);
    return;
  }
  if(!connectionConfigured)statusPill("подключение…","warn");
}
const fallbackChannelNames:Record<number,string>={0:"Первичный",1:"BarbiePriv",2:"SVAO",3:"Ping"};
const channelName=(index:number)=>channels.get(index)?.settings?.name||fallbackChannelNames[index]||`Канал ${index}`;
const messageKey=(m:Message)=>`${m.ts}|${m.from}|${m.to}|${m.channel}|${m.text}`;
const isLocalOutgoing=(m:Message)=>m.event==="tx"&&(m.source==="этот браузер"||m.source==="исходящие ESP");
const messageOrder=(m:Message)=>m.ts+(isLocalOutgoing(m)?archiveClockAheadSeconds:0);
const messageIdentity=(m:Message)=>Number.isFinite(Number(m.id))&&Number(m.id)!==0?`${m.event}|${m.from}|${m.channel}|packet:${Number(m.id)>>>0}`:messageKey(m);
function mergeMessageCopies(rows:Message[]){
  const merged:Message[]=[];
  for(const message of rows.slice().sort((a,b)=>messageOrder(a)-messageOrder(b))){
    const packetId=Number(message.id)>>>0;
    const duplicate=merged.slice().reverse().find(existing=>{
      if(message.event!=="tx"||existing.event!=="tx")return false;
      const existingId=Number(existing.id)>>>0;
      if(packetId&&existingId)return packetId===existingId;
      const archiveCopies=new Set(["этот браузер","исходящие ESP","исходящие Orange Pi"]);
      return archiveCopies.has(message.source)&&archiveCopies.has(existing.source)&&message.source!==existing.source&&message.to===existing.to&&message.channel===existing.channel&&message.text===existing.text&&Math.abs(message.ts-existing.ts)<=120;
    });
    if(!duplicate){merged.push(message);continue}
    if(packetId&&!(Number(duplicate.id)>>>0))Object.assign(duplicate,message);
  }
  return merged;
}
function rememberArchiveMessage(m:Message){
  const key=messageIdentity(m);
  if(archiveSeenKeys.has(key))return;
  archiveSeenKeys.add(key);
  while(archiveSeenKeys.size>MAX_ARCHIVE_SEEN_KEYS)archiveSeenKeys.delete(archiveSeenKeys.values().next().value!);
}
const durationShort=(seconds:number)=>{const value=Math.abs(Math.round(seconds));if(value>=86400)return`${(value/86400).toFixed(1)} д`;if(value>=3600)return`${(value/3600).toFixed(1)} ч`;return`${Math.max(1,Math.round(value/60))} мин`};
const messagesViewActive=()=>$("messages").classList.contains("active");

type Addressing={label:string;kind:"direct"|"ours"|"other"|"broadcast"|"unknown";target?:string};
const normalizedNodeId=(value:string|undefined)=>/^![0-9a-f]{8}$/i.test(value||"")?value!.toLowerCase():undefined;
const ownNodeIds=()=>new Set([myNode,ownNodeNum].filter(Boolean).map(n=>hex(n).toLowerCase()));
const pongTarget=(text:string)=>text.match(/\bpong\b!?\s*(?:\[\s*)?(![0-9a-f]{8})(?:\s*\])?/i)?.[1]?.toLowerCase();
function replyTargetsOwn(text:string){const value=text.toLocaleLowerCase("ru-RU"),owner=ownerConfig||nodes.get((myNode||ownNodeNum)>>>0)?.user||fallbackOwner,ids=[...ownNodeIds()],aliases=[owner?.longName,owner?.shortName,fallbackOwner.longName,fallbackOwner.shortName,...ids,...ids.map(short)].map(item=>String(item||"").trim().toLocaleLowerCase("ru-RU")).filter(item=>item.length>=4);return aliases.some(alias=>value.includes(alias))}
const hasPingReplyCue=(text:string)=>/pong|п[рp]инял|\bto\b/i.test(text);
function messageAddressing(m:Message):Addressing{
  const ownIds=ownNodeIds(),from=normalizedNodeId(m.from),to=normalizedNodeId(m.to),target=pongTarget(m.text),isPong=/\bpong\b/i.test(m.text),isPing=m.text.trim().toLowerCase()==="ping"&&(channelName(m.channel).toLowerCase()==="ping"||m.channel===3);
  if(isPong){
    if(!target)return replyTargetsOwn(m.text)?{label:"Ответ на наш Ping · по имени",kind:"ours"}:{label:"Pong без указанного ID",kind:"unknown"};
    if(m.event==="tx"||(from!==undefined&&ownIds.has(from)))return {label:`Наш ответ на Ping · ${short(target)}`,kind:"ours",target};
    if(ownIds.has(target))return {label:"Ответ на наш Ping",kind:"ours",target};
    return {label:`Ответ на чужой Ping · ${short(target)}`,kind:"other",target};
  }
  if(isPing)return m.event==="tx"?{label:"Наш Ping",kind:"ours"}:{label:`Ping от ${short(m.from)}`,kind:"other",target:normalizedNodeId(m.from)};
  if(m.event!=="tx"&&(channelName(m.channel).toLowerCase()==="ping"||m.channel===3)&&hasPingReplyCue(m.text)&&replyTargetsOwn(m.text))return {label:"Ответ на наш Ping · по имени",kind:"ours"};
  if(to){
    if(m.event!=="tx"&&ownIds.has(to))return {label:"Лично вам",kind:"direct",target:to};
    return {label:`Лично → ${short(to)}`,kind:"direct",target:to};
  }
  return {label:"Всем в канале",kind:"broadcast"};
}

function directPeer(m:Message){
  if(messageAddressing(m).kind!=="direct")return 0;
  const from=normalizedNodeId(m.from),to=normalizedNodeId(m.to),ownIds=ownNodeIds();
  if(m.event==="tx"||(from!==undefined&&ownIds.has(from)))return to?Number.parseInt(to.slice(1),16)>>>0:0;
  if(to!==undefined&&ownIds.has(to))return from?Number.parseInt(from.slice(1),16)>>>0:0;
  return 0;
}
const isForeignDirect=(m:Message)=>messageAddressing(m).kind==="direct"&&directPeer(m)===0;

function updateUnreadIndicators(){
  $<HTMLElement>("messages-unread").hidden=unreadChannels.size===0;
  $<HTMLElement>("direct-messages-unread").hidden=unreadDirectPeers.size===0;
  document.querySelectorAll<HTMLElement>("#message-channels [data-channel]").forEach(button=>{
    const value=button.dataset.channel!,dot=button.querySelector<HTMLElement>(".unread-dot");
    if(dot)dot.hidden=value==="all"?unreadChannels.size===0&&unreadDirectPeers.size===0:value==="direct"?unreadDirectPeers.size===0:!unreadChannels.has(Number(value));
  });
  document.querySelectorAll<HTMLElement>("#message-directs [data-peer]").forEach(button=>{const value=button.dataset.peer!,dot=button.querySelector<HTMLElement>(".unread-dot");if(dot)dot.hidden=value==="all"?unreadDirectPeers.size===0:!unreadDirectPeers.has(Number(value))});
}
function markUnread(m:Message){
  if(isForeignDirect(m))return;
  const peer=directPeer(m);
  if(peer){if(!messagesViewActive()||selectedMessageChannel!=="direct"||(selectedDirectPeer!=="all"&&selectedDirectPeer!==peer))unreadDirectPeers.add(peer)}
  else if(!messagesViewActive()||(selectedMessageChannel!=="all"&&selectedMessageChannel!==m.channel))unreadChannels.add(m.channel);
  updateUnreadIndicators();
}
function selectMessageChannel(value:number|"all"|"direct"){
  selectedMessageChannel=value;
  if(messagesViewActive())document.querySelectorAll<HTMLElement>('.tab[data-tab="messages"]').forEach(button=>button.classList.toggle("active",value==="direct"?button.dataset.messageStream==="direct":button.dataset.messageStream==="all"));
  if(value==="all"){unreadChannels.clear();unreadDirectPeers.clear();setBroadcast()}else if(value==="direct"){if(selectedDirectPeer==="all")unreadDirectPeers.clear();else unreadDirectPeers.delete(selectedDirectPeer)}else unreadChannels.delete(value);
  if(typeof value==="number"){
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
  const entries:[number|"all"|"direct",string][]=[["all","Каналы"],["direct","Личные"],...[...indexes].sort((a,b)=>a-b).map(i=>[i,channelName(i)] as [number,string])];
  box.replaceChildren(...entries.map(([value,label])=>{
    const button=document.createElement("button");button.type="button";button.className=`channel-tab${selectedMessageChannel===value?" active":""}`;button.dataset.channel=String(value);button.setAttribute("role","tab");button.setAttribute("aria-selected",String(selectedMessageChannel===value));
    button.append(document.createTextNode(label+" "),Object.assign(document.createElement("span"),{className:"unread-dot",hidden:value==="all"?unreadChannels.size===0&&unreadDirectPeers.size===0:value==="direct"?unreadDirectPeers.size===0:!unreadChannels.has(Number(value))}));
    button.addEventListener("click",()=>selectMessageChannel(value));return button;
  }));
  renderDirectTabs();
  $("message-filter-label").textContent=selectedMessageChannel==="all"?"Показаны только сообщения каналов":selectedMessageChannel==="direct"?(selectedDirectPeer==="all"?"Все личные диалоги":`Диалог с ${nodeName(selectedDirectPeer)}`):`Показан канал: ${channelName(selectedMessageChannel)}`;
}

function renderDirectTabs(){
  const box=$("message-directs");box.hidden=selectedMessageChannel!=="direct";
  const picker=$<HTMLLabelElement>("direct-recipient-picker"),select=$<HTMLSelectElement>("direct-recipient");picker.hidden=box.hidden;
  if(box.hidden)return;
  const nodeOptions=[...nodes.values()].filter(node=>Number(node.num)&&!isOwnNode(Number(node.num))).sort((a,b)=>Number(hasUsablePublicKey(b.num))-Number(hasUsablePublicKey(a.num))||nodeName(a.num).localeCompare(nodeName(b.num),"ru"));
  select.replaceChildren(Object.assign(document.createElement("option"),{value:"",textContent:"Выберите ноду…"}),...nodeOptions.map(node=>Object.assign(document.createElement("option"),{value:String(Number(node.num)>>>0),textContent:`${hasUsablePublicKey(node.num)?"🔒":"—"} ${nodeName(node.num)} (${short(hex(node.num))})`})));select.value=selectedNode===undefined?"":String(selectedNode);
  const peers=new Map<number,number>();
  for(const message of messages){const peer=directPeer(message);if(peer)peers.set(peer,Math.max(peers.get(peer)||0,messageOrder(message)))}
  if(selectedNode!==undefined&&!peers.has(selectedNode))peers.set(selectedNode,0);
  const entries:[number|"all",string,number][]=[["all","Все личные",0],...[...peers].sort((a,b)=>b[1]-a[1]).map(([peer,ts])=>[peer,nodeName(peer),ts] as [number,string,number])];
  box.replaceChildren(...entries.map(([value,label,ts])=>{const button=document.createElement("button");button.type="button";button.className=`direct-tab${selectedDirectPeer===value?" active":""}`;button.dataset.peer=String(value);button.setAttribute("role","tab");button.append(document.createTextNode(label),...(ts?[Object.assign(document.createElement("small"),{textContent:fmtTime(ts)})]:[]),Object.assign(document.createElement("span"),{className:"unread-dot",hidden:value==="all"?unreadDirectPeers.size===0:!unreadDirectPeers.has(Number(value))}));button.addEventListener("click",()=>{selectedDirectPeer=value;if(value==="all"){unreadDirectPeers.clear();selectedNode=undefined;directContext=""}else{unreadDirectPeers.delete(value);selectedNode=value;directContext=""}renderMessageChannelTabs();renderMessages();updateDestination();updateUnreadIndicators();void acknowledgeViewedMessages()});return button}));
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
    const payload=hasBackend()?await fetchJson("/node-cache.json"):JSON.parse(localStorage.getItem(NODE_CACHE_STORAGE_KEY)||"{}");
    if(!Array.isArray(payload.nodes))return;
    for(const node of payload.nodes){const num=Number(node?.num)>>>0;if(num)addNode(num,cachedNode(node))}
    const savedOwnNode=Number(payload.ownNodeNum)>>>0;let recoveredOwnNode=false;
    if(!myNode&&!ownNodeNum){
      if(savedOwnNode)ownNodeNum=savedOwnNode;
      else{
        const matches=payload.nodes.filter((node:AnyRecord)=>{
          const num=Number(node?.num)>>>0,user=node?.user;
          return num&&normalizedNodeId(user?.id)===hex(num)&&user?.longName===fallbackOwner.longName&&user?.shortName===fallbackOwner.shortName;
        });
        if(matches.length===1){ownNodeNum=Number(matches[0].num)>>>0;recoveredOwnNode=true}
      }
    }
    if(ownNodeNum){const owner=nodes.get(ownNodeNum)?.user;if(owner)ownerConfig=owner}
    normalizeBrowserHistory(Math.floor(Date.now()/1000),0);
    renderOwnIdentity();renderMessages();renderNodes();renderAir();scheduleMap();
    if(recoveredOwnNode)void saveNodeCache();
  }catch{}
}

async function saveNodeCache(){
  const snapshot={savedAt:Math.floor(Date.now()/1000),ownNodeNum:(myNode||ownNodeNum)>>>0,nodes:[...nodes.values()].filter(node=>Number(node.num)).map(cachedNode)};
  if(!hasBackend()){localStorage.setItem(NODE_CACHE_STORAGE_KEY,JSON.stringify(snapshot));return}
  try{await fetch("/node-cache.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(snapshot)})}catch{}
}

function scheduleNodeCacheSave(){
  if(nodeCacheTimer!==undefined)window.clearTimeout(nodeCacheTimer);
  nodeCacheTimer=window.setTimeout(()=>{nodeCacheTimer=undefined;void saveNodeCache()},1500);
}

function parseAimSamples(raw:string|null|unknown):AimSample[]{
  try{
    const values=typeof raw==="string"?JSON.parse(raw):raw;
    if(!Array.isArray(values))return [];
    return values.flatMap((item:any)=>{
      const ts=Number(item?.ts),from=Number(item?.from)>>>0,rssi=Number(item?.rssi),snr=item?.snr===undefined?undefined:Number(item.snr),rawHeading=Number(item?.heading),heading=(rawHeading%360+360)%360;
      if(!Number.isFinite(ts)||!from||!Number.isFinite(rssi)||!Number.isFinite(heading)||rssi< -200||rssi>50||snr!==undefined&&!Number.isFinite(snr))return [];
      return [{ts:Math.floor(ts),heading,from,rssi,snr}];
    }).sort((a,b)=>a.ts-b.ts).slice(-2000);
  }catch{return []}
}
function parseAimTrials(raw:string|null):AimTrial[]{
  try{
    const values=JSON.parse(raw||"[]");if(!Array.isArray(values))return [];
    return values.flatMap((item:any)=>{const id=Number(item?.id),start=Number(item?.start),end=item?.end===undefined?undefined:Number(item.end),rawHeading=Number(item?.heading),heading=(rawHeading%360+360)%360;if(!Number.isFinite(id)||!Number.isFinite(start)||!Number.isFinite(heading)||end!==undefined&&!Number.isFinite(end))return[];return[{id,start:Math.floor(start),end:end===undefined?undefined:Math.floor(end),heading,packetCount:Math.max(0,Number(item.packetCount)||0),directCount:Math.max(0,Number(item.directCount)||0),relayedCount:Math.max(0,Number(item.relayedCount)||0),unknownCount:Math.max(0,Number(item.unknownCount)||0),nodes:Array.isArray(item.nodes)?item.nodes.map((n:any)=>Number(n)>>>0).filter(Boolean).slice(-200):[],rssis:Array.isArray(item.rssis)?item.rssis.map(Number).filter(Number.isFinite).slice(-200):[],snrs:Array.isArray(item.snrs)?item.snrs.map(Number).filter(Number.isFinite).slice(-200):[],pingSentAt:Number.isFinite(Number(item.pingSentAt))?Number(item.pingSentAt):undefined,pingPacketId:Number.isFinite(Number(item.pingPacketId))?Number(item.pingPacketId):undefined,pingReplies:Array.isArray(item.pingReplies)?item.pingReplies.map((n:any)=>Number(n)>>>0).filter(Boolean).slice(-50):[]}]}).sort((a,b)=>a.start-b.start).slice(-20);
  }catch{return[]}
}
function persistAimTrials(){aimTrials=aimTrials.slice(-20);localStorage.setItem("meshtastic-aim-trials",JSON.stringify(aimTrials))}
function activeAimTrial(){return aimTrials.slice().reverse().find(trial=>trial.end===undefined)}
function trialDuration(trial:AimTrial){return Math.max(0,(trial.end||Math.floor(Date.now()/1000))-trial.start)}
function formatDuration(seconds:number){return seconds<60?`${seconds} с`:`${Math.floor(seconds/60)} мин ${seconds%60} с`}
function renderAimTrials(){
  const body=document.getElementById("aim-trial-body");if(!body)return;const active=activeAimTrial(),start=$<HTMLButtonElement>("aim-trial-start"),stop=$<HTMLButtonElement>("aim-trial-stop"),ping=$<HTMLButtonElement>("aim-trial-ping"),state=$("aim-trial-state");
  start.disabled=!!active;stop.disabled=!active;ping.disabled=!active||!!active.pingSentAt;
  state.textContent=active?`Идёт замер ${Math.round(active.heading)}° ${cardinal(active.heading)} · ${formatDuration(trialDuration(active))}. Положение зафиксировано на момент старта.`:"Запустите замер после установки антенны и азимута. Для сравнения используйте одинаковую длительность.";
  body.replaceChildren();if(!aimTrials.length){body.append(Object.assign(document.createElement("p"),{className:"muted aim-comparison-empty",textContent:"Контрольных замеров пока нет."}));return}
  for(const trial of aimTrials.slice().reverse()){const line=document.createElement("div"),rssi=median(trial.rssis),snr=median(trial.snrs);line.className=`aim-trial-row${trial.end===undefined?" active":""}`;line.append(Object.assign(document.createElement("strong"),{textContent:`${Math.round(trial.heading)}° ${cardinal(trial.heading)}${trial.end===undefined?" · идёт":""}`}),Object.assign(document.createElement("span"),{textContent:formatDuration(trialDuration(trial))}),Object.assign(document.createElement("span"),{textContent:`${trial.packetCount} RF · ${trial.directCount} прямо · ${new Set(trial.nodes).size} нод`}),Object.assign(document.createElement("span"),{textContent:rssi===undefined?"RSSI —":`RSSI ${rssi.toFixed(1)} · SNR ${snr===undefined?"—":snr.toFixed(1)}`}),Object.assign(document.createElement("span"),{textContent:trial.pingSentAt?`Ping: ${new Set(trial.pingReplies).size} явных ответов`:"Ping не отправлялся"}));body.append(line)}
}
function startAimTrial(){if(activeAimTrial())return;aimTrials.push({id:Date.now(),start:Math.floor(Date.now()/1000),heading:normalizeDegrees(aimHeading),packetCount:0,directCount:0,relayedCount:0,unknownCount:0,nodes:[],rssis:[],snrs:[],pingReplies:[]});persistAimTrials();renderAimTrials()}
function stopAimTrial(){const trial=activeAimTrial();if(!trial)return;trial.end=Math.floor(Date.now()/1000);persistAimTrials();renderAimTrials()}
function recordAimTrialEvent(event:AirEvent){const trial=activeAimTrial();if(!trial||!event.from||isOwnNode(event.from)||event.viaMqtt)return;trial.packetCount++;if(event.hops===0)trial.directCount++;else if(event.hops===undefined)trial.unknownCount++;else trial.relayedCount++;if(!trial.nodes.includes(event.from))trial.nodes.push(event.from);if(event.rssi!==undefined)trial.rssis.push(Number(event.rssi));if(event.snr!==undefined)trial.snrs.push(Number(event.snr));trial.nodes=trial.nodes.slice(-200);trial.rssis=trial.rssis.slice(-200);trial.snrs=trial.snrs.slice(-200);persistAimTrials();renderAimTrials()}
function recordAimTrialReply(message:Message){const trial=activeAimTrial(),own=(myNode||ownNodeNum)>>>0;if(!trial?.pingSentAt||!own||message.event!=="rx"||message.ts<trial.pingSentAt||message.ts>trial.pingSentAt+600||!hasPingReplyCue(message.text)||!replyTargetsOwn(message.text))return;const from=Number.parseInt(message.from.replace("!",""),16)>>>0;if(from&&!trial.pingReplies.includes(from)){trial.pingReplies.push(from);persistAimTrials();renderAimTrials()}}
async function sendAimTrialPing(){
  const trial=activeAimTrial(),state=$("aim-trial-state");if(!trial||trial.pingSentAt)return;const pingChannel=[...channels.values()].find(channel=>channel.settings?.name==="Ping"&&Number(channel.role)!==0);if(!pingChannel){state.textContent="Канал с точным именем Ping не найден; передача отменена.";return}
  const button=$<HTMLButtonElement>("aim-trial-ping");button.disabled=true;state.textContent="Передаю один Ping…";
  try{const id=await sendLoRa("text","Ping","^all",Number(pingChannel.index));trial.pingSentAt=Math.floor(Date.now()/1000);trial.pingPacketId=id;persistAimTrials();const m:Message={ts:trial.pingSentAt,event:"tx",from:myNode?hex(myNode):"self",to:"^all",channel:Number(pingChannel.index),text:"Ping",id,source:"этот браузер"};messages.push(m);localStorage.setItem("meshtastic-esp-sent",JSON.stringify(messages.filter(x=>x.event==="tx").slice(-200)));renderMessages();try{await saveSentArchive()}catch{}state.textContent=`Ping #${id} принят платой. Повтор в этом замере заблокирован; ждём явные ответы.`}catch(error){state.textContent=`Ping не отправлен: ${errorText(error)}`}renderAimTrials();
}
function persistAimSamples(){aimSamples=aimSamples.filter(sample=>sample.ts>Date.now()/1000-86400).slice(-2000);localStorage.setItem("meshtastic-aim-samples",JSON.stringify(aimSamples))}
async function saveAimMeasurements(){persistAimSamples();if(!hasBackend())return;try{await fetch("/aim-measurements.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({version:AIM_MEASUREMENTS_VERSION,savedAt:Math.floor(Date.now()/1000),samples:aimSamples})})}catch{}}
function scheduleAimMeasurementsSave(){persistAimSamples();if(aimSamplesSaveTimer!==undefined)window.clearTimeout(aimSamplesSaveTimer);aimSamplesSaveTimer=window.setTimeout(()=>{aimSamplesSaveTimer=undefined;void saveAimMeasurements()},1000)}
function removeOwnAimSamples(){
  const own=(myNode||ownNodeNum)>>>0;if(!own)return;
  const filtered=aimSamples.filter(sample=>sample.from!==own);
  if(filtered.length!==aimSamples.length){aimSamples=filtered;scheduleAimMeasurementsSave();renderAimTracking()}
}
async function loadAimMeasurements(){
  if(!hasBackend()){persistAimSamples();renderAimTracking();return}
  try{
    const payload=await fetchJson("/aim-measurements.json"),remote=Number(payload.version)===AIM_MEASUREMENTS_VERSION?parseAimSamples(payload.samples):[],merged=new Map<string,AimSample>();
    for(const sample of [...remote,...aimSamples])merged.set(`${sample.ts}|${sample.heading}|${sample.from}|${sample.rssi}|${sample.snr??""}`,sample);
    aimSamples=[...merged.values()].sort((a,b)=>a.ts-b.ts).slice(-2000);persistAimSamples();renderAimTracking();
    if(Number(payload.version)!==AIM_MEASUREMENTS_VERSION)void saveAimMeasurements();
  }catch{}
}

function hasUsablePublicKey(num:number){
  const value=nodes.get(num>>>0)?.user?.publicKey;
  const bytes=value instanceof Uint8Array?value:Array.isArray(value)?Uint8Array.from(value):undefined;
  return !!bytes&&bytes.length===32&&bytes.some(byte=>byte!==0);
}
function publicKeyBytes(num:number){
  const value=nodes.get(num>>>0)?.user?.publicKey;
  const bytes=value instanceof Uint8Array?value:Array.isArray(value)?Uint8Array.from(value):undefined;
  return bytes&&bytes.length===32&&bytes.some(byte=>byte!==0)?bytes:undefined;
}
function publicKeyBase64(num:number){const bytes=publicKeyBytes(num);if(!bytes)return "";let value="";for(const byte of bytes)value+=String.fromCharCode(byte);return btoa(value)}
function publicKeyFingerprint(num:number){const bytes=publicKeyBytes(num);return bytes?`${[...bytes.slice(0,4)].map(byte=>byte.toString(16).padStart(2,"0")).join("")}…${[...bytes.slice(-4)].map(byte=>byte.toString(16).padStart(2,"0")).join("")}`:"—"}

function sendFailureText(error:unknown,target?:number){
  const packet=error&&typeof error==="object"&&"id" in error?`Пакет #${(error as AnyRecord).id??"—"}. `:"";
  const rawCode=error&&typeof error==="object"?(error as AnyRecord).errorCode??(typeof (error as AnyRecord).error==="number"?(error as AnyRecord).error:undefined):undefined,code=Number.isFinite(Number(rawCode))?Number(rawCode):undefined;
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

const normalizeDegrees=(value:number)=>(value%360+360)%360;
const signedAngle=(value:number)=>normalizeDegrees(value+180)-180;
function bearingAndDistance(from:{lat:number;lon:number},to:{lat:number;lon:number}){
  const rad=Math.PI/180,lat1=from.lat*rad,lat2=to.lat*rad,dLon=(to.lon-from.lon)*rad;
  const y=Math.sin(dLon)*Math.cos(lat2),x=Math.cos(lat1)*Math.sin(lat2)-Math.sin(lat1)*Math.cos(lat2)*Math.cos(dLon);
  const a=Math.sin((lat2-lat1)/2)**2+Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLon/2)**2;
  return {bearing:normalizeDegrees(Math.atan2(y,x)/rad),distance:6371*2*Math.atan2(Math.sqrt(a),Math.sqrt(Math.max(0,1-a)))};
}
const cardinal=(degrees:number)=>["С","СВ","В","ЮВ","Ю","ЮЗ","З","СЗ"][Math.round(normalizeDegrees(degrees)/45)%8];
function ownCoordinates(){const own=nodes.get((myNode||ownNodeNum)>>>0);return own?nodeCoordinates(own):ownFixedPosition?{...ownFixedPosition,source:"local-fixed"}:undefined}
type NodeSortKey="last-heard"|"name"|"hops"|"rssi"|"snr"|"distance";
function nodeSortKey(id:string,fallback:NodeSortKey="last-heard"){const value=$<HTMLSelectElement>(id)?.value as NodeSortKey;return (["last-heard","name","hops","rssi","snr","distance"] as NodeSortKey[]).includes(value)?value:fallback}
function nodeSortNumber(node:AnyRecord,key:NodeSortKey){if(key==="last-heard"){const value=Number(node.lastHeard);return Number.isFinite(value)?value:undefined}if(key==="hops"){const value=Number(node._sortHops??node.lastHops);return Number.isFinite(value)?value:undefined}if(key==="rssi"){const value=Number(node._sortRssi??node.lastRssi);return Number.isFinite(value)?value:undefined}if(key==="snr"){const value=Number(node._sortSnr??node.lastSnr);return Number.isFinite(value)?value:undefined}if(key==="distance"){const preset=Number(node._sortDistance);if(Number.isFinite(preset))return preset;const own=ownCoordinates(),position=nodeCoordinates(node);return own&&position?bearingAndDistance(own,position).distance:undefined}}
function compareNodes(a:AnyRecord,b:AnyRecord,key:NodeSortKey){if(key==="name"){const byName=nodeName(a.num).localeCompare(nodeName(b.num),"ru",{sensitivity:"base",numeric:true});if(byName)return byName}else{const av=nodeSortNumber(a,key),bv=nodeSortNumber(b,key),missing=av===undefined?bv===undefined?0:1:bv===undefined?-1:0;if(missing)return missing;if(av!==bv)return key==="hops"||key==="distance"?av!-bv!:bv!-av!}return (Number(b.lastHeard)||0)-(Number(a.lastHeard)||0)||nodeName(a.num).localeCompare(nodeName(b.num),"ru",{sensitivity:"base",numeric:true})}
function renderAimTargets(){
  const select=$<HTMLSelectElement>("aim-target");if(!select)return;
  const values=[...nodes.values()].filter(n=>!isOwnNode(Number(n.num))).sort((a,b)=>compareNodes(a,b,nodeSortKey("aim-target-sort")));
  const signature=values.map(n=>`${n.num}:${nodeName(n.num)}`).join("|");
  if(select.dataset.signature!==signature){select.dataset.signature=signature;select.replaceChildren(Object.assign(document.createElement("option"),{value:"",textContent:"Выберите ноду"}),...values.map(n=>Object.assign(document.createElement("option"),{value:String(n.num>>>0),textContent:`${nodeName(n.num)} · ${short(hex(n.num))}`})))}
  if(aimTarget&&!values.some(n=>(n.num>>>0)===aimTarget)){aimTarget=0;localStorage.removeItem("meshtastic-aim-target");select.value=""}
  if(aimTarget)select.value=String(aimTarget);else{const recent=airEvents.slice().reverse().find(e=>e.from&&!isOwnNode(e.from)&&!e.viaMqtt&&e.hops===0);if(recent){aimTarget=recent.from;select.value=String(aimTarget)}}
  renderAim();renderAimDirectNodes();renderAimNodeDirections();
}

function renderAimNodeDirections(){
  const list=document.getElementById("aim-node-directions-list"),caption=document.getElementById("aim-node-directions-caption");if(!list||!caption)return;
  const own=ownCoordinates(),query=$<HTMLInputElement>("aim-node-direction-search")?.value.trim().toLowerCase()||"";list.replaceChildren();
  if(!own){caption.textContent="Сначала сохраните нашу точку";list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:"Без координат нашей точки азимут и расстояние рассчитать нельзя."}));return}
  const rows=[...nodes.values()].filter(node=>!isOwnNode(Number(node.num))&&(!query||nodeSearchText(node).includes(query))).map(node=>{const num=Number(node.num)>>>0,position=nodeCoordinates(node),latest=airEvents.slice().reverse().find(event=>event.from===num&&!event.viaMqtt&&event.hops!==undefined),cachedHops=Number(node.lastHops),hops=latest?.hops??(!node.viaMqtt&&Number.isInteger(cachedHops)?cachedHops:undefined);return position&&hops!==undefined&&hops>=0&&hops<=2?{node,hops,route:bearingAndDistance(own,position)}:undefined}).filter((item):item is {node:AnyRecord;hops:number;route:{bearing:number;distance:number}}=>!!item).sort((a,b)=>compareNodes({...a.node,_sortHops:a.hops,_sortDistance:a.route.distance},{...b.node,_sortHops:b.hops,_sortDistance:b.route.distance},nodeSortKey("aim-directions-sort","hops"))||a.route.bearing-b.route.bearing);
  caption.textContent=`${rows.length} нод с координатами и маршрутом 0–2 перехода · наша антенна ${Math.round(normalizeDegrees(aimHeading))}° ${cardinal(aimHeading)}`;
  for(const {node,hops,route} of rows){const num=Number(node.num)>>>0,turn=signedAngle(route.bearing-aimHeading),button=document.createElement("button");button.type="button";button.className=`aim-direct-row aim-direction-row secondary${num===aimTarget?" selected":""}`;
    const identity=document.createElement("span");identity.className="aim-direct-identity";identity.append(Object.assign(document.createElement("strong"),{textContent:nodeName(num)}),Object.assign(document.createElement("small"),{textContent:`${short(hex(num))} · ${hops} ${hops===1?"переход":"перехода"}${node.lastHeard?` · ${aimAge(Number(node.lastHeard))}`:""}`}));
    const bearing=document.createElement("span");bearing.className="aim-direct-direction";bearing.append(Object.assign(document.createElement("b"),{textContent:`${Math.round(route.bearing)}° ${cardinal(route.bearing)}`}),Object.assign(document.createElement("small"),{textContent:"азимут от нашей точки"}));
    const turnElement=document.createElement("span");turnElement.className="aim-direction-turn";turnElement.append(Object.assign(document.createElement("b"),{textContent:Math.abs(turn)<=1?"по оси":`${Math.abs(Math.round(turn))}° ${turn>0?"вправо":"влево"}`}),Object.assign(document.createElement("small"),{textContent:`от ${Math.round(normalizeDegrees(aimHeading))}°`}));
    const distance=document.createElement("span");distance.className="aim-direction-distance";distance.append(Object.assign(document.createElement("b"),{textContent:route.distance<10?`${route.distance.toFixed(2)} км`:`${route.distance.toFixed(1)} км`}),Object.assign(document.createElement("small"),{textContent:"по прямой"}));
    button.append(identity,bearing,turnElement,distance);button.addEventListener("click",()=>selectAimTarget(num));list.append(button)}
  if(!rows.length)list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:query?"Совпадений среди нод с координатами и маршрутом 0–2 перехода нет.":"Нет нод с координатами и известным маршрутом в 0–2 перехода."}));
}
function drawAimChart(samples:AirEvent[]){
  const canvas=$<HTMLCanvasElement>("aim-chart");if(!canvas)return;const rect=canvas.getBoundingClientRect(),scale=devicePixelRatio||1,w=Math.max(280,rect.width),h=150;canvas.width=w*scale;canvas.height=h*scale;const c=canvas.getContext("2d");if(!c)return;c.scale(scale,scale);c.clearRect(0,0,w,h);c.strokeStyle="#26344c";c.fillStyle="#8295ae";c.font="11px system-ui";
  for(const rssi of [-40,-60,-80,-100,-120]){const y=10+(-40-rssi)/80*(h-24);c.beginPath();c.moveTo(34,y);c.lineTo(w,y);c.stroke();c.fillText(String(rssi),2,y+4)}
  if(samples.length<2)return;c.strokeStyle="#61e7a5";c.lineWidth=3;c.beginPath();samples.forEach((sample,index)=>{const x=34+index*(w-38)/(samples.length-1),y=10+Math.max(0,Math.min(1,(-40-Number(sample.rssi))/80))*(h-24);index?c.lineTo(x,y):c.moveTo(x,y)});c.stroke();
}
function renderAim(){
  const heading=normalizeDegrees(aimHeading),headingNeedle=$<HTMLElement>("aim-heading-needle");if(!headingNeedle)return;
  headingNeedle.style.transform=`rotate(${heading}deg)`;$<HTMLInputElement>("aim-heading").value=String(Math.round(heading));$("aim-heading-output").textContent=`${Math.round(heading)}°`;$("aim-heading-value").textContent=`${Math.round(heading)}° ${cardinal(heading)}`;
  const targetNeedle=$<HTMLElement>("aim-target-needle"),target=nodes.get(aimTarget),own=ownCoordinates(),targetPosition=target?nodeCoordinates(target):undefined;
  let bearing:number|undefined;
  if(own&&targetPosition){const route=bearingAndDistance(own,targetPosition);bearing=route.bearing;targetNeedle.hidden=false;targetNeedle.style.transform=`rotate(${bearing}deg)`;$("aim-bearing").textContent=`${Math.round(bearing)}° ${cardinal(bearing)}`;$("aim-distance").textContent=route.distance<10?`${route.distance.toFixed(2)} км`:`${route.distance.toFixed(1)} км`;const turn=signedAngle(bearing-heading),aligned=Math.abs(turn)<=5;$("aim-turn").textContent=aligned?"по оси":`${Math.abs(Math.round(turn))}° ${turn>0?"вправо":"влево"}`;$("aim").classList.toggle("aim-aligned",aligned)}else{targetNeedle.hidden=true;$("aim-bearing").textContent="—";$("aim-distance").textContent="—";$("aim-turn").textContent="—";$("aim").classList.remove("aim-aligned")}
  const samples=airEvents.filter(e=>e.from===aimTarget&&!e.viaMqtt&&e.hops===0&&e.rssi!==undefined&&e.ts>=aimStartedAt).slice(-40),latest=samples.at(-1),best=samples.length?Math.max(...samples.map(e=>Number(e.rssi))):undefined;
  if(latest){const age=Math.max(0,Math.floor(Date.now()/1000-latest.ts));$("aim-rssi").textContent=`${latest.rssi} dBm`;$("aim-snr").textContent=latest.snr===undefined?"SNR —":`SNR ${Number(latest.snr).toFixed(1)} dB`;$("aim-best").textContent=`${best} dBm`;$("aim-age").textContent=age<2?"сейчас":age<60?`${age} с назад`:`${Math.floor(age/60)} мин назад`;$("aim-hops").textContent="0 · прямо";$<HTMLElement>("aim-signal-fill").style.width=`${Math.max(0,Math.min(100,(Number(latest.rssi)+120)/80*100))}%`;$("aim").classList.toggle("aim-stale",age>120)}else{$("aim-rssi").textContent="—";$("aim-snr").textContent="SNR —";$("aim-best").textContent="—";$("aim-age").textContent="—";$("aim-hops").textContent="—";$<HTMLElement>("aim-signal-fill").style.width="0";$("aim").classList.remove("aim-stale")}
  drawAimChart(samples);
  if(!aimTarget)$("aim-status").textContent="Выберите целевую ноду.";else if(!target)$("aim-status").textContent="Целевая нода пока отсутствует в локальной базе.";else if(!latest){const any=airEvents.slice().reverse().find(e=>e.from===aimTarget);$("aim-status").textContent=any?.viaMqtt?"Последний пакет пришёл через MQTT и не подходит для наведения.":any&&any.hops!==0?"Последний пакет был ретранслирован или число переходов неизвестно; его RSSI не используется.":"Ждём прямой пакет выбранной ноды. Экран сам ничего не запрашивает."}else $("aim-status").textContent=`${nodeName(aimTarget)} · ${samples.length} прямых RF‑отсчётов после сброса${bearing===undefined?" · для азимута нужны координаты обеих нод":""}.`;
}
const aimSector=(heading:number)=>Math.round(normalizeDegrees(heading)/10)*10%360;
const median=(values:number[])=>{if(!values.length)return undefined;const sorted=values.slice().sort((a,b)=>a-b),middle=Math.floor(sorted.length/2);return sorted.length%2?sorted[middle]:(sorted[middle-1]+sorted[middle])/2};
const aimMeter=(rssi:number|undefined)=>rssi===undefined?0:Math.max(0,Math.min(100,(rssi+120)/80*100));
function aimAge(ts:number){const age=Math.max(0,Math.floor(Date.now()/1000-ts));return age<2?"сейчас":age<60?`${age} с назад`:age<3600?`${Math.floor(age/60)} мин назад`:`${Math.floor(age/3600)} ч назад`}
function renderAimTracking(){
  const body=$("aim-comparison-body");if(!body)return;
  const now=Math.floor(Date.now()/1000),sector=aimSector(aimHeading),scope=aimSamples.filter(sample=>(!aimTarget||sample.from===aimTarget)&&sample.ts>=now-1800),current=scope.filter(sample=>aimSector(sample.heading)===sector),fast=current.filter(sample=>sample.ts>=now-60),latest=fast.at(-1),fastMedian=median(fast.map(sample=>sample.rssi)),longMedian=median(current.map(sample=>sample.rssi)),fastSnr=median(fast.flatMap(sample=>sample.snr===undefined?[]:[sample.snr])),longSnr=median(current.flatMap(sample=>sample.snr===undefined?[]:[sample.snr]));
  $("aim-monitor-scope").textContent=aimTarget?`Цель: ${nodeName(aimTarget)} · только прямой RF‑приём`:`Все прямые ноды · грубая оценка; выберите одну цель для точности`;
  $("aim-direction-state").textContent=`Сейчас измеряется сектор ${sector}° ${cardinal(sector)} (азимут антенны ${Math.round(normalizeDegrees(aimHeading))}°). Пакеты записываются автоматически.`;
  $("aim-fast-rssi").textContent=latest?`${latest.rssi} dBm`:"Ждём пакет";
  $("aim-fast-snr").textContent=fastSnr===undefined?"SNR —":`медиана SNR ${fastSnr.toFixed(1)} dB`;
  $("aim-fast-detail").textContent=fast.length?`${fast.length} пак. · медиана ${fastMedian!.toFixed(1)} dBm · последний ${aimAge(latest!.ts)}`:"За последнюю минуту прямых пакетов нет";
  $<HTMLElement>("aim-fast-fill").style.width=`${aimMeter(fastMedian)}%`;
  $("aim-long-rssi").textContent=longMedian===undefined?"Нет данных":`${longMedian.toFixed(1)} dBm`;
  $("aim-long-snr").textContent=longSnr===undefined?"SNR —":`медиана SNR ${longSnr.toFixed(1)} dB`;
  $("aim-long-detail").textContent=current.length?`${current.length} пак. · лучший ${Math.max(...current.map(sample=>sample.rssi))} dBm · ${new Set(current.map(sample=>sample.from)).size} нод.`:"За 30 минут данных в этом секторе нет";
  $<HTMLElement>("aim-long-fill").style.width=`${aimMeter(longMedian)}%`;
  const groups=new Map<number,AimSample[]>();for(const sample of scope){const key=aimSector(sample.heading),values=groups.get(key)||[];values.push(sample);groups.set(key,values)}
  const rows=[...groups].map(([heading,samples])=>({heading,samples,rssi:median(samples.map(sample=>sample.rssi))!,snr:median(samples.flatMap(sample=>sample.snr===undefined?[]:[sample.snr]))})).sort((a,b)=>a.heading-b.heading),best=rows.length?Math.max(...rows.map(row=>row.rssi)):undefined;
  body.replaceChildren();
  if(!rows.length){body.append(Object.assign(document.createElement("p"),{className:"muted aim-comparison-empty",textContent:"Поверните антенну и дождитесь прямых пакетов."}));return}
  for(const row of rows){const line=document.createElement("div");line.className=`aim-comparison-row${row.heading===sector?" active":""}`;const title=document.createElement("strong");title.textContent=`${row.heading}° ${cardinal(row.heading)}${row.rssi===best?" · лучший":""}`;line.append(title,Object.assign(document.createElement("span"),{textContent:`медиана RSSI ${row.rssi.toFixed(1)} dBm`}),Object.assign(document.createElement("span"),{textContent:row.snr===undefined?"медиана SNR —":`медиана SNR ${row.snr.toFixed(1)} dB`}),Object.assign(document.createElement("span"),{textContent:`лучший ${Math.max(...row.samples.map(sample=>sample.rssi))} dBm · ${row.samples.length} пак.`}));body.append(line)}
}
function selectAimTarget(num:number){
  aimTarget=num>>>0;if(aimTarget)localStorage.setItem("meshtastic-aim-target",String(aimTarget));else localStorage.removeItem("meshtastic-aim-target");
  const select=$<HTMLSelectElement>("aim-target");if([...select.options].some(option=>Number(option.value)===aimTarget))select.value=String(aimTarget);
  renderAim();renderAimTracking();renderAimDirectNodes();
}
function renderAimDirectNodes(){
  const list=document.getElementById("aim-direct-list"),caption=document.getElementById("aim-direct-caption");if(!list||!caption)return;
  const now=Math.floor(Date.now()/1000),cutoff=now-7200,grouped=new Map<number,{latest:AirEvent;best:number;count:number}>();
  for(const event of airEvents){if(event.ts<cutoff||!event.from||isOwnNode(event.from)||event.viaMqtt||event.hops!==0||event.rssi===undefined)continue;const current=grouped.get(event.from);if(current){current.count++;current.best=Math.max(current.best,Number(event.rssi));if(event.ts>=current.latest.ts)current.latest=event}else grouped.set(event.from,{latest:event,best:Number(event.rssi),count:1})}
  for(const node of nodes.values()){const num=Number(node.num)>>>0,lastHeard=Number(node.lastHeard),rssi=Number(node.lastRssi);if(!num||isOwnNode(num)||grouped.has(num)||node.viaMqtt||Number(node.lastHops)!==0||!Number.isFinite(lastHeard)||lastHeard<cutoff||!Number.isFinite(rssi))continue;grouped.set(num,{latest:{ts:lastHeard,kind:"последний RF-пакет",from:num,to:0,channel:Number(node.channel)||0,rssi,snr:Number.isFinite(Number(node.lastSnr))?Number(node.lastSnr):undefined,hops:0},best:rssi,count:1})}
  const directSort=nodeSortKey("aim-direct-sort"),directNode=([num,data]:[number,{latest:AirEvent;best:number;count:number}])=>({...nodes.get(num),num,lastHeard:data.latest.ts,_sortHops:0,_sortRssi:data.latest.rssi,_sortSnr:data.latest.snr});
  const rows=[...grouped].sort((a,b)=>compareNodes(directNode(a),directNode(b),directSort));caption.textContent=`${rows.length} нод · локальное обновление ${new Date().toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit",second:"2-digit"})}`;list.replaceChildren();
  const own=ownCoordinates();
  for(const [num,data] of rows){const node=nodes.get(num),position=node?nodeCoordinates(node):undefined,route=own&&position?bearingAndDistance(own,position):undefined,age=Math.max(0,now-data.latest.ts),button=document.createElement("button");button.type="button";button.className=`aim-direct-row secondary${num===aimTarget?" selected":""}`;
    const identity=document.createElement("span");identity.className="aim-direct-identity";identity.append(Object.assign(document.createElement("strong"),{textContent:nodeName(num)}),Object.assign(document.createElement("small"),{textContent:`${short(hex(num))} · ${data.latest.kind} · канал ${data.latest.channel}`}));
    const signal=document.createElement("span");signal.className="aim-direct-signal";signal.append(Object.assign(document.createElement("b"),{textContent:`${data.latest.rssi} dBm`}),Object.assign(document.createElement("small"),{textContent:data.latest.snr===undefined?"SNR —":`SNR ${Number(data.latest.snr).toFixed(1)} dB`}));
    const observation=document.createElement("span");observation.className="aim-direct-observation";observation.append(Object.assign(document.createElement("b"),{textContent:`лучший ${data.best} dBm`}),Object.assign(document.createElement("small"),{textContent:`${data.count} пак. · ${age<60?`${age} с`:age<3600?`${Math.floor(age/60)} мин`:`${Math.floor(age/3600)} ч`} назад`}));
    const direction=document.createElement("span");direction.className="aim-direct-direction";direction.append(Object.assign(document.createElement("b"),{textContent:route?`${Math.round(route.bearing)}° ${cardinal(route.bearing)}`:"азимут —"}),Object.assign(document.createElement("small"),{textContent:route?(route.distance<10?`${route.distance.toFixed(2)} км`:`${route.distance.toFixed(1)} км`):"нет координат"}));
    button.append(identity,signal,observation,direction);button.addEventListener("click",()=>{selectAimTarget(num);$("aim-target").scrollIntoView({behavior:"smooth",block:"center"})});list.append(button)}
  if(!rows.length)list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:"Прямых LoRa‑пакетов за последние 2 часа пока нет. Список появится после пассивного приёма пакета с 0 переходов."}));
}
async function refreshAimDirectNodes(){const button=$<HTMLButtonElement>("aim-direct-refresh");button.disabled=true;$("aim-direct-caption").textContent="Обновляю локальный кэш…";await loadNodeCache();renderAimDirectNodes();button.disabled=false}
async function refreshAimReadings(){const button=$<HTMLButtonElement>("aim-refresh"),state=$("aim-refresh-state");button.disabled=true;state.textContent="Перечитываю локальные данные…";await Promise.all([loadNodeCache(),loadAimMeasurements()]);renderAim();renderAimTracking();renderAimDirectNodes();state.textContent=`Показания обновлены локально в ${new Date().toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit",second:"2-digit"})}. Новый RSSI появится только после следующего прямого пакета.`;button.disabled=false}
function handleOrientation(event:DeviceOrientationEvent){
  const raw=event as DeviceOrientationEvent&{webkitCompassHeading?:number};let heading=raw.webkitCompassHeading;
  if(!Number.isFinite(heading)&&event.absolute&&Number.isFinite(event.alpha))heading=360-Number(event.alpha)+(screen.orientation?.angle||0);
  if(!Number.isFinite(heading))return;aimHeading=normalizeDegrees(Number(heading));localStorage.setItem("meshtastic-aim-heading",String(Math.round(aimHeading)));$("aim-compass-state").textContent="Компас активен. Совместите верх телефона с осью антенны.";renderAim();renderAimTracking();
}
async function enableAimCompass(){
  const orientation=DeviceOrientationEvent as unknown as {requestPermission?:()=>Promise<string>};
  try{if(orientation.requestPermission&&(await orientation.requestPermission())!=="granted")throw new Error("доступ не разрешён");if(!compassActive){window.addEventListener("deviceorientationabsolute",handleOrientation as EventListener);window.addEventListener("deviceorientation",handleOrientation);compassActive=true}$<HTMLButtonElement>("aim-compass").textContent="Компас включён";$("aim-compass-state").textContent="Ожидаю данные компаса…"}catch(error){$("aim-compass-state").textContent=`Компас недоступен: ${errorText(error)}. Используйте ручной угол.`}
}

async function loadOwnLocation(){
  try{let p:AnyRecord={};if(hasBackend()){try{p=await fetchJson("/own-location.json")}catch{}}else{try{p=JSON.parse(localStorage.getItem(OWN_LOCATION_STORAGE_KEY)||"{}")}catch{}}if(!Number.isFinite(Number(p.latitude))||!Number.isFinite(Number(p.longitude)))p=await fetchJson("/node-location.json");const lat=Number(p.latitude),lon=Number(p.longitude),nodeId=Number(p.nodeId)>>>0;if(Number.isFinite(lat)&&lat>=-90&&lat<=90&&Number.isFinite(lon)&&lon>=-180&&lon<=180){ownFixedPosition={lat,lon};ownNodeNum=nodeId||ownNodeNum;$<HTMLInputElement>("aim-own-lat").value=lat.toFixed(6);$<HTMLInputElement>("aim-own-lon").value=lon.toFixed(6);if(ownNodeNum&&!nodes.get(ownNodeNum)?.user)addNode(ownNodeNum,{user:fallbackOwner});renderOwnIdentity();renderMessages();renderNodes();renderAir();scheduleMap();renderAim();renderAimNodeDirections()}}
  catch{$("aim-own-location-state").textContent="Не удалось загрузить локальную точку. Введите координаты вручную."}
}
async function saveOwnLocation(){
  const button=$<HTMLButtonElement>("aim-own-location-save"),state=$("aim-own-location-state"),latitude=Number($<HTMLInputElement>("aim-own-lat").value),longitude=Number($<HTMLInputElement>("aim-own-lon").value);
  if(!Number.isFinite(latitude)||latitude < -90||latitude > 90||!Number.isFinite(longitude)||longitude < -180||longitude > 180){state.textContent="Проверьте широту (−90…90) и долготу (−180…180).";return}
  button.disabled=true;state.textContent="Сохраняю локально…";
  try{let saved:AnyRecord;if(hasBackend()){const response=await fetch("/own-location.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({latitude,longitude,nodeId:(myNode||ownNodeNum)>>>0})});if(!response.ok)throw new Error(await response.text());saved=await response.json()}else{saved={latitude,longitude,nodeId:(myNode||ownNodeNum)>>>0,savedAt:Math.floor(Date.now()/1000)};localStorage.setItem(OWN_LOCATION_STORAGE_KEY,JSON.stringify(saved))}ownFixedPosition={lat:Number(saved.latitude),lon:Number(saved.longitude)};state.textContent=`Сохранено ${hasBackend()?"на Orange Pi":"в этом браузере"} ${new Date(Number(saved.savedAt)*1000).toLocaleString("ru-RU")}. Координаты не передавались.`;scheduleMap();renderMap();renderAim();renderAimNodeDirections()}
  catch(error){state.textContent=`Ошибка сохранения: ${errorText(error)}`}finally{button.disabled=false}
}

function pingHistory(){
  const values:{ts:number;id?:number;source:string}[]=[];
  for(const ping of scheduledPings)values.push({...ping,source:"автоматический"});
  for(const trial of aimTrials)if(trial.pingSentAt)values.push({ts:trial.pingSentAt,id:trial.pingPacketId,source:`замер ${Math.round(trial.heading)}°`});
  for(const message of messages)if(message.event==="tx"&&message.text.trim().toLowerCase()==="ping"&&(channelName(message.channel).toLowerCase()==="ping"||message.channel===3))values.push({ts:message.ts,id:message.id,source:"ручной"});
  const unique=new Map<string,{ts:number;id?:number;source:string}>();for(const value of values)if(Number.isFinite(value.ts)&&value.ts>0)unique.set(value.id?`id:${value.id}`:`time:${value.ts}`,value);
  return [...unique.values()].sort((a,b)=>a.ts-b.ts).slice(-24);
}
function renderAimPingReplies(){
  const list=document.getElementById("aim-ping-replies-list"),summary=document.getElementById("aim-ping-replies-summary");if(!list||!summary)return;const pings=pingHistory(),now=Math.floor(Date.now()/1000);list.replaceChildren();
  if(!pings.length){summary.textContent="Наших Ping в доступной истории пока нет.";list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:"После следующего Ping здесь появятся ответившие ноды и время ответа."}));return}
  const groups=pings.map((ping,index)=>{const until=Math.min(ping.ts+600,pings[index+1]?.ts||Number.POSITIVE_INFINITY),byNode=new Map<number,Message>();for(const message of messages){if(message.event!=="rx"||message.ts<ping.ts||message.ts>=until||!(channelName(message.channel).toLowerCase()==="ping"||message.channel===3)||!hasPingReplyCue(message.text)||messageAddressing(message).kind!=="ours")continue;const from=Number.parseInt(message.from.replace("!",""),16)>>>0;if(from&&!byNode.has(from))byNode.set(from,message)}return {ping,replies:[...byNode.entries()].sort((a,b)=>a[1].ts-b[1].ts)}});
  const latest=groups.at(-1)!;summary.textContent=`Последний Ping ${new Date(latest.ping.ts*1000).toLocaleTimeString("ru-RU")} · ${latest.replies.length} ${latest.replies.length===1?"ответившая нода":"ответивших нод"}${now>latest.ping.ts+600?" · окно закрыто":" · ждём до 10 минут"}.`;
  for(const group of groups.slice(-7).reverse()){const article=document.createElement("article"),head=document.createElement("div"),replies=document.createElement("div");article.className="aim-ping-reply-group";head.className="aim-ping-reply-head";head.append(Object.assign(document.createElement("strong"),{textContent:`Ping · ${new Date(group.ping.ts*1000).toLocaleString("ru-RU")}`}),Object.assign(document.createElement("span"),{className:"pill",textContent:`${group.replies.length} нод`}),Object.assign(document.createElement("span"),{className:"muted",textContent:group.ping.id?`#${group.ping.id} · ${group.ping.source}`:group.ping.source}));replies.className="aim-ping-reply-nodes";
    if(!group.replies.length)replies.append(Object.assign(document.createElement("span"),{className:"muted",textContent:now<=group.ping.ts+600?"Ожидаем ответы…":"Явных ответов не найдено."}));
    for(const [num,message] of group.replies){const row=document.createElement("button");row.type="button";row.className="secondary aim-ping-reply-node";row.textContent=`${nodeName(num)} · ${new Date(message.ts*1000).toLocaleTimeString("ru-RU")} · +${message.ts-group.ping.ts} с`;row.addEventListener("click",()=>openNode(num));replies.append(row)}article.append(head,replies);list.append(article)}
}
function syncScheduledPingMessages(){
  messages=messages.filter(message=>message.source!=="автоматический Ping");
  const pingChannel=[...channels.entries()].find(([,channel])=>String(channel.settings?.name||"").trim().toLowerCase()==="ping")?.[0]??3;
  for(const ping of scheduledPings){
    const alreadyShown=messages.some(message=>message.event==="tx"&&message.text.trim().toLowerCase()==="ping"&&((ping.id&&message.id===ping.id)||Math.abs(message.ts-ping.ts)<=2));
    if(!alreadyShown)messages.push({ts:ping.ts,event:"tx",from:myNode?hex(myNode):"self",to:"^all",channel:pingChannel,text:"Ping",id:ping.id,source:"автоматический Ping"});
  }
  messages=messages.slice().sort((a,b)=>a.ts-b.ts).slice(-MAX_BROWSER_MESSAGES);
  renderMessageChannelTabs();renderMessages();
}
function renderPingSchedule(data:AnyRecord){
  const config=data?.config||{},progress=data?.progress||{},running=Boolean(config.enabled)&&!progress.completedAt&&!progress.error;$<HTMLButtonElement>("aim-autoping-start").disabled=running;$<HTMLButtonElement>("aim-autoping-cancel").disabled=!running;
  const sentPings=Array.isArray(progress.sentPings)?progress.sentPings:[];scheduledPings=sentPings.map((item:AnyRecord)=>({ts:Number(item.sentAt),id:Number(item.packetId)||undefined})).filter((item:{ts:number})=>Number.isFinite(item.ts)&&item.ts>0);if(!scheduledPings.length&&Number(progress.lastSentAt)>0)scheduledPings=[{ts:Number(progress.lastSentAt),id:Number(progress.lastPacketId)||undefined}];syncScheduledPingMessages();renderAimPingReplies();
  if(config.intervalMinutes)$<HTMLInputElement>("aim-autoping-interval").value=String(config.intervalMinutes);if(config.count)$<HTMLInputElement>("aim-autoping-count").value=String(config.count);
  if(running){const sent=Number(progress.sent)||0,next=Number(progress.nextAt)||Number(config.createdAt)||0,last=Number(progress.lastSentAt)||0,packetId=Number(progress.lastPacketId)||0;$("aim-autoping-state").textContent=`Выполняется: отправлено ${sent} из ${config.count}; направление ${Number(config.heading).toFixed(0)}°. ${last?`Последний ${new Date(last*1000).toLocaleString("ru-RU")}${packetId?` · пакет #${packetId}`:""}. `:""}${next?`Следующий ${new Date(next*1000).toLocaleString("ru-RU")}.`:"Ожидаю отправку."}`}
  else if(progress.error)$("aim-autoping-state").textContent=`Остановлен из-за ошибки: ${progress.error}`;
  else if(progress.completedAt)$("aim-autoping-state").textContent=`Завершён: отправлено ${progress.sent||0} из ${config.count||0}.`;
  else if(config.cancelledAt)$("aim-autoping-state").textContent=`Остановлен: отправлено ${progress.sent||0} из ${config.count||0}.`;
  else $("aim-autoping-state").textContent="Не запущен. Первый Ping отправляется сразу, остальные — только в точный канал Ping с заданным интервалом.";
}
async function loadPingSchedule(){try{renderPingSchedule(await fetchJson("/ping-schedule.json"))}catch(error){$("aim-autoping-state").textContent=`Планировщик недоступен: ${errorText(error)}`}}
async function changePingSchedule(action:"start"|"cancel"){
  const intervalMinutes=Number($<HTMLInputElement>("aim-autoping-interval").value),count=Number($<HTMLInputElement>("aim-autoping-count").value),state=$("aim-autoping-state");
  if(action==="start"&&(!Number.isInteger(intervalMinutes)||intervalMinutes<15||intervalMinutes>1440||!Number.isInteger(count)||count<1||count>24)){state.textContent="Интервал: 15–1440 минут; количество: 1–24.";return}
  state.textContent=action==="start"?"Сохраняю задание; первый Ping будет отправлен планировщиком…":"Останавливаю…";
  try{const response=await fetch("/ping-schedule.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action,intervalMinutes,count,heading:normalizeDegrees(aimHeading)})});if(!response.ok)throw new Error(await response.text());renderPingSchedule(await response.json());window.setTimeout(()=>void loadPingSchedule(),1500)}catch(error){state.textContent=`Ошибка: ${errorText(error)}`}
}

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
  // HTTP configuration replays a stored packet snapshot. It is useful for
  // building NodeDB, but it must not be presented as live RF reception. The
  // HTTP queue can keep draining after configure() resolves, so require a
  // quiet interval before treating subsequent packets as live.
  if(!livePacketCapture){if(!initialNodeSync)scheduleLivePacketCapture();return}
  const decoded=packet.payloadVariant?.case==="decoded"?packet.payloadVariant.value:undefined;
  const port=decoded?.portnum;
  const kind=port===undefined?(packet.payloadVariant?.case||"UNKNOWN"):(Protobuf.Portnums.PortNum as AnyRecord)[port]||`PORT_${port}`;
  const hopStart=Number(packet.hopStart),hopLimit=Number(packet.hopLimit),relayNode=Number(packet.relayNode);
  const hops=Number.isFinite(hopStart)&&Number.isFinite(hopLimit)&&hopStart>=hopLimit?hopStart-hopLimit:undefined;
  const event:AirEvent={ts:Math.floor(Date.now()/1000),kind,from:Number(packet.from)>>>0,to:Number(packet.to)>>>0,channel:Number(packet.channel)||0,rssi:packet.rxRssi||undefined,snr:packet.rxSnr||undefined,hops,hopStart:Number.isFinite(hopStart)?hopStart:undefined,hopLimit:Number.isFinite(hopLimit)?hopLimit:undefined,relayNode:Number.isFinite(relayNode)?relayNode&0xff:undefined,id:packet.id,viaMqtt:packet.viaMqtt,wantAck:packet.wantAck===true||undefined,wantResponse:decoded?.wantResponse===true||undefined};
  const packetKey=airPacketIdentity(event);
  if(event.viaMqtt){if(packetKey){mqttPacketIds.add(packetKey);while(mqttPacketIds.size>1200)mqttPacketIds.delete(mqttPacketIds.values().next().value!)}return}
  if(packetKey&&seenAirPacketIds.has(packetKey))return;
  if(packetKey){seenAirPacketIds.add(packetKey);while(seenAirPacketIds.size>1200)seenAirPacketIds.delete(seenAirPacketIds.values().next().value!)}
  airEvents.push(event);airEvents=airEvents.slice(-400);localStorage.setItem("meshtastic-air-events",JSON.stringify(airEvents));
  recordAimTrialEvent(event);
  if(event.from&&!isOwnNode(event.from)&&!event.viaMqtt&&event.hops===0&&event.rssi!==undefined){aimSamples.push({ts:event.ts,heading:normalizeDegrees(aimHeading),from:event.from,rssi:Number(event.rssi),snr:event.snr});scheduleAimMeasurementsSave()}
  if(event.from){const current=nodes.get(event.from)||{},history=[...(current.signalHistory||[]),{ts:event.ts,rssi:event.rssi,snr:event.snr,hops:event.hops}].slice(-50);addNode(event.from,{lastHeard:event.ts,lastRssi:event.rssi,lastSnr:event.snr,lastHops:event.hops,signalHistory:history})}
  renderAir();renderNodes();renderAim();renderAimTracking();
}

function scheduleLivePacketCapture(){
  if(livePacketCaptureTimer!==undefined)window.clearTimeout(livePacketCaptureTimer);
  livePacketCaptureTimer=window.setTimeout(()=>{livePacketCaptureTimer=undefined;livePacketCapture=true},5000);
}

async function fetchJson(url:string) {
  const response = await fetch(url, {cache:"no-store"});
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}
async function detectRuntimeMode(){
  try{
    const response=await fetch("/runtime-capabilities.json",{cache:"no-store",headers:{Accept:"application/json"}});
    const contentType=response.headers.get("content-type")||"";
    if(response.ok&&contentType.includes("application/json")&&(await response.json())?.backend==="orangepi")runtimeMode="orangepi";
    else runtimeMode="board";
  }catch{runtimeMode="board"}
  const backend=hasBackend();
  document.documentElement.dataset.runtime=runtimeMode;
  document.querySelectorAll<HTMLElement>("[data-requires-backend]").forEach(element=>element.hidden=!backend);
  $("runtime-label").textContent=backend?"домашний интерфейс · Orange Pi":"портативный интерфейс · напрямую к плате";
  $("aim-own-location-state").textContent=backend?"Координаты хранятся только на Orange Pi и не публикуются по LoRa или MQTT.":"Координаты хранятся только в этом браузере телефона и не публикуются по LoRa.";
  $("aim-monitor-storage-note").textContent=`Замеры автоматически группируются по секторам 10° и сохраняются ${backend?"на Orange Pi":"в этом браузере телефона"}. Для честного сравнения держите одну и ту же целевую ноду и дайте каждому направлению набрать несколько пакетов.`;
  $("aim-direct-storage-note").textContent=`Обновление только перечитывает локальный журнал и кэш нод ${backend?"Orange Pi":"этого браузера и платы"}; оно ничего не передаёт в LoRa. Нажмите на ноду, чтобы выбрать её целью.`;
}
async function sendLoRa(action:"text"|"position"|"trace",text:string,destination:string,channel:number,publicKey=""){
  if(!hasBackend()){
    if(!device)throw new Error("Плата ещё не подключена");
    const client=device as AnyRecord,target:number|"broadcast"=destination==="^all"?"broadcast":Number.parseInt(destination.replace("!",""),16)>>>0;
    if(action==="text")return Number(await client.sendText(text,target,destination!=="^all",channel))>>>0;
    if(typeof target!=="number"||!Number.isFinite(target))throw new Error("Для запроса нужна конкретная нода");
    return Number(await (action==="position"?client.requestPosition(target):client.traceRoute(target)))>>>0;
  }
  const response=await fetch("/lora-send.json",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action,text,destination,channel,publicKey})});
  const raw=await response.text();let result:AnyRecord={};try{result=JSON.parse(raw)}catch{}if(!response.ok||!result?.ok){if(result&&typeof result==="object"&&Object.keys(result).length)throw result;throw new Error(raw.replace(/<[^>]+>/g," ").replace(/\s+/g," ").trim()||`${response.status} ${response.statusText}`)}return Number(result.packetId)>>>0;
}

async function loadDeliveryStates(){
  if(!hasBackend())return;
  try{const payload=await fetchJson("/delivery-status.json"),events=Array.isArray(payload.events)?payload.events:[];for(const item of events){const packetId=Number(item?.packetId)>>>0,status=item?.status;if(!packetId||!["accepted","delivered","failed"].includes(status))continue;const next={...item,packetId,status} as DeliveryState,current=deliveryStates.get(packetId);if(!current||next.status!=="accepted"||current.status==="accepted")deliveryStates.set(packetId,next)}renderMessages()}catch{}
}
function deliveryLabel(message:Message){
  const state=deliveryStates.get(Number(message.id)>>>0);
  if(!state)return {cls:"accepted",text:"Принято нашей платой · ACK ожидается",title:"Packet ID назначен локально; доставка адресату ещё не подтверждена."};
  if(state.status==="delivered")return {cls:"delivered",text:`✓ Доставлено адресату · ${fmtTime(state.ts)}`,title:state.evidence||"Получен адресный routing ACK."};
  if(state.status==="failed")return {cls:"failed",text:`Ошибка доставки${state.errorCode?` · код ${state.errorCode}`:""}`,title:[state.errorReason,state.evidence].filter(Boolean).join(" · ")};
  return {cls:"accepted",text:"Принято нашей платой · ACK ожидается",title:state.evidence||"Packet ID назначен локально; доставка адресату ещё не подтверждена."};
}

function mqttMessageKey(message:MqttMessage){return `${message.source}|${message.id}`}
function mergeMqttMessages(brokerMessages:unknown,onemeshMessages:unknown=[]){
  const merged=new Map<string,MqttMessage>();
  for(const item of [...(Array.isArray(brokerMessages)?brokerMessages:[]),...(Array.isArray(onemeshMessages)?onemeshMessages:[]),...mqttMeshMessages]){if(!item||typeof item!=="object")continue;const row=item as MqttMessage;if(!row.id||!row.text)continue;merged.set(mqttMessageKey(row),row)}
  mqttMessages=[...merged.values()].sort((a,b)=>a.ts-b.ts).slice(-500);
}
function renderMqtt(){
  const list=document.getElementById("mqtt-message-list");if(!list)return;list.replaceChildren();
  for(const message of mqttMessages.slice().reverse()){const card=document.createElement("article");card.className=`card mqtt-message${message.direction==="tx"?" local":""}`;const head=document.createElement("div");head.className="card-head";const sender=message.source==="onemesh-api"&&message.senderId?nodeName(message.senderId):message.sender||"MQTT",sourceLabel=message.source==="meshtastic"?"MQTT через плату":message.source==="onemesh-api"?"OneMesh API":"MQTT брокер";head.append(Object.assign(document.createElement("strong"),{textContent:message.direction==="tx"?"Мы":sender}),Object.assign(document.createElement("span"),{className:"badge mqtt-source-badge",textContent:sourceLabel}),Object.assign(document.createElement("time"),{textContent:fmtTime(message.ts)}));const body=Object.assign(document.createElement("div"),{className:"text",textContent:message.text});const meta=Object.assign(document.createElement("div"),{className:"meta mqtt-topic",textContent:`Источник: ${message.topic}${message.senderId?` · ${message.senderId}`:""}`});card.append(head,body,meta);list.append(card)}
  if(!mqttMessages.length)list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:"Сообщений OneMesh или MQTT пока нет."}));
  const unread=mqttMessages.some(message=>message.direction==="rx"&&message.ts>mqttLastSeenTs);$<HTMLElement>("mqtt-unread").hidden=!unread;
}
function applyMqttState(payload:AnyRecord){
  const status=payload?.status||{},onemesh=payload?.onemesh||{};mqttConnected=Boolean(status.connected);mergeMqttMessages(payload?.messages,payload?.onemeshMessages);const state=$("mqtt-state"),onemeshAvailable=Boolean(onemesh.available);state.textContent=mqttConnected&&onemeshAvailable?"OneMesh + MQTT":mqttConnected?"MQTT подключён":onemeshAvailable?"OneMesh API":status.enabled?"подключение…":"выключен";state.className=`pill ${mqttConnected||onemeshAvailable?"ok":status.enabled?"warn":"warn"}`;
  const active=document.activeElement;for(const [id,value] of [["mqtt-host",status.host||""],["mqtt-port",String(status.port||8883)],["mqtt-topic",status.topic||"msh/RU/MSK/2/json/MediumFast"],["mqtt-username",status.username||""]]){const field=$<HTMLInputElement>(id);if(active!==field)field.value=String(value)}$<HTMLInputElement>("mqtt-tls").checked=status.tls!==false;
  const profile=String(status.profile||detectMqttProfile(status));if(active!==$("mqtt-profile"))$<HTMLSelectElement>("mqtt-profile").value=MQTT_PROFILES[profile]?profile:"manual";renderMqttProfileNote();
  $<HTMLButtonElement>("mqtt-connect").disabled=mqttConnected;$<HTMLButtonElement>("mqtt-disconnect").disabled=!status.enabled;$<HTMLButtonElement>("mqtt-send").disabled=!mqttConnected;
  const brokerNote=status.error?`Ошибка MQTT: ${status.error}`:mqttConnected?`MQTT подключён: ${status.topic}.`:status.hasPassword?"MQTT выключен; настройки сохранены локально.":"MQTT подключается только после нажатия кнопки.",onemeshNote=onemeshAvailable?`Публичный чат Москвы загружен через OneMesh API (${Array.isArray(payload?.onemeshMessages)?payload.onemeshMessages.length:0} сообщений).`:onemesh.error?`OneMesh API недоступен: ${onemesh.error}`:"OneMesh API ещё не ответил.";$("mqtt-config-result").textContent=`${onemeshNote} ${brokerNote}`;renderMqtt();
}
async function loadMqtt(){try{applyMqttState(await fetchJson("/mqtt-chat.json"))}catch(error){$("mqtt-state").textContent="недоступен";$("mqtt-state").className="pill bad";$("mqtt-config-result").textContent=`MQTT-сервис недоступен: ${errorText(error)}`}}
function detectMqttProfile(status:AnyRecord){for(const [key,p] of Object.entries(MQTT_PROFILES))if(status.host===p.host&&Number(status.port)===p.port&&status.topic===p.topic&&status.username===p.username&&Boolean(status.tls)===p.tls)return key;return"manual"}
function renderMqttProfileNote(){const key=$<HTMLSelectElement>("mqtt-profile").value,p=MQTT_PROFILES[key];$("mqtt-profile-note").textContent=p?p.note:"Ручной режим: все параметры задаются ниже.";$("mqtt-settings-summary").textContent=p?p.label:"Ручные настройки"}
function selectMqttProfile(){const key=$<HTMLSelectElement>("mqtt-profile").value,p=MQTT_PROFILES[key];if(p){$<HTMLInputElement>("mqtt-host").value=p.host;$<HTMLInputElement>("mqtt-port").value=String(p.port);$<HTMLInputElement>("mqtt-topic").value=p.topic;$<HTMLInputElement>("mqtt-username").value=p.username;$<HTMLInputElement>("mqtt-password").value=p.password;$<HTMLInputElement>("mqtt-tls").checked=p.tls}renderMqttProfileNote()}
function mqttConfigBody(action:"save"|"connect"|"disconnect"){return{action,profile:$<HTMLSelectElement>("mqtt-profile").value,host:$<HTMLInputElement>("mqtt-host").value.trim(),port:Number($<HTMLInputElement>("mqtt-port").value),topic:$<HTMLInputElement>("mqtt-topic").value.trim(),username:$<HTMLInputElement>("mqtt-username").value,password:$<HTMLInputElement>("mqtt-password").value,tls:$<HTMLInputElement>("mqtt-tls").checked}}
async function configureMqtt(action:"save"|"connect"|"disconnect"){
  const result=$("mqtt-config-result");result.textContent=action==="connect"?"Сохраняю и подключаю к брокеру…":action==="save"?"Сохраняю настройки…":"Отключаю…";
  try{const response=await fetch("/mqtt-chat/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(mqttConfigBody(action))}),payload=await response.json();if(!response.ok)throw new Error(payload.error||`HTTP ${response.status}`);$<HTMLInputElement>("mqtt-password").value="";applyMqttState(payload);result.textContent=action==="save"?"Настройки сохранены на Orange Pi. MQTT остаётся выключенным.":action==="disconnect"?"MQTT отключён; сохранённые поля оставлены для следующего подключения.":"Настройки сохранены. Подключаю MQTT…";if(action==="connect")window.setTimeout(()=>void loadMqtt(),1200)}catch(error){result.textContent=`Ошибка: ${errorText(error)}`}
}
async function sendMqtt(event:SubmitEvent){
  event.preventDefault();const field=$<HTMLTextAreaElement>("mqtt-message"),text=field.value.trim(),result=$("mqtt-send-result");if(!text)return;result.className="send-feedback muted";result.textContent="Публикую в MQTT…";
  const owner=ownerConfig||nodes.get((myNode||ownNodeNum)>>>0)?.user||fallbackOwner;
  try{const response=await fetch("/mqtt-chat/publish",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text,sender:owner.longName||fallbackOwner.longName,senderId:(myNode||ownNodeNum)?hex((myNode||ownNodeNum)>>>0):""})}),payload=await response.json();if(!response.ok)throw new Error(payload.error||`HTTP ${response.status}`);field.value="";$("mqtt-chars").textContent="0/500";result.className="send-feedback ok";result.textContent="Опубликовано в MQTT. В LoRa-журнал сообщение попадёт только если плата отдельно примет его по радио.";await loadMqtt()}catch(error){result.className="send-feedback bad";result.textContent=`Ошибка MQTT: ${errorText(error)}`}
}
function recordMeshtasticMqttMessage(packet:AnyRecord){
  const ts=Math.floor(new Date(packet.rxTime).getTime()/1000)||Math.floor(Date.now()/1000),from=Number(packet.from)>>>0,message:MqttMessage={id:String(packet.id||`${from}-${ts}-${String(packet.data)}`),ts,direction:from===(myNode||ownNodeNum)?"tx":"rx",sender:from?nodeName(from):"Meshtastic MQTT",senderId:from?hex(from):"",text:String(packet.data),topic:`Meshtastic · канал ${Number(packet.channel)||0}`,source:"meshtastic"};
  if(!mqttMeshMessages.some(item=>item.id===message.id)){mqttMeshMessages.push(message);mqttMeshMessages=mqttMeshMessages.slice(-300);localStorage.setItem("barbienode-mqtt-mesh",JSON.stringify(mqttMeshMessages));mqttMessages.push(message);mqttMessages=mqttMessages.slice(-500);renderMqtt()}
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
  const scope=selectedMessageChannel==="all"||selectedMessageChannel==="direct"?"all":String(selectedMessageChannel);
  try{
    const response=await fetch("/notifications/read",{method:"POST",headers:{"Content-Type":"text/plain"},body:scope});
    if(!response.ok)throw new Error(`${response.status}`);
    if(selectedMessageChannel==="all"){unreadChannels.clear();unreadDirectPeers.clear()}else if(selectedMessageChannel==="direct"){if(selectedDirectPeer==="all")unreadDirectPeers.clear();else unreadDirectPeers.delete(selectedDirectPeer)}else unreadChannels.delete(selectedMessageChannel);
    updateUnreadIndicators();await loadNotificationStatus();
  }catch{}
}

type SettingsEntry={id:string;label:string;kind:"radio"|"module"|"channel"|"owner";key?:string;index?:number;value:AnyRecord};
const bytesToBase64=(bytes:Uint8Array)=>{let s="";for(const b of bytes)s+=String.fromCharCode(b);return btoa(s)};
const base64ToBytes=(text:string)=>{const raw=atob(text),bytes=new Uint8Array(raw.length);for(let i=0;i<raw.length;i++)bytes[i]=raw.charCodeAt(i);return bytes};
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
function exportSettings(suffix=""){
  const radio=Object.fromEntries(radioConfigs),observedRole=observedNodeRole();
  if(defaultRadioConfigs.has("device")&&observedRole!==undefined)radio.device=create(Protobuf.Config.Config_DeviceConfigSchema,{role:observedRole,nodeInfoBroadcastSecs:PROJECT_NODE_INFO_BROADCAST_SECS});
  const observedFixed=observedFixedPosition();if(defaultRadioConfigs.has("position")&&observedFixed!==undefined)radio.position=projectPositionConfig(observedFixed);
  const backup={exportedAt:new Date().toISOString(),node:ownNodeNum?hex(ownNodeNum):undefined,radio,modules:Object.fromEntries(moduleConfigs),channels:[...channels.values()].filter(c=>c.$typeName),owner:ownerConfig,local:{listenBeforeTalk:listenBeforeTalkEnabled}},blob=new Blob([settingsStringify(backup)],{type:"application/json"}),link=document.createElement("a");link.href=URL.createObjectURL(blob);link.download=`barbienode-config-${new Date().toISOString().slice(0,10)}${suffix?`-${suffix}`:""}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(link.href),1000);$("settings-state").textContent="Резервная копия скачана. Она может содержать пароли и ключи каналов — храните её безопасно.";
}

type SavedFixedPosition={lat:number;lon:number;alt?:number;savedAt:number};
let phonePositionAccuracy:number|undefined;
function savedFixedPosition():SavedFixedPosition|undefined{
  try{
    const value=JSON.parse(localStorage.getItem(SAVED_FIXED_POSITION_STORAGE_KEY)||"null"),lat=Number(value?.lat),lon=Number(value?.lon),alt=Number(value?.alt);
    if(!Number.isFinite(lat)||lat< -90||lat>90||!Number.isFinite(lon)||lon< -180||lon>180)return;
    return {lat,lon,alt:Number.isFinite(alt)?alt:undefined,savedAt:Number(value?.savedAt)||0};
  }catch{return}
}
function currentOwnMeshPosition():SavedFixedPosition|undefined{
  const position=nodes.get((myNode||ownNodeNum)>>>0)?.position,lat=Number(position?.latitudeI)/1e7,lon=Number(position?.longitudeI)/1e7,alt=Number(position?.altitude);
  if(!Number.isFinite(lat)||lat< -90||lat>90||!Number.isFinite(lon)||lon< -180||lon>180||(!lat&&!lon))return;
  return {lat,lon,alt:Number.isFinite(alt)?alt:undefined,savedAt:Math.floor(Date.now()/1000)};
}
function observedNodeRole(){
  const role=Number(ownerConfig?.role??nodes.get((myNode||ownNodeNum)>>>0)?.user?.role);
  return role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT||role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE?role:undefined;
}
function observedFixedPosition(){
  const position=nodes.get((myNode||ownNodeNum)>>>0)?.position,source=Number(position?.locationSource);
  if(source===Protobuf.Mesh.Position_LocSource.LOC_MANUAL)return true;
  if(source===Protobuf.Mesh.Position_LocSource.LOC_EXTERNAL)return false;
  return connectionConfigured&&currentOwnMeshPosition()?true:undefined;
}
function renderMobilityControls(){
  const deviceConfig=radioConfigs.get("device"),positionConfig=radioConfigs.get("position"),roleState=$("node-role-state"),client=$<HTMLButtonElement>("role-client"),mute=$<HTMLButtonElement>("role-client-mute"),positionState=$("position-mode-state"),send=$<HTMLButtonElement>("phone-position-send"),restore=$<HTMLButtonElement>("fixed-position-restore");
  const deviceReady=Boolean(deviceConfig)&&!defaultRadioConfigs.has("device"),observedRole=observedNodeRole(),roleReady=deviceReady||observedRole!==undefined,receivedPosition=Boolean(positionConfig)&&!defaultRadioConfigs.has("position"),observedFixed=observedFixedPosition(),positionReady=receivedPosition||observedFixed!==undefined;
  if(!roleReady){roleState.textContent="Роль ещё не получена";roleState.className="pill warn";client.disabled=true;mute.disabled=true}
  else{
    const role=deviceReady?Number(deviceConfig.role):observedRole!,supported=role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT||role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE;
    roleState.textContent=(role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT?"CLIENT · стационарная":role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE?"CLIENT_MUTE · мобильная":`Другая роль (${role})`)+(deviceReady?"":" · NodeInfo");
    roleState.className=`pill ${supported?role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT?"ok":"warn":"bad"}`;
    client.disabled=!device||!connectionConfigured||!supported||role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT;
    mute.disabled=!device||!connectionConfigured||!supported||role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE;
  }
  if(!positionReady){positionState.textContent="Режим позиции ещё не получен";positionState.className="pill warn"}
  else{const fixed=receivedPosition?Boolean(positionConfig.fixedPosition):observedFixed!;positionState.textContent=(fixed?"Фиксированная позиция":"Позиция телефона разрешена")+(receivedPosition?"":" · NodeInfo");positionState.className=`pill ${fixed?"warn":"ok"}`}
  send.disabled=!device||!positionReady;
  restore.hidden=!savedFixedPosition();restore.disabled=!device;
}
async function setNodeRole(nextRole:number){
  if(!device||!connectionConfigured)return;
  const received=radioConfigs.get("device"),observedRole=observedNodeRole(),hasReceived=Boolean(received)&&!defaultRadioConfigs.has("device"),current=hasReceived?received:create(Protobuf.Config.Config_DeviceConfigSchema,{role:observedRole,nodeInfoBroadcastSecs:PROJECT_NODE_INFO_BROADCAST_SECS}),result=$("node-role-result");
  if(observedRole===undefined&&!hasReceived)return void(result.textContent="Текущая роль ещё не подтверждена платой.");
  if(![Protobuf.Config.Config_DeviceConfig_Role.CLIENT,Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE].includes(nextRole))return;
  const label=nextRole===Protobuf.Config.Config_DeviceConfig_Role.CLIENT?"CLIENT":"CLIENT_MUTE";
  if(!confirm(`Переключить ноду в ${label}? Перед изменением будет скачана резервная копия. Радиопрофиль, каналы и мощность не меняются.`))return;
  const buttons=[$<HTMLButtonElement>("role-client"),$<HTMLButtonElement>("role-client-mute")];buttons.forEach(button=>button.disabled=true);exportSettings(`before-role-${label.toLowerCase().replace("_","-")}`);result.textContent=`Применяю ${label}…`;
  try{
    const value=create(Protobuf.Config.Config_DeviceConfigSchema,{...current,role:nextRole}),editor=device.meshClient.config.editor as AnyRecord;editor.setRadioSection("device",value);const committed=await editor.commit();if(committed.status==="error")throw committed.error;
    radioConfigs.set("device",value);defaultRadioConfigs.delete("device");if(ownerConfig)ownerConfig={...ownerConfig,role:nextRole};refreshSettingsOptions();renderMobilityControls();result.textContent=`Роль сохранена: ${label}. ${label==="CLIENT_MUTE"?"Нода не будет ретранслировать чужие пакеты.":"Обычная ретрансляция CLIENT снова разрешена."}`;
    void device.meshClient.config.getRadio(Protobuf.Admin.AdminMessage_ConfigType.DEVICE_CONFIG);
  }catch(error){result.textContent=`Ошибка смены роли: ${errorText(error)}`;renderMobilityControls()}
}
function readPhonePosition(){
  const result=$("phone-position-result"),button=$<HTMLButtonElement>("phone-position-read");
  if(!navigator.geolocation){result.textContent="Этот браузер не предоставляет GPS. Введите координаты вручную.";return}
  button.disabled=true;result.textContent="Запрашиваю у телефона разовую GPS-позицию…";
  navigator.geolocation.getCurrentPosition(position=>{
    const {latitude,longitude,altitude,accuracy}=position.coords;$<HTMLInputElement>("phone-position-lat").value=latitude.toFixed(7);$<HTMLInputElement>("phone-position-lon").value=longitude.toFixed(7);$<HTMLInputElement>("phone-position-alt").value=altitude===null?"":String(Math.round(altitude));phonePositionAccuracy=Number.isFinite(accuracy)?accuracy:undefined;result.textContent=`GPS получен локально${phonePositionAccuracy!==undefined?` · точность около ${Math.round(phonePositionAccuracy)} м`:""}. Для передачи нажмите отдельную кнопку.`;button.disabled=false;
  },error=>{const insecure=!window.isSecureContext?" Страница открыта по HTTP; некоторые браузеры разрешают GPS только через HTTPS. Можно ввести координаты вручную.":"";result.textContent=`GPS телефона недоступен: ${error.message||`код ${error.code}`}.${insecure}`;button.disabled=false;},{enableHighAccuracy:true,maximumAge:0,timeout:20000});
}
async function allowPhonePosition(){
  if(!device)throw new Error("плата ещё не подключена");
  const received=radioConfigs.get("position"),hasReceived=Boolean(received)&&!defaultRadioConfigs.has("position"),observedFixed=observedFixedPosition(),config=hasReceived?received:observedFixed===undefined?undefined:projectPositionConfig(observedFixed);if(!config)throw new Error("конфигурация позиции ещё не получена");
  if(!config.fixedPosition)return;
  const old=currentOwnMeshPosition();if(old)localStorage.setItem(SAVED_FIXED_POSITION_STORAGE_KEY,JSON.stringify(old));
  exportSettings("before-phone-position");
  const value=create(Protobuf.Config.Config_PositionConfigSchema,{...config,fixedPosition:false}),editor=device.meshClient.config.editor as AnyRecord;editor.setRadioSection("position",value);const committed=await editor.commit();if(committed.status==="error")throw committed.error;radioConfigs.set("position",value);defaultRadioConfigs.delete("position");refreshSettingsOptions();renderMobilityControls();
}
async function sendPhonePosition(){
  if(!device)return;
  const lat=Number($<HTMLInputElement>("phone-position-lat").value),lon=Number($<HTMLInputElement>("phone-position-lon").value),altField=$<HTMLInputElement>("phone-position-alt").value.trim(),alt=altField===""?undefined:Number(altField),result=$("phone-position-result"),button=$<HTMLButtonElement>("phone-position-send"),fixed=defaultRadioConfigs.has("position")?observedFixedPosition()!==false:Boolean(radioConfigs.get("position")?.fixedPosition);
  if(!Number.isFinite(lat)||lat< -90||lat>90||!Number.isFinite(lon)||lon< -180||lon>180||alt!==undefined&&!Number.isFinite(alt))return void(result.textContent="Проверьте широту −90…90, долготу −180…180 и необязательную высоту.");
  const warning=fixed?" Фиксированная позиция будет отключена; её текущая точка по возможности сохранится в этом браузере для ручного возврата. Перед изменением скачается резервная копия.":"";
  if(!confirm(`Передать плате одну позицию ${lat.toFixed(5)}, ${lon.toFixed(5)}? Она может быть опубликована по LoRa с точностью, разрешённой настройками каналов.${warning}`))return;
  button.disabled=true;result.textContent=fixed?"Сохраняю резервную копию, отключаю фиксированную позицию и передаю GPS…":"Передаю одну позицию плате…";
  try{
    await allowPhonePosition();const now=Math.floor(Date.now()/1000),position=create(Protobuf.Mesh.PositionSchema,{latitudeI:Math.round(lat*1e7),longitudeI:Math.round(lon*1e7),altitude:alt===undefined?undefined:Math.round(alt),time:now,timestamp:now,locationSource:Protobuf.Mesh.Position_LocSource.LOC_EXTERNAL,altitudeSource:alt===undefined?Protobuf.Mesh.Position_AltSource.ALT_UNSET:Protobuf.Mesh.Position_AltSource.ALT_EXTERNAL,gpsAccuracy:phonePositionAccuracy===undefined?0:Math.max(0,Math.round(phonePositionAccuracy*1000))}),id=await device.setPosition(position);
    if(myNode)addNode(myNode,{position,lastHeard:now});scheduleNodeCacheSave();scheduleMap();renderNodes();renderMobilityControls();result.textContent=`Позиция #${id} принята платой. Это разовая передача, а не фоновое слежение; получение другими нодами не подтверждено.`;
    void device.meshClient.config.getRadio(Protobuf.Admin.AdminMessage_ConfigType.POSITION_CONFIG);
  }catch(error){result.textContent=`Позиция не передана: ${errorText(error)}`}finally{button.disabled=false}
}
async function restoreFixedPosition(){
  if(!device)return;const saved=savedFixedPosition(),result=$("phone-position-result"),button=$<HTMLButtonElement>("fixed-position-restore");if(!saved)return void renderMobilityControls();
  if(!confirm(`Вернуть сохранённую фиксированную точку ${saved.lat.toFixed(5)}, ${saved.lon.toFixed(5)}? Meshtastic может сразу передать её по LoRa согласно точности каналов. Перед изменением скачается резервная копия.`))return;
  button.disabled=true;exportSettings("before-fixed-position-restore");result.textContent="Возвращаю фиксированную позицию…";
  try{const id=await device.setFixedPosition(saved.lat,saved.lon),config=radioConfigs.get("position");radioConfigs.set("position",create(Protobuf.Config.Config_PositionConfigSchema,{...(config||projectPositionConfig(true)),fixedPosition:true}));defaultRadioConfigs.delete("position");renderMobilityControls();result.textContent=`Фиксированная позиция возвращена, пакет #${id} принят платой. Получение другими нодами не подтверждено.`;void device.meshClient.config.getRadio(Protobuf.Admin.AdminMessage_ConfigType.POSITION_CONFIG)}catch(error){result.textContent=`Не удалось вернуть фиксированную позицию: ${errorText(error)}`}finally{button.disabled=false}
}

function emptyChannel(index:number){return create(Protobuf.Channel.ChannelSchema,{index,role:Protobuf.Channel.Channel_Role.DISABLED,settings:create(Protobuf.Channel.ChannelSettingsSchema,{})})}
async function commitManagedChannel(value:AnyRecord,success:string){
  const result=$("channel-manager-result");if(!device){result.textContent="Плата ещё не подключена.";return false}
  exportSettings("before-channel-change");result.textContent="Резервная копия скачана. Применяю канал…";
  try{const editor=device.meshClient.config.editor as AnyRecord;editor.setChannel(value);const committed=await editor.commit();if(committed.status==="error")throw committed.error;channels.set(Number(value.index),value);renderChannels();renderChannelManager();refreshSettingsOptions();result.textContent=success;return true}catch(error){result.textContent=`Ошибка канала: ${errorText(error)}`;return false}
}
function renderChannelManager(){
  const box=$("channel-manager");box.replaceChildren();
  for(let index=0;index<8;index++){
    const channel=channels.get(index)||emptyChannel(index),role=Number(channel.role)||0,primary=role===Protobuf.Channel.Channel_Role.PRIMARY,card=document.createElement("section");card.className="channel-editor";card.dataset.index=String(index);
    if(role===Protobuf.Channel.Channel_Role.DISABLED){
      card.classList.add("channel-slot-free");const label=document.createElement("span");label.innerHTML=`<strong>Слот ${index}</strong><br><span class="muted">Свободен</span>`;const add=document.createElement("button");add.type="button";add.className="secondary";add.textContent="Добавить канал";add.addEventListener("click",()=>{const psk=crypto.getRandomValues(new Uint8Array(32));channels.set(index,create(Protobuf.Channel.ChannelSchema,{index,role:Protobuf.Channel.Channel_Role.SECONDARY,settings:create(Protobuf.Channel.ChannelSettingsSchema,{psk})}));renderChannelManager()});card.append(label,add);box.append(card);continue;
    }
    card.innerHTML='<div class="channel-editor-head"><strong></strong><span class="pill"></span></div><div class="channel-editor-grid"><label>Имя<input data-field="name" maxlength="11" placeholder="До 11 байт UTF‑8"></label><label>Точность позиции, бит<input data-field="precision" type="number" min="0" max="32" step="1"></label><label class="channel-wide">Новый ключ Base64<input data-field="key" type="password" autocomplete="new-password" placeholder="Оставьте пустым, чтобы сохранить текущий"><small class="channel-key-note"></small></label><label class="channel-check"><input data-field="muted" type="checkbox">Приглушить канал</label><label class="channel-check"><input data-field="aead" type="checkbox">AEAD (только совместимые ноды)</label><label class="channel-check"><input data-field="uplink" type="checkbox">MQTT uplink</label><label class="channel-check"><input data-field="downlink" type="checkbox">MQTT downlink</label></div><div class="channel-editor-actions"><button data-action="generate" type="button" class="secondary">Новый ключ 256 бит</button><button data-action="copy" type="button" class="secondary">Копировать новый ключ</button><button data-action="save" type="button">Сохранить</button></div>';
    card.querySelector("strong")!.textContent=`Слот ${index} · ${channel.settings?.name||channelName(index)}`;const badge=card.querySelector<HTMLElement>(".pill")!;badge.textContent=primary?"Основной":"Вторичный";badge.classList.add(primary?"ok":"warn");
    const field=<T extends HTMLInputElement>(name:string)=>card.querySelector<T>(`[data-field="${name}"]`)!;field("name").value=channel.settings?.name||"";field("precision").value=String(channel.settings?.moduleSettings?.positionPrecision??0);field("muted").checked=Boolean(channel.settings?.moduleSettings?.isMuted);field("aead").checked=Boolean(channel.settings?.useAead);field("uplink").checked=Boolean(channel.settings?.uplinkEnabled);field("downlink").checked=Boolean(channel.settings?.downlinkEnabled);const currentKey=channel.settings?.psk instanceof Uint8Array?channel.settings.psk:new Uint8Array();card.querySelector<HTMLElement>(".channel-key-note")!.textContent=`Текущий ключ скрыт · ${currentKey.length||0} байт`;
    card.querySelector<HTMLButtonElement>('[data-action="generate"]')!.addEventListener("click",()=>{field("key").value=bytesToBase64(crypto.getRandomValues(new Uint8Array(32)));card.querySelector<HTMLElement>(".channel-key-note")!.textContent="Подготовлен новый случайный ключ · 32 байта"});
    card.querySelector<HTMLButtonElement>('[data-action="copy"]')!.addEventListener("click",async()=>{const key=field("key");if(!key.value)return void($("channel-manager-result").textContent="Сначала создайте или введите новый ключ.");try{await navigator.clipboard.writeText(key.value)}catch{key.select();document.execCommand("copy");key.setSelectionRange(0,0)}$("channel-manager-result").textContent="Новый ключ скопирован. Передавайте его только участникам канала."});
    card.querySelector<HTMLButtonElement>('[data-action="save"]')!.addEventListener("click",async()=>{const name=field("name").value.trim(),nameBytes=new TextEncoder().encode(name).length;if(nameBytes>11)return void($("channel-manager-result").textContent="Имя канала должно занимать не более 11 байт UTF‑8.");let psk=currentKey;const entered=field("key").value.trim();try{if(entered)psk=base64ToBytes(entered)}catch{return void($("channel-manager-result").textContent="Ключ должен быть корректной строкой Base64.")}if(![1,16,32].includes(psk.length))return void($("channel-manager-result").textContent="Ключ должен содержать 1, 16 или 32 байта.");const uplink=field("uplink").checked,downlink=field("downlink").checked;if((uplink||downlink)&&!confirm("Включить MQTT-направление для этого канала? Оно начнёт работать, если позже будет включён общий модуль MQTT."))return;const value=create(Protobuf.Channel.ChannelSchema,{...channel,index,role:primary?Protobuf.Channel.Channel_Role.PRIMARY:Protobuf.Channel.Channel_Role.SECONDARY,settings:create(Protobuf.Channel.ChannelSettingsSchema,{...channel.settings,name,psk,uplinkEnabled:uplink,downlinkEnabled:downlink,useAead:field("aead").checked,moduleSettings:{...channel.settings?.moduleSettings,positionPrecision:Math.max(0,Math.min(32,Number(field("precision").value)||0)),isMuted:field("muted").checked}})});if(!confirm(`Сохранить параметры канала «${name||`слот ${index}`}»? Перед изменением будет скачана резервная копия.`))return;await commitManagedChannel(value,"Канал сохранён.")});
    if(!primary){const remove=document.createElement("button");remove.type="button";remove.className="danger";remove.textContent="Отключить канал";remove.addEventListener("click",async()=>{if(!confirm(`Отключить канал «${channel.settings?.name||`слот ${index}`}»? Перед изменением будет скачана резервная копия.`))return;await commitManagedChannel(emptyChannel(index),"Канал отключён; слот свободен.")});card.querySelector(".channel-editor-actions")!.append(remove)}
    box.append(card);
  }
}
function hydrateSettingsFromEditor(){if(!device)return;const editor=device.meshClient.config.editor as AnyRecord,radio=editor.radio?.value??editor.radio?.peek?.(),modules=editor.modules?.value??editor.modules?.peek?.(),editorChannels=editor.channels?.value??editor.channels?.peek?.();if(radio)for(const [key,value] of Object.entries(radio))if(value){radioConfigs.set(key,value as AnyRecord);defaultRadioConfigs.delete(key)}if(modules)for(const [key,value] of Object.entries(modules))if(value){moduleConfigs.set(key,value as AnyRecord);defaultModuleConfigs.delete(key)}if(editorChannels instanceof Map)for(const [index,value] of editorChannels)channels.set(Number(index),value);renderChannels();renderChannelManager();refreshSettingsOptions();renderTxPowerMetric();renderModemPreset();renderMobilityControls()}
async function applySettings(){
  const entry=selectedSettingsEntry();if(!entry||!device)return;
  let value:AnyRecord;try{value=settingsParse($<HTMLTextAreaElement>("settings-json").value)}catch(e){$("settings-state").textContent=`Ошибка JSON: ${errorText(e)}`;return}
  if(!confirm(`Применить раздел «${entry.label}»? Плата может перезагрузиться или отключиться от Wi‑Fi.`))return;
  const button=$<HTMLButtonElement>("settings-apply");button.disabled=true;$("settings-state").textContent="Отправляю конфигурацию на плату…";
  try{const editor=device.meshClient.config.editor as AnyRecord;if(entry.kind==="radio")editor.setRadioSection(entry.key,value);else if(entry.kind==="module")editor.setModuleSection(entry.key,value);else if(entry.kind==="channel")editor.setChannel(value);else editor.setOwner(value);const committed=await editor.commit();if(committed.status==="error")throw committed.error;if(entry.kind==="radio")radioConfigs.set(entry.key!,value);else if(entry.kind==="module")moduleConfigs.set(entry.key!,value);else if(entry.kind==="channel")channels.set(entry.index!,value);else{ownerConfig=value;renderOwnIdentity()}settingsEditing=false;$("settings-state").textContent="Настройки применены. Если раздел требует перезагрузки, соединение восстановится автоматически.";refreshSettingsOptions();if(entry.kind==="radio"&&entry.key==="lora")renderTxPowerMetric()}catch(e){$("settings-state").textContent=`Ошибка применения: ${errorText(e)}`}finally{button.disabled=false}
}

async function loadArchive() {
  const result:Message[]=[];
  for (const path of ["/nightbot.previous.jsonl","/nightbot.jsonl","/nightbot.sent.jsonl","/server-sent.jsonl"]) {
    try {
      const response=await fetch(path,{cache:"no-store"});
      if(!response.ok) continue;
      for(const line of (await response.text()).split(/\r?\n/)) {
        try {
          const row=JSON.parse(line);
          if(row && typeof row.ts==="number" && typeof row.text==="string") {
            result.push({...row,channel:Number(row.channel)||0,source:row.automatic?"автоматический Ping":path==="/server-sent.jsonl"?"исходящие Orange Pi":path.includes("sent")?"исходящие ESP":"архив ESP"});
            if(typeof row.from==="string" && row.from.startsWith("!")){const num=Number.parseInt(row.from.slice(1),16),known=nodes.get(num)||{};addNode(num,{lastHeard:Math.max(known.lastHeard||0,row.ts),lastRssi:known.lastRssi??row.rssi,lastSnr:known.lastSnr??row.snr,signalAt:known.signalAt??row.ts})}
          }
        } catch {}
      }
    } catch {}
  }
  const sent:Message[]=JSON.parse(localStorage.getItem("meshtastic-esp-sent")||"[]");
  const latestBoardTime=Math.max(0,...result.filter(m=>m.source==="архив ESP"&&hasValidMessageTime(m.ts)).map(m=>m.ts));
  const wallTime=Math.floor(Date.now()/1000);
  // The ESP RTC can temporarily be ahead of the browser clock. Preserve the
  // truthful local timestamp shown on our outgoing card, but compare it on the
  // archive's time axis so a successfully sent message stays at the top.
  archiveClockAheadSeconds=latestBoardTime>wallTime+300?latestBoardTime-wallTime:0;
  archiveFutureTimestamps=result.filter(m=>m.source==="архив ESP"&&hasValidMessageTime(m.ts)&&m.ts>wallTime+300).length;
  // A freshly rebooted board can archive packets with ts=0 before its clock is
  // synchronized. They are retained for inspection, but must not create an
  // unread badge for an event that cannot be placed after the previous poll.
  if(archiveInitialized)for(const m of result)if(m.event==="rx"&&hasValidMessageTime(m.ts)&&!archiveSeenKeys.has(messageIdentity(m)))markUnread(m);
  for(const m of result)rememberArchiveMessage(m);
  const retained=messages.filter(m=>m.source!=="архив ESP"&&m.source!=="исходящие ESP"&&m.source!=="исходящие Orange Pi");
  messages=mergeMessageCopies([...result,...sent,...retained]).slice(-MAX_BROWSER_MESSAGES);
  archiveInitialized=true;renderMessageChannelTabs();renderMessages();renderNodes();renderAimPingReplies();
  if(messagesViewActive())void acknowledgeViewedMessages();
}

function normalizeBrowserHistory(exactEpoch:number,offsetSeconds:number){
  if(offsetSeconds<=300){
    const sent:Message[]=JSON.parse(localStorage.getItem("meshtastic-esp-sent")||"[]");
    const timestamps=[
      ...airEvents.map(event=>event.ts),...airtime.map(sample=>sample.ts),...aimSamples.map(sample=>sample.ts),
      ...sent.map(message=>message.ts),
      ...[...nodes.values()].flatMap(node=>[node.lastHeard,node.signalAt,...(Array.isArray(node.signalHistory)?node.signalHistory.map((sample:AnyRecord)=>sample.ts):[])])
    ].map(Number).filter(Number.isFinite);
    const latest=Math.max(0,...timestamps);
    if(latest>exactEpoch+300)offsetSeconds=latest-exactEpoch;
  }
  if(offsetSeconds<=300)return;
  const fix=(ts:number)=>ts>exactEpoch+300?Math.max(MESSAGE_TIME_FLOOR,ts-offsetSeconds):ts;
  airEvents=airEvents.map(event=>({...event,ts:fix(event.ts)}));
  airtime=airtime.map(sample=>({...sample,ts:fix(sample.ts)}));
  aimSamples=aimSamples.map(sample=>({...sample,ts:fix(sample.ts)}));
  const sent:Message[]=JSON.parse(localStorage.getItem("meshtastic-esp-sent")||"[]");
  localStorage.setItem("meshtastic-esp-sent",JSON.stringify(sent.map(message=>({...message,ts:fix(message.ts)}))));
  localStorage.setItem("meshtastic-air-events",JSON.stringify(airEvents));
  localStorage.setItem("meshtastic-airtime",JSON.stringify(airtime));
  localStorage.setItem("meshtastic-aim-samples",JSON.stringify(aimSamples));
  for(const [num,node] of nodes){const next={...node};if(Number.isFinite(Number(next.lastHeard)))next.lastHeard=fix(Number(next.lastHeard));if(Number.isFinite(Number(next.signalAt)))next.signalAt=fix(Number(next.signalAt));if(Array.isArray(next.signalHistory))next.signalHistory=next.signalHistory.map((sample:AnyRecord)=>({...sample,ts:fix(Number(sample.ts))}));nodes.set(num,next)}
  void saveNodeCache();
}

async function saveSentArchive(){
  const rows=messages.filter(m=>m.event==="tx").slice(-200).map(m=>JSON.stringify({...m,source:undefined})).join("\n")+"\n";
  const form=new FormData();form.append("file",new Blob([rows],{type:"application/x-ndjson"}),"nightbot.sent.jsonl");
  const response=await fetch("/upload",{method:"POST",body:form});if(!response.ok)throw new Error(`${response.status} ${response.statusText}`);
}

function renderMessages() {
  const list=$("message-list"); list.replaceChildren();
  const visible=messages.filter(m=>!isForeignDirect(m)&&(selectedMessageChannel==="all"?directPeer(m)===0:selectedMessageChannel==="direct"?(directPeer(m)>0&&(selectedDirectPeer==="all"||directPeer(m)===selectedDirectPeer)):directPeer(m)===0&&m.channel===selectedMessageChannel)).slice().sort((a,b)=>messageOrder(b)-messageOrder(a));
  if(!visible.length){list.innerHTML='<p class="muted">В этом потоке сообщений пока нет.</p>';return;}
  for(const m of visible) {
    const fromNum=m.from?.startsWith("!")?Number.parseInt(m.from.slice(1),16)>>>0:0;
    const addressing=messageAddressing(m);
    const directForUs=m.event!=="tx"&&addressing.label==="Лично вам",pingReplyForUs=m.event!=="tx"&&addressing.kind==="ours"&&hasPingReplyCue(m.text);
    const card=document.createElement("article"); card.className=`card${directForUs?" message-direct":pingReplyForUs?" message-ping-reply":""}`;
    const head=document.createElement("div"); head.className="card-head";
    const who=document.createElement(fromNum?"button":"strong"); who.className=fromNum?"node-button":""; who.textContent=m.event==="tx"?"Вы":nodeName(fromNum);
    if(fromNum) who.addEventListener("click",()=>openNode(fromNum));
    const channel=document.createElement(addressing.kind==="direct"?"span":"button");if(channel instanceof HTMLButtonElement){channel.type="button";channel.title=`Показать только канал ${channelName(m.channel)}`;channel.addEventListener("click",()=>selectMessageChannel(m.channel))}channel.className="channel-badge";channel.textContent=addressing.kind==="direct"?"Адресный PKI-пакет":channelName(m.channel);
    const addressingBadge=Object.assign(document.createElement("span"),{className:`addressing-badge ${addressing.kind}`,textContent:addressing.label});
    head.append(who, Object.assign(document.createElement("span"),{className:"meta",textContent:fmtTime(m.ts)}),channel,addressingBadge,Object.assign(document.createElement("span"),{className:"badge",textContent:m.source}));
    const text=document.createElement("div"); text.className="text"; text.textContent=m.text;
    const actions=document.createElement("div");actions.className="message-actions";
    if(m.event!=="tx"){const direct=addressing.kind==="direct"&&fromNum>0,reply=document.createElement("button");reply.type="button";reply.className="secondary";reply.textContent=direct?`Ответить лично ${short(m.from)}`:`Ответить в ${channelName(m.channel)}`;reply.addEventListener("click",()=>{const foreignDirect=direct&&addressing.label!=="Лично вам",context=[m.source==="архив ESP"?"Это архивная запись: данные о ноде и её ключ могли устареть.":"",foreignDirect?`Исходное сообщение было адресовано ${addressing.target?short(addressing.target):"другой ноде"}, а не вашей ноде.`:""] .filter(Boolean).join(" ");direct?setDirect(fromNum,context):setBroadcast();$<HTMLSelectElement>("channel").value=String(m.channel);updateDestination();$<HTMLTextAreaElement>("message").focus()});actions.append(reply)}
    if(m.rssi!==undefined||m.snr!==undefined){const signal=document.createElement("span");signal.className="meta signal";signal.title="RSSI: ближе к 0 — сильнее. SNR: выше — чище.";signal.textContent=[m.rssi!==undefined?`RSSI ${m.rssi} dBm`:"",m.snr!==undefined?`SNR ${Number(m.snr).toFixed(2)} dB`:""].filter(Boolean).join(" · ");actions.append(signal)}
    if(m.event==="tx"&&directPeer(m)>0){const state=deliveryLabel(m),delivery=document.createElement("span");delivery.className=`delivery-state ${state.cls}`;delivery.textContent=state.text;delivery.title=state.title;actions.append(delivery)}
    const details=document.createElement("details"); const summary=document.createElement("summary"); summary.textContent="Полные данные";
    const pre=document.createElement("pre"); pre.textContent=json({...m,addressing,fromName:fromNum?nodeName(fromNum):undefined,fromDecimal:fromNum||undefined,fromHex:fromNum?hex(fromNum):m.from});
    details.append(summary,pre); card.append(head,text,actions,details); list.append(card);
  }
}

function nodeSearchText(n:AnyRecord) { return `${n.num} ${hex(n.num)} ${short(hex(n.num))} ${n.user?.longName||""} ${n.user?.shortName||""}`.toLowerCase(); }
function nodeActivityCounts(){
  let active=0,recent=0;
  for(const n of nodes.values()){
    if(!n.lastHeard)continue;
    const state=activity(n.lastHeard).cls;
    if(state==="activity-live")active++;
    else if(state==="activity-recent")recent++;
  }
  return {active,recent};
}
function updateNodeActivityCounts(){
  const {active,recent}=nodeActivityCounts(),activeElement=document.getElementById("node-active-count"),recentElement=document.getElementById("node-recent-count");
  if(activeElement)activeElement.textContent=String(active);
  if(recentElement)recentElement.textContent=String(recent);
}
function renderNodes() {
  const q=($("node-search") as HTMLInputElement).value.trim().toLowerCase().replace(/^0x/,"");
  const list=$("node-list"); list.replaceChildren();
  updateNodeActivityCounts();
  const values=[...nodes.values()].filter(n=>!q||nodeSearchText(n).includes(q)).sort((a,b)=>compareNodes(a,b,nodeSortKey("node-sort")));
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
  renderAimTargets();
}

function renderAir(){
  const q=$<HTMLInputElement>("air-search")?.value.trim().toLowerCase()||"",kind=$<HTMLSelectElement>("air-kind")?.value||"";
  const filtered=airEvents.filter(e=>(!kind||e.kind===kind)&&(!q||`${e.kind} ${e.id} ${hex(e.from)} ${short(hex(e.from))} ${nodeName(e.from)}`.toLowerCase().includes(q))).slice().reverse();
  $("air-count").textContent=`(${airEvents.length})`;
  const heard=new Set(airEvents.map(e=>e.from).filter(Boolean)).size,withSignal=airEvents.filter(e=>e.rssi!==undefined&&e.rssi!==0),avg=withSignal.length?withSignal.reduce((s,e)=>s+Number(e.rssi),0)/withSignal.length:undefined;
  const summary=$("air-summary");summary.replaceChildren(...[["Пакетов",airEvents.length],["Нод",heard],["Средний RSSI",avg===undefined?"—":`${avg.toFixed(1)} dBm`],["Последний",airEvents.length?fmtTime(airEvents.at(-1)!.ts):"—"]].map(([a,b])=>{const el=document.createElement("div");el.className="metric";el.append(Object.assign(document.createElement("span"),{textContent:String(a)}),Object.assign(document.createElement("b"),{textContent:String(b)}));return el}));
  renderAirContacts();
  const list=$("air-list");list.replaceChildren();
  for(const e of filtered.slice(0,200)){
    const card=document.createElement("article");card.className="card air-event";
    const left=document.createElement("div");left.append(Object.assign(document.createElement("strong"),{textContent:e.kind}),document.createElement("br"),Object.assign(document.createElement("span"),{className:"meta",textContent:`${fmtTime(e.ts)} · #${e.id??"—"}`}));
    const right=document.createElement("div"),route=packetRoute(e);const from=document.createElement("button");from.className="node-button";from.textContent=`${nodeName(e.from)} (${short(hex(e.from))})`;from.addEventListener("click",()=>openNode(e.from));right.append(from,document.createElement("br"),Object.assign(document.createElement("span"),{className:"meta signal",textContent:[`канал ${e.channel}`,e.rssi?`RSSI ${e.rssi} dBm`:"",e.snr?`SNR ${Number(e.snr).toFixed(1)} dB`:"",e.hops!==undefined?`${e.hops} переходов`:"",e.viaMqtt?"MQTT":e.from===myNode?"локально":"LoRa"].filter(Boolean).join(" · ")}),Object.assign(document.createElement("div"),{className:"meta packet-route",textContent:route.path}));
    const details=document.createElement("details"),summary=document.createElement("summary"),pre=document.createElement("pre");summary.textContent="Свойства пакета";pre.textContent=json({time:fmtTime(e.ts),packetId:e.id,application:e.kind,channel:e.channel,source:{id:hex(e.from),name:nodeName(e.from)},destination:e.to===0xffffffff?"^all":{id:hex(e.to),name:nodeName(e.to)},radio:{transport:e.viaMqtt?"MQTT":e.from===myNode?"local":"LoRa",rssi:e.rssi,snr:e.snr},routing:{hopStart:e.hopStart,hopLimit:e.hopLimit,observedHops:e.hops,relayNode:e.relayNode===undefined?undefined:`0x${e.relayNode.toString(16).padStart(2,"0")}`,relayCandidates:route.relayCandidates,path:route.path}});details.append(summary,pre);
    card.append(left,right,details);list.append(card);
  }
  if(!filtered.length)list.innerHTML='<p class="muted">Подходящих пакетов пока нет.</p>';
}

function airContactReasons(event:AirEvent){
  const own=(myNode||ownNodeNum)>>>0,reasons:string[]=[];
  if(own&&event.to===own)reasons.push("адресован нашей ноде");
  if(event.wantResponse)reasons.push("запрошен ответ");
  if(event.wantAck)reasons.push("запрошено подтверждение");
  return reasons;
}

function renderAirContacts(){
  const list=document.getElementById("air-contact-list"),count=document.getElementById("air-contact-count");if(!list||!count)return;
  const contacts=airEvents.filter(event=>event.from&&!isOwnNode(event.from)&&!event.viaMqtt&&airContactReasons(event).length).slice().reverse();
  count.textContent=String(contacts.length);list.replaceChildren();
  for(const event of contacts.slice(0,20)){const row=document.createElement("article");row.className="air-contact-row";const reasons=airContactReasons(event),source=document.createElement("button");source.type="button";source.className="node-button";source.textContent=`${nodeName(event.from)} (${short(hex(event.from))})`;source.addEventListener("click",()=>openNode(event.from));const info=document.createElement("span");info.className="meta";info.textContent=`${fmtTime(event.ts)} · ${event.kind} · ${reasons.join(" · ")}`;const signal=document.createElement("span");signal.className="meta signal";signal.textContent=[event.hops!==undefined?`${event.hops} переходов`:"путь неизвестен",event.rssi!==undefined?`RSSI ${event.rssi} dBm`:"",event.snr!==undefined?`SNR ${Number(event.snr).toFixed(1)} dB`:""].filter(Boolean).join(" · ");row.append(source,info,signal);list.append(row)}
  if(!contacts.length)list.append(Object.assign(document.createElement("p"),{className:"muted",textContent:"В журнале пока нет входящих LoRa-пакетов, адресованных нашей ноде или запрашивающих ответ/подтверждение."}));
}

function packetRoute(e:AirEvent){
  const source=`${nodeName(e.from)} (${short(hex(e.from))})`,ownNum=(myNode||ownNodeNum)>>>0,own=ownerConfig?.longName||nodes.get(ownNum)?.user?.longName||fallbackOwner.longName;
  if(e.viaMqtt)return {path:`${source} → MQTT → ${own}`,relayHex:undefined,relayCandidates:[] as string[]};
  if(e.from===(myNode||ownNodeNum))return {path:`${source} → локально в эфир`,relayHex:undefined,relayCandidates:[] as string[]};
  if(e.hops===0)return {path:`${source} → напрямую → ${own}`,relayHex:undefined,relayCandidates:[] as string[]};
  const relayHex=e.relayNode===undefined?undefined:`0x${e.relayNode.toString(16).padStart(2,"0")}`;
  const hasRelay=e.relayNode!==undefined&&e.relayNode!==0;
  const candidates=hasRelay?[...nodes.values()].filter(n=>(Number(n.num)>>>0)!==(myNode||ownNodeNum)&&(Number(n.num)&0xff)===e.relayNode).map(n=>`${nodeName(n.num)} (${hex(n.num)})`):[];
  const relay=!hasRelay?"последний ретранслятор не указан":candidates.length===1?`${candidates[0]} — совпадение по ${relayHex}`:candidates.length>1?`${relayHex}: ${candidates.length} возможных нод`:`${relayHex}, нода не найдена в NodeDB`;
  return {path:e.hops===undefined?`${source} → путь неизвестен → ${own}`:`${source} → … ${e.hops} пер. → ${relay} → ${own}`,relayHex,relayCandidates:candidates};
}

function drawAirtime(){
  const canvas=$<HTMLCanvasElement>("air-chart"),rect=canvas.getBoundingClientRect(),scale=devicePixelRatio||1;canvas.width=Math.max(300,rect.width*scale);canvas.height=180*scale;const c=canvas.getContext("2d");if(!c)return;c.scale(scale,scale);const w=canvas.width/scale,h=180;c.clearRect(0,0,w,h);c.strokeStyle="#26344c";for(let y=0;y<=4;y++){c.beginPath();c.moveTo(0,y*h/4);c.lineTo(w,y*h/4);c.stroke()}if(airtime.length<2)return;const draw=(key:"channel"|"tx",color:string)=>{c.strokeStyle=color;c.lineWidth=2;c.beginPath();airtime.forEach((s,i)=>{const x=i*w/(airtime.length-1),y=h-Math.min(100,s[key])*h/100;i?c.lineTo(x,y):c.moveTo(x,y)});c.stroke()};draw("channel","#61e7a5");draw("tx","#ffb454");
}

function drawLinkQuality(){
  const canvas=$<HTMLCanvasElement>("link-quality-chart");if(!canvas)return;const rect=canvas.getBoundingClientRect(),scale=devicePixelRatio||1,w=Math.max(300,rect.width),h=210;
  canvas.width=w*scale;canvas.height=h*scale;const c=canvas.getContext("2d");if(!c)return;c.scale(scale,scale);c.clearRect(0,0,w,h);
  const left=48,right=42,top=12,bottom=28,plotW=w-left-right,plotH=h-top-bottom,now=Math.floor(Date.now()/1000),start=now-86400;
  c.font="10px system-ui";c.lineWidth=1;
  for(let i=0;i<=4;i++){const y=top+i*plotH/4;c.strokeStyle="#26344c";c.beginPath();c.moveTo(left,y);c.lineTo(w-right,y);c.stroke();c.fillStyle="#8295ae";c.textBaseline="middle";c.textAlign="right";c.fillText(`${Math.round(-70-i*55/4)} dBm`,left-5,y);c.textAlign="left";c.fillText(`${Math.round(15-i*35/4)} dB`,w-right+5,y)}
  for(let i=0;i<=4;i++){const x=left+i*plotW/4;c.fillStyle="#8295ae";c.textAlign=i===0?"left":i===4?"right":"center";c.textBaseline="top";c.fillText(i===4?"сейчас":`${24-i*6} ч`,x,h-bottom+7)}
  const x=(ts:number)=>left+Math.max(0,Math.min(1,(ts-start)/86400))*plotW,rssiY=(value:number)=>top+Math.max(0,Math.min(1,(-70-value)/55))*plotH,snrY=(value:number)=>top+Math.max(0,Math.min(1,(15-value)/35))*plotH;
  const line=(key:"rssi"|"snr",color:string,y:(value:number)=>number)=>{c.strokeStyle=color;c.lineWidth=2;c.beginPath();let drawing=false;for(const bin of linkQualityBins){const value=bin[key];if(value===null){drawing=false;continue}const px=x(bin.ts+450),py=y(value);if(drawing)c.lineTo(px,py);else{c.moveTo(px,py);drawing=true}}c.stroke()};
  line("rssi","#61e7a5",rssiY);line("snr","#54b9ff",snrY);c.fillStyle="#ffb454";for(const bin of linkQualityBins)if(bin.directRssi!==null){c.beginPath();c.arc(x(bin.ts+450),rssiY(bin.directRssi),3.5,0,Math.PI*2);c.fill()}
}

async function loadLinkQuality(){
  const caption=$("link-quality-caption");
  try{const result=await fetchJson("/link-quality.json") as {bins?:LinkQualityBin[]};linkQualityBins=Array.isArray(result.bins)?result.bins:[];drawLinkQuality();const packets=linkQualityBins.reduce((sum,bin)=>sum+bin.packets,0),direct=linkQualityBins.reduce((sum,bin)=>sum+bin.direct,0),nodes=Math.max(0,...linkQualityBins.map(bin=>bin.nodes));caption.textContent=packets?`${packets} RF‑пакетов за сутки · ${direct} прямых · до ${nodes} нод за 15 минут. RSSI ретрансляций относится к последнему RF‑участку.`:"За последние 24 часа RF‑пакетов пока нет."}catch(error){caption.textContent=`Суточная история недоступна: ${errorText(error)}`}
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
  const nodeNum=num>>>0,n=nodes.get(nodeNum)||{num:nodeNum};
  const box=$("node-details"); box.replaceChildren();
  const title=document.createElement("h2"); title.textContent=nodeName(nodeNum);
  const pre=document.createElement("pre"); pre.className="result"; pre.textContent=json({...n,num:nodeNum,hex:hex(nodeNum),shortId:short(hex(nodeNum))});
  const actions=document.createElement("div"); actions.className="actions";
  for(const [label,action] of [["Личное сообщение",()=>setDirect(nodeNum)],["Запросить позицию",()=>requestPosition(nodeNum)],["Трассировка",()=>traceRoute(nodeNum)]] as const) {
    const b=document.createElement("button"); b.textContent=label; b.addEventListener("click",action); actions.append(b);
  }
  const actionResults=document.createElement("div");actionResults.className="node-action-results";
  for(const [kind,label,fallback] of [["position","Позиция","Пакеты позиции от этой ноды пока не получены."],["trace","Трассировка","Трассировка ещё не запрашивалась."]] as const){const section=document.createElement("section"),heading=document.createElement("strong"),result=document.createElement("div");section.className="node-action-result";heading.textContent=label;result.id=`node-${kind}-result`;result.className="result muted";result.textContent=nodeActionState[kind].get(nodeNum)||fallback;section.append(heading,result);actionResults.append(section)}
  box.append(title,pre,actions,actionResults); ($<HTMLDialogElement>("node-dialog")).showModal();
}
function updateDestination(){
  const channel=Number($<HTMLSelectElement>("channel").value)||0,guidance=$("send-guidance"),send=$<HTMLButtonElement>("send-button");
  $<HTMLLabelElement>("channel-picker").hidden=selectedNode!==undefined;
  const conversation=$("direct-conversation");conversation.hidden=selectedMessageChannel!=="direct"||selectedNode===undefined;
  if(selectedNode===undefined){
    if(selectedMessageChannel==="direct"){$("destination").textContent="Выберите личный диалог";guidance.hidden=false;guidance.textContent="Отправка выключена, пока не выбран конкретный адресат.";guidance.className="send-guidance warn";send.disabled=true;send.textContent="Выберите адресата";return}
    $("destination").textContent=`Широковещательно · ${channelName(channel)}`;guidance.hidden=true;guidance.textContent="";guidance.className="send-guidance";send.disabled=sendInFlight;send.textContent=sendInFlight?"Отправка…":"Отправить";return;
  }
  const keyKnown=hasUsablePublicKey(selectedNode),name=`${nodeName(selectedNode)} (${short(hex(selectedNode))})`,nodeInfoAt=Number(nodes.get(selectedNode)?.nodeInfoAt)||0;
  $("direct-conversation-name").textContent=nodeName(selectedNode);$("direct-conversation-id").textContent=hex(selectedNode);const security=$("direct-conversation-security");security.textContent=keyKnown?`PKI-ключ получен от платы${nodeInfoAt?` ${fmtTime(nodeInfoAt)}`:""} · отпечаток ${publicKeyFingerprint(selectedNode)}. Пакет будет адресным и не появится в публичном канале.`:"Публичного PKI-ключа нет: передача заблокирована до эфира.";security.className=keyKnown?"ok":"bad";
  $("destination").textContent=`Лично и зашифрованно: ${name}`;
  guidance.hidden=false;
  guidance.className=`send-guidance ${keyKnown?directContext?"warn":"ok":"bad"}`;
  guidance.textContent=[directContext,keyKnown?"Публичный ключ получателя известен: личная отправка доступна.":`Личная отправка заблокирована: у платы нет 32-байтного публичного ключа ${name}. Без него возникнет ошибка 39, а пакет не выйдет в эфир. Дождитесь свежего NodeInfo или вернитесь в общий канал.`].filter(Boolean).join("\n");
  send.disabled=!keyKnown||sendInFlight;send.textContent=sendInFlight?"Отправка…":keyKnown?"Отправить лично":"Нет публичного ключа";
}
function setDirect(num:number,context="") { selectedNode=num;selectedDirectPeer=num;selectedMessageChannel="direct";directContext=context;document.querySelectorAll(".tab,.view").forEach(x=>x.classList.remove("active"));document.querySelector<HTMLElement>('.tab[data-message-stream="direct"]')?.classList.add("active");$("messages").classList.add("active");renderMessageChannelTabs();renderMessages();updateDestination();$<HTMLButtonElement>("broadcast").hidden=false;($<HTMLDialogElement>("node-dialog")).close();$<HTMLTextAreaElement>("message").focus()}
function setBroadcast(){selectedNode=undefined;directContext="";updateDestination();$<HTMLButtonElement>("broadcast").hidden=true}
function setNodeAction(kind:NodeActionKind,num:number,text:string){num>>>=0;nodeActionState[kind].set(num,text);const e=document.getElementById(`node-${kind}-result`);if(e)e.textContent=text}
async function requestPosition(num:number){setNodeAction("position",num,`Запрос позиции отправляется ${nodeName(num)} ${hasBackend()?"через Orange Pi":"напрямую через плату"}…`);try{const id=await sendLoRa("position","",hex(num),0);setNodeAction("position",num,`Запрос #${id} принят платой ${new Date().toLocaleTimeString("ru-RU")}. Ждём пакет позиции по LoRa; он может прийти через несколько минут или не прийти.`)}catch(e){setNodeAction("position",num,`Ошибка запроса позиции: ${errorText(e)}`)}}
async function traceRoute(num:number){setNodeAction("trace",num,`Запрос трассировки отправляется ${nodeName(num)} ${hasBackend()?"через Orange Pi":"напрямую через плату"}…`);try{const id=await sendLoRa("trace","",hex(num),0);setNodeAction("trace",num,`Запрос #${id} принят платой ${new Date().toLocaleTimeString("ru-RU")}. Ждём отдельный ответ трассировки по LoRa; пакет позиции не считается таким ответом.`)}catch(e){setNodeAction("trace",num,`Ошибка запроса трассировки: ${errorText(e)}`)}}

const TX_POWER_HARDWARE_CAP_DBM=22;
const MODEM_PRESETS={LONG_FAST:0,MEDIUM_FAST:4} as const;
let modemPresetSwitching=false;
function renderModemPreset(){
  const current=$("modem-preset-current"),detail=$("modem-preset-detail"),medium=$<HTMLButtonElement>("preset-medium-fast"),long=$<HTMLButtonElement>("preset-long-fast");if(!current)return;
  const ready=!defaultRadioConfigs.has("lora"),config=radioConfigs.get("lora"),value=Number(config?.modemPreset),region=Number(config?.region),label=value===MODEM_PRESETS.MEDIUM_FAST?"MediumFast":value===MODEM_PRESETS.LONG_FAST?"LongFast":ready?`Другой профиль (${value})`:"получаю с платы…";current.textContent=label;detail.textContent=ready?`Регион ${region===9?"RU":region===0?"не задан":String(region)} · слот ${config?.channelNum??"—"} · TX ${config?.txPower??"—"} dBm. Параметры LoRa сохраняются отдельно для MediumFast и LongFast.`:"Получаю настройки профиля с платы…";medium.disabled=modemPresetSwitching||!ready||value===MODEM_PRESETS.MEDIUM_FAST;long.disabled=modemPresetSwitching||!ready||value===MODEM_PRESETS.LONG_FAST;medium.classList.toggle("active",value===MODEM_PRESETS.MEDIUM_FAST);long.classList.toggle("active",value===MODEM_PRESETS.LONG_FAST);
  const pendingRaw=sessionStorage.getItem("barbienode-pending-modem-preset");
  if(ready&&pendingRaw){
    try{const pending=JSON.parse(pendingRaw),expected=Number(pending.value),pendingLabel=String(pending.label||"профиль"),result=$("modem-preset-result");result.textContent=value===expected?`Активен ${pendingLabel}. Плата подтвердила профиль; настройки обоих режимов сохранены.`:`Плата не применила ${pendingLabel}; остался ${label}.`;sessionStorage.removeItem("barbienode-pending-modem-preset")}catch{sessionStorage.removeItem("barbienode-pending-modem-preset")}
  }
}
async function applyModemPreset(value:number,label:string){
  const current=radioConfigs.get("lora"),result=$("modem-preset-result");if(modemPresetSwitching)return;if(!device||defaultRadioConfigs.has("lora")||!current){result.textContent="Конфигурация LoRa ещё не получена с платы.";return}if(Number(current.modemPreset)===value)return;
  modemPresetSwitching=true;renderModemPreset();result.textContent=`Сохраняю текущий профиль и загружаю ${label}…`;
  try{
    const currentKey=Number(current.modemPreset)===MODEM_PRESETS.LONG_FAST?"LONG_FAST":"MEDIUM_FAST",targetKey=value===MODEM_PRESETS.LONG_FAST?"LONG_FAST":"MEDIUM_FAST";
    const snapshot={node:ownNodeNum?hex(ownNodeNum):undefined,radio:Object.fromEntries(radioConfigs),modules:Object.fromEntries(moduleConfigs),channels:[...channels.values()].filter(c=>c.$typeName),owner:ownerConfig};
    const savedResponse=await fetch("/radio-profile-memory.json",{method:"POST",headers:{"Content-Type":"application/json"},body:settingsStringify({profile:currentKey,snapshot})});
    if(!savedResponse.ok)throw new Error(`не удалось сохранить ${currentKey}: ${await savedResponse.text()}`);
    const memory=await (await fetch("/radio-profile-memory.json",{cache:"no-store"})).json(),stored=memory?.profiles?.[targetKey]?.lora;
    const restored=stored?settingsParse(JSON.stringify(stored)):current,next=create(Protobuf.Config.Config_LoRaConfigSchema,{...restored,usePreset:true,modemPreset:value,txPower:Math.min(Number(restored.txPower)||Number(current.txPower)||TX_POWER_HARDWARE_CAP_DBM,TX_POWER_HARDWARE_CAP_DBM)});
    const config=create(Protobuf.Config.ConfigSchema,{payloadVariant:{case:"lora",value:next}}),setMessage=create(Protobuf.Admin.AdminMessageSchema,{payloadVariant:{case:"setConfig",value:config}});
    sessionStorage.setItem("barbienode-pending-modem-preset",JSON.stringify({value,label,startedAt:Date.now()}));
    void device.meshClient.sendPacket(toBinary(Protobuf.Admin.AdminMessageSchema,setMessage),Protobuf.Portnums.PortNum.ADMIN_APP,"self",0,false,true).catch(()=>{});
    result.textContent=`Команда ${label} передана плате. Перезапускаю радио для чистого применения…`;
    // The HTTP transport queues the admin packet locally. Give the ESP enough
    // time to persist the LoRa section before restarting its radio stack.
    await new Promise(resolve=>window.setTimeout(resolve,4000));
    const restart=await fetch("/restart",{method:"POST",cache:"no-store"});if(!restart.ok)throw new Error(`не удалось перезапустить плату: HTTP ${restart.status}`);
    result.textContent=`${label} записан. Плата перезапускается; через несколько секунд проверю профиль…`;
    window.setTimeout(()=>location.reload(),6500);
  }catch(error){sessionStorage.removeItem("barbienode-pending-modem-preset");result.textContent=`Ошибка переключения: ${errorText(error)}`;modemPresetSwitching=false;renderModemPreset()}
}

function txPowerInfo(){
  if(defaultRadioConfigs.has("lora"))return undefined;
  const configuredDbm=Number(radioConfigs.get("lora")?.txPower);
  if(!Number.isFinite(configuredDbm))return undefined;
  const appliedDbm=Math.min(configuredDbm,TX_POWER_HARDWARE_CAP_DBM);
  return{configuredDbm,appliedDbm,milliwatts:Math.pow(10,appliedDbm/10)};
}
function txPowerDetail(){const power=txPowerInfo();if(!power)return"Ждём данные от платы";return power.configuredDbm>power.appliedDbm?`В конфигурации: ${power.configuredDbm} dBm · ограничено прошивкой`:`Настройка платы: ${power.configuredDbm} dBm`}
function txPowerTitle(){const power=txPowerInfo();return power?`Применяемый прошивкой предел: ≈${power.milliwatts.toLocaleString("ru-RU",{maximumFractionDigits:1})} мВт до потерь и усиления антенны. Это расчётное, а не измеренное значение.`:"Ждём конфигурацию LoRa от платы"}
function renderTxPowerMetric(){const value=document.getElementById("tx-power-value");if(!value)return;const power=txPowerInfo();value.textContent=power?`${power.appliedDbm} dBm`:"—";const detail=document.getElementById("tx-power-detail");if(detail)detail.textContent=txPowerDetail();const card=value.closest<HTMLElement>(".metric");if(card){card.title=txPowerTitle();const control=card.querySelector<HTMLElement>(".tx-power-control"),ready=!!power;if(control&&control.dataset.ready!==String(ready))control.remove();if(!card.querySelector(".tx-power-control"))addTxPowerControl(card)}}

function renderListenBeforeTalk(){
  const toggle=$<HTMLInputElement>("lbt-toggle"),state=$("lbt-state");
  toggle.checked=listenBeforeTalkEnabled===true;toggle.disabled=!listenBeforeTalkAvailable||listenBeforeTalkSaving;
  state.textContent=!listenBeforeTalkAvailable?"Недоступно":listenBeforeTalkSaving?"Сохраняю…":listenBeforeTalkEnabled?"Включён":"Выключен";
  state.className=`pill ${!listenBeforeTalkAvailable?"bad":listenBeforeTalkEnabled?"ok":"warn"}`;
}
async function loadListenBeforeTalk(){
  try{const status=await fetchJson("/radio/lbt");listenBeforeTalkEnabled=Boolean(status.enabled);listenBeforeTalkAvailable=true;if(!listenBeforeTalkSaving)$("lbt-result").textContent="Проверка CAD выполняется непосредственно перед каждой передачей. Настройка сохраняется после перезагрузки."}
  catch(error){listenBeforeTalkAvailable=false;listenBeforeTalkEnabled=undefined;$("lbt-result").textContent=`Требуется прошивка с управлением LBT: ${errorText(error)}`}
  renderListenBeforeTalk();
}
async function toggleListenBeforeTalk(){
  const toggle=$<HTMLInputElement>("lbt-toggle"),result=$("lbt-result"),next=toggle.checked,previous=listenBeforeTalkEnabled;
  const prompt=next?"Включить LBT/CAD перед передачей?":"Отключить LBT? Нода перестанет проверять занятость канала через CAD перед передачей, что повышает риск коллизий.";
  if(!confirm(prompt)){toggle.checked=previous===true;return}
  exportSettings("before-lbt");listenBeforeTalkSaving=true;renderListenBeforeTalk();result.textContent="Сохраняю режим на плате…";
  try{const response=await fetch("/radio/lbt",{method:"POST",headers:{"Content-Type":"text/plain"},body:String(next),cache:"no-store"}),raw=await response.text();let reply:AnyRecord={};try{reply=JSON.parse(raw)}catch{}if(!response.ok||!reply.ok||Boolean(reply.enabled)!==next)throw new Error(raw||`HTTP ${response.status}`);listenBeforeTalkEnabled=next;listenBeforeTalkAvailable=true;result.textContent=next?"LBT включён и проверен: CAD будет выполняться перед передачей.":"LBT выключен и проверен. Случайная задержка Meshtastic остаётся, но CAD перед передачей пропускается."}
  catch(error){listenBeforeTalkEnabled=previous;result.textContent=`Ошибка сохранения: ${errorText(error)}`}
  finally{listenBeforeTalkSaving=false;renderListenBeforeTalk()}
}

function clockMetric(){
  const correctedNote=clockArchiveCorrected?` Исправлено архивных записей: ${clockArchiveCorrected}.`:"";
  const archiveNote=archiveFutureTimestamps?` В архиве ещё обнаружено ${archiveFutureTimestamps} записей с будущими метками; они ожидают нормализации.`:"";
  if(clockSyncCompleted)return{value:"синхронизированы",detail:`Unix-время источника передано плате ${new Date(lastClockSyncAt).toLocaleString("ru-RU")}; часовой пояс применяется только при показе.${correctedNote}${archiveNote}`};
  if(clockSyncInFlight)return{value:"синхронизация…",detail:"Передаю плате Unix-время подключённого компьютера или телефона без LoRa-передачи."};
  if(clockSyncError)return{value:"ошибка",detail:clockSyncError};
  return{value:"ожидают",detail:`Время будет установлено после подключения браузера к API платы.${archiveNote}`};
}

const CLOCK_SYNC_INTERVAL_MS=60*60*1000;
async function syncBoardClock(force=false){
  if(!device||!connectionConfigured||clockSyncInFlight)return;
  const nowMs=Date.now();
  if(!force&&lastClockSyncAt&&nowMs-lastClockSyncAt<CLOCK_SYNC_INTERVAL_MS)return;
  clockSyncInFlight=true;clockSyncError="";void loadStatus();
  try{
    const now=Math.floor(nowMs/1000);
    const browserOffset=archiveClockAheadSeconds;
    let endpointApplied=false;
    try{
      const response=await fetch("/clock/sync",{method:"POST",headers:{"Content-Type":"text/plain"},body:String(now),cache:"no-store"});
      if(response.ok){const result=await response.json();endpointApplied=Boolean(result.ok);clockArchiveCorrected=Number(result.corrected_archive_records)||0}
    }catch{}
    if(!endpointApplied){
      const message=create(Protobuf.Admin.AdminMessageSchema,{payloadVariant:{case:"setTimeOnly",value:now}});
      await device.meshClient.sendPacket(toBinary(Protobuf.Admin.AdminMessageSchema,message),Protobuf.Portnums.PortNum.ADMIN_APP,"self",0,false,false);
    }
    normalizeBrowserHistory(now,browserOffset);
    lastClockSyncAt=nowMs;clockSyncCompleted=true;
    await loadArchive();
  }catch(error){clockSyncError=`Не удалось синхронизировать RTC: ${errorText(error)}`}
  finally{clockSyncInFlight=false}
  void loadStatus();
}

function addTxPowerControl(card:HTMLElement){
  const power=txPowerInfo();
  card.classList.add("tx-power-metric");
  const form=document.createElement("form");form.className="tx-power-control";form.dataset.ready=String(!!power);
  const row=document.createElement("div");row.className="tx-power-row";
  const input=document.createElement("input");input.type="range";input.min="2";input.max=String(TX_POWER_HARDWARE_CAP_DBM);input.step="1";input.value=String(Math.max(2,Math.min(TX_POWER_HARDWARE_CAP_DBM,power?.configuredDbm??TX_POWER_HARDWARE_CAP_DBM)));input.disabled=!power;input.setAttribute("aria-label","Мощность передатчика, dBm");
  const output=document.createElement("output");output.value=power?`${input.value} dBm`:"—";output.textContent=output.value;
  input.addEventListener("input",()=>{output.value=`${input.value} dBm`;output.textContent=output.value});
  const button=document.createElement("button");button.type="submit";button.className="secondary compact";button.textContent="Применить";button.disabled=!power;
  const result=document.createElement("small");result.className="tx-power-result";result.textContent=power?"Диапазон этой платы: 2–22 dBm":"Получаю текущую настройку мощности с платы…";
  row.append(input,output);form.append(row,button,result);
  form.addEventListener("submit",event=>{event.preventDefault();void applyTxPower(Number(input.value),button,result)});
  card.append(form);
}

async function applyTxPower(requestedDbm:number,button:HTMLButtonElement,result:HTMLElement){
  const current=radioConfigs.get("lora");
  if(!device||defaultRadioConfigs.has("lora")||!current){result.textContent="Конфигурация LoRa ещё не получена от платы.";return}
  const value=Math.round(requestedDbm);
  if(value<2||value>TX_POWER_HARDWARE_CAP_DBM){result.textContent=`Допустимый диапазон: 2–${TX_POWER_HARDWARE_CAP_DBM} dBm.`;return}
  const previous=Number(current.txPower);
  if(previous===value){result.textContent=`На плате уже задано ${value} dBm.`;return}
  if(!confirm(`Изменить мощность передатчика с ${previous} на ${value} dBm?`))return;
  button.disabled=true;result.textContent="Записываю настройку на плату…";
  try{
    // This board exposes a local-only setter so a Wi-Fi client does not have
    // to wait for a Meshtastic admin ACK while the radio is being reconfigured.
    // The Orange Pi proxy forwards the same route.
    try{
      const response=await fetch("/radio/tx-power",{method:"POST",headers:{"Content-Type":"text/plain"},body:String(value),cache:"no-store"});
      if(response.ok){
        const reply=await response.json();
        const applied=Number(reply.applied_dbm);
        if(!reply.ok||applied!==value)throw new Error(`плата применила ${applied} dBm вместо ${value} dBm`);
        radioConfigs.set("lora",create(Protobuf.Config.Config_LoRaConfigSchema,{...current,txPower:applied}));
        result.textContent=`Сохранено и проверено: ${applied} dBm. Это заданная мощность, не измерение ваттметром.`;
        renderTxPowerMetric();window.setTimeout(()=>void loadStatus(),1200);
        return;
      }
      if(response.status!==404)throw new Error((await response.text())||`HTTP ${response.status}`);
    }catch(error){
      if(!(error instanceof TypeError))throw error;
      // Older firmware or a direct transport can still use the admin fallback.
    }
    const next=create(Protobuf.Config.Config_LoRaConfigSchema,{...current,txPower:value});
    const config=create(Protobuf.Config.ConfigSchema,{payloadVariant:{case:"lora",value:next}});
    const setMessage=create(Protobuf.Admin.AdminMessageSchema,{payloadVariant:{case:"setConfig",value:config}});
    // A local config write can reconfigure the radio before the firmware sends
    // an admin ACK. Waiting for that ACK leaves the quick control stuck even
    // though no LoRa acknowledgement is needed. Queue the local command
    // without ACK, then explicitly read the section back for verification.
    await device.meshClient.sendPacket(toBinary(Protobuf.Admin.AdminMessageSchema,setMessage),Protobuf.Portnums.PortNum.ADMIN_APP,"self",0,false,false);
    await new Promise(resolve=>window.setTimeout(resolve,700));
    const query=create(Protobuf.Admin.AdminMessageSchema,{payloadVariant:{case:"getConfigRequest",value:Protobuf.Admin.AdminMessage_ConfigType.LORA_CONFIG}});
    await device.meshClient.sendPacket(toBinary(Protobuf.Admin.AdminMessageSchema,query),Protobuf.Portnums.PortNum.ADMIN_APP,"self",0,false,true);
    let verified=false;
    for(let attempt=0;attempt<16;attempt++){
      await new Promise(resolve=>window.setTimeout(resolve,500));
      if(Number(radioConfigs.get("lora")?.txPower)===value){verified=true;break}
    }
    if(!verified)throw new Error("плата не подтвердила новое значение за 8 секунд");
    result.textContent=`Сохранено и проверено: ${value} dBm. Это заданная мощность, не измерение ваттметром.`;
    renderTxPowerMetric();window.setTimeout(()=>void loadStatus(),1200);
  }catch(error){result.textContent=`Ошибка записи: ${errorText(error)}`}
  finally{button.disabled=false}
}

async function loadStatus(){
  const updated=$("status-updated");
  try{
    const [r,counters]=await Promise.all([fetchJson("/json/report"),fetchJson("/notifications/status").catch(()=>({}))]),d=r.data||r,channel=Number(d.airtime?.channel_utilization)||0,tx=Number(d.airtime?.utilization_tx)||0;
    const counts=nodeActivityCounts(),power=txPowerInfo(),clock=clockMetric(),role=defaultRadioConfigs.has("device")?(observedNodeRole()??Number.NaN):Number(radioConfigs.get("device")?.role),roleText=role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT?"CLIENT":role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE?"CLIENT_MUTE":Number.isFinite(role)?`роль ${role}`:"—",fixedPosition=defaultRadioConfigs.has("position")?observedFixedPosition():radioConfigs.get("position")?.fixedPosition,metrics:Array<[string,string,string?,string?,string?,string?]>=[
      ["Wi‑Fi",`${d.wifi?.rssi??"—"} dBm`],["LoRa",`${d.radio?.frequency?.toFixed?.(3)??"—"} MHz`],["Эфир занят",`${channel.toFixed(1)}%`],["Передача",`${tx.toFixed(2)}%`],
      ["Работает",`${Math.floor((d.airtime?.seconds_since_boot||0)/3600)} ч`],["Архив свободно",`${Math.round((d.memory?.fs_free||0)/1024)} КБ`],
      ["Роль ноды",roleText,undefined,role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE?"clock-metric-warn":undefined,role===Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE?"Мобильная роль без ретрансляции чужих пакетов":"Стационарная роль CLIENT"],
      ["Источник позиции",fixedPosition===undefined?"—":fixedPosition?"фиксированная":"телефон / внешний",undefined,undefined,"Меняется вручную во вкладке Настройки"],
      ["Принято пакетов",counters.rx_packets===undefined?"—":String(counters.rx_packets),undefined,undefined,"Успешно принятые LoRa-пакеты с момента загрузки платы"],
      ["Отправлено пакетов",counters.tx_packets===undefined?"—":String(counters.tx_packets),undefined,undefined,"Фактически начатые LoRa-передачи с момента загрузки, включая ретрансляции"],
      ["Активные ноды",String(counts.active),"node-active-count","node-metric-active","Слышали менее 15 минут назад"],
      ["Недавние ноды",String(counts.recent),"node-recent-count","node-metric-recent","Слышали от 15 минут до 2 часов назад"],
      ["Мощность TX",power?`${power.appliedDbm} dBm`:"—","tx-power-value",undefined,txPowerTitle(),txPowerDetail()],
      ["Часы ESP",clock.value,"esp-clock-value",clockSyncError?"clock-metric-error":clockSyncCompleted?"clock-metric-ok":clockSyncInFlight?"clock-metric-warn":undefined,clock.detail,clock.detail]
    ];
    const grid=$("status-grid");grid.replaceChildren(...metrics.map(([a,b,id,cls,title,detail])=>{const e=document.createElement("div"),label=document.createElement("span"),value=document.createElement("b");e.className=`metric${cls?` ${cls}`:""}`;if(title)e.title=title;label.textContent=a;value.textContent=b;if(id)value.id=id;e.append(label,value);if(detail){const note=document.createElement("small");note.textContent=detail;if(id==="tx-power-value")note.id="tx-power-detail";e.append(note)}if(id==="tx-power-value")addTxPowerControl(e);return e}));
    const last=airtime.at(-1);if(!last||Date.now()/1000-last.ts>20){airtime.push({ts:Math.floor(Date.now()/1000),channel,tx});airtime=airtime.slice(-120);localStorage.setItem("meshtastic-airtime",JSON.stringify(airtime))}drawAirtime();if(hasBackend())await loadLinkQuality();updated.textContent=`Обновлено ${new Date().toLocaleTimeString("ru-RU",{hour:"2-digit",minute:"2-digit",second:"2-digit"})}`;
  }catch(e){$("status-grid").textContent=`Нет данных: ${errorText(e)}`;updated.textContent="Не удалось обновить"}
}

async function refreshStatus(){const button=$<HTMLButtonElement>("status-refresh");button.disabled=true;button.textContent="Обновляю…";try{await Promise.all([loadStatus(),loadListenBeforeTalk()])}finally{button.disabled=false;button.textContent="Обновить"}}

let pingBotEnabled=false;
async function loadPingBotStatus(){
  const state=$("pingbot-state"),button=$<HTMLButtonElement>("pingbot-toggle");
  try{
    const status=await fetchJson("/pingbot/status");pingBotEnabled=Boolean(status.enabled);
    state.textContent=pingBotEnabled?"Включён":"Выключен";state.className="pill "+(pingBotEnabled?"ok":"warn");
    button.textContent=pingBotEnabled?"Выключить":"Включить";button.className=pingBotEnabled?"danger":"";button.disabled=false;
  }catch(e){state.textContent="Недоступно";state.className="pill bad";button.disabled=true;$("pingbot-result").textContent="Требуется новая прошивка: "+errorText(e)}
}
async function togglePingBot(){
  const button=$<HTMLButtonElement>("pingbot-toggle"),result=$("pingbot-result"),next=!pingBotEnabled;
  button.disabled=true;result.textContent=next?"Включаю…":"Выключаю…";
  try{
    const response=await fetch("/pingbot/toggle",{method:"POST",headers:{"Content-Type":"text/plain"},body:String(next)}),message=await response.text();
    if(!response.ok)throw new Error(message||String(response.status));
    result.textContent=message;await loadPingBotStatus();
  }catch(e){result.textContent="Ошибка: "+errorText(e);button.disabled=false}
}

async function loadDualBootStatus(){
  const box=$("dualboot-status"),rnodeButton=$<HTMLButtonElement>("boot-rnode"),meshcoreButton=$<HTMLButtonElement>("boot-meshcore"),portable=$<HTMLElement>("portable-controls"),home=$<HTMLButtonElement>("boot-home"),settingsHome=$<HTMLElement>("settings-portable-home");
  try{
    const status=await fetchJson("/dualboot/status");
    if(status.portable_ap){
      box.textContent=`Портативный Meshtastic AP включён: ${status.portable_ssid} · ${status.portable_ip}`;
      portable.hidden=true;home.hidden=false;settingsHome.hidden=false;
    }else{
      box.textContent="Meshtastic подключён к домашней Wi‑Fi сети.";
      portable.hidden=false;home.hidden=true;settingsHome.hidden=true;
    }
    if(status.rnode_installed){
      box.textContent+=` RNode готов: ${status.version||"образ найден"}, раздел ${status.partition||"app1"}.`;
      box.className="result";rnodeButton.disabled=Boolean(status.portable_ap);
    }else{
      box.textContent+=" Образ RNode во втором разделе не найден; загрузите его по Wi‑Fi ниже.";
      box.className="result muted";rnodeButton.disabled=true;
    }
    if(status.meshcore_installed&&status.meshcore_remote_return){
      box.textContent+=` MeshCore готов в ${status.meshcore_partition||"app2"}; веб-возврат в Meshtastic проверен образом.`;
      meshcoreButton.disabled=Boolean(status.portable_ap);
    }else if(status.meshcore_installed){
      box.textContent+=` MeshCore найден в ${status.meshcore_partition||"app2"}, но образ не подтверждает удалённый возврат.`;
      meshcoreButton.disabled=true;
    }else{
      box.textContent+=" Образ MeshCore в app2 не найден; загрузите специальную BLE-сборку ниже.";
      box.className="result muted";meshcoreButton.disabled=true;
    }
  }catch(e){
    settingsHome.hidden=true;
    box.textContent=`Эта версия Meshtastic ещё не поддерживает triple‑boot: ${errorText(e)}`;
    box.className="result muted";rnodeButton.disabled=true;meshcoreButton.disabled=true;
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
  const buttons=[$<HTMLButtonElement>("boot-home"),$<HTMLButtonElement>("settings-boot-home")],results=[$("dualboot-result"),$("settings-home-result")];buttons.forEach(button=>button.disabled=true);results.forEach(result=>result.textContent="Возвращаю домашний Wi‑Fi…");
  try{
    const response=await fetch("/dualboot/home",{method:"POST"}),text=await response.text();
    if(!response.ok)throw new Error(text||`${response.status}`);
    results.forEach(result=>result.textContent=text);
  }catch(e){results.forEach(result=>result.textContent=`Ошибка: ${errorText(e)}`);buttons.forEach(button=>button.disabled=false)}
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

async function bootMeshCore(){
  if(!confirm("Остановить Meshtastic и загрузить MeshCore? MeshCore работает как отдельная LoRa-сеть. Для возврата откройте этот же IP-адрес и нажмите «Вернуться в Meshtastic»."))return;
  const button=$<HTMLButtonElement>("boot-meshcore"),result=$("dualboot-result");
  button.disabled=true;result.textContent="Сохраняю Wi-Fi и переключаю раздел…";
  try{
    const response=await fetch("/dualboot/meshcore",{method:"POST"}),text=await response.text();
    if(!response.ok)throw new Error(text||`${response.status}`);
    result.textContent=text||"Плата перезагружается. Через 10–20 секунд этот адрес откроет страницу MeshCore с кнопкой возврата.";
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

async function uploadMeshCoreFirmware(){
  const input=$<HTMLInputElement>("meshcore-firmware"),button=$<HTMLButtonElement>("upload-meshcore"),result=$("meshcore-firmware-result"),file=input.files?.[0];
  if(!file)return void(result.textContent="Выберите .bin файл MeshCore Companion BLE для BarbieNode.");
  if(!confirm("Записать проверяемый образ MeshCore в app2? Meshtastic и RNode останутся активными в своих разделах."))return;
  button.disabled=true;result.textContent="Считаю SHA‑256…";
  try{
    const sha=sha256Hex(await file.arrayBuffer());
    result.textContent="Загружаю и проверяю образ…";
    const response=await fetch("/dualboot/update/meshcore",{method:"POST",headers:{"Content-Type":"application/octet-stream","X-Firmware-SHA256":sha,"X-Firmware-Target":"meshcore-app2"},body:file});
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
    device.events.onDeviceStatus.subscribe(updateConnectionStatus);
    device.events.onMyNodeInfo.subscribe(info=>{markConnectionConfigured();myNode=info.myNodeNum>>>0;removeOwnAimSamples();if(!nodes.get(myNode)?.user)addNode(myNode,{user:fallbackOwner});renderOwnIdentity();renderMessages();renderNodes();renderAir();scheduleMap()});
    device.events.onNodeInfoPacket.subscribe(info=>{const n=info as AnyRecord;const num=(n.num??n.nodeNum)>>>0;if(num){addNode(num,{...n,nodeInfoAt:Math.floor(Date.now()/1000)});if(selectedNode===num)updateDestination();if(isOwnNode(num)&&n.user){ownerConfig=n.user;device!.meshClient.config.editor.setBaselineOwner(n.user);renderOwnIdentity();refreshSettingsOptions();renderMobilityControls()}if(!initialNodeSync){scheduleNodeRender();scheduleNodeCacheSave()}}});
    device.events.onChannelPacket.subscribe(info=>{const c=info as AnyRecord;channels.set(Number(c.index),c);renderChannels();renderChannelManager();refreshSettingsOptions()});
    device.events.onConfigPacket.subscribe(info=>{const c=info as AnyRecord,key=c.payloadVariant?.case,value=c.payloadVariant?.value;if(key&&value){radioConfigs.set(key,value);defaultRadioConfigs.delete(key);refreshSettingsOptions();renderMobilityControls();if(key==="lora"){markConnectionConfigured();renderTxPowerMetric();renderModemPreset();window.setTimeout(()=>void syncBoardClock(),2000)}}});
    device.events.onModuleConfigPacket.subscribe(info=>{const c=info as AnyRecord,key=c.payloadVariant?.case,value=c.payloadVariant?.value;if(key&&value){moduleConfigs.set(key,value);defaultModuleConfigs.delete(key);refreshSettingsOptions()}});
    device.events.onMeshPacket.subscribe(packet=>recordPacket(packet as AnyRecord));
    device.events.onMessagePacket.subscribe(packet=>{const p=packet as AnyRecord,transportKey=airPacketIdentity({from:Number(p.from)>>>0,id:Number(p.id)});if(Boolean(p.viaMqtt)||(transportKey&&mqttPacketIds.has(transportKey))){recordMeshtasticMqttMessage(p);return}const m:Message={ts:Math.floor(new Date(p.rxTime).getTime()/1000)||Math.floor(Date.now()/1000),event:p.from===myNode?"tx":"rx",from:hex(p.from),to:p.type==="broadcast"?"^all":hex(p.to),channel:Number(p.channel)||0,text:String(p.data),id:p.id,source:"эфир"},firstSeen=!archiveSeenKeys.has(messageIdentity(m));rememberArchiveMessage(m);recordAimTrialReply(m);messages.push(m);messages=messages.slice(-MAX_BROWSER_MESSAGES);if(m.event==="rx"&&firstSeen)markUnread(m);addNode(p.from,{lastHeard:Math.floor(Date.now()/1000)});renderMessageChannelTabs();renderMessages();renderNodes();renderAimPingReplies();if(messagesViewActive()&&(selectedMessageChannel==="all"||selectedMessageChannel==="direct"&&directPeer(m)>0||selectedMessageChannel===m.channel))void acknowledgeViewedMessages()});
    device.events.onPositionPacket.subscribe(packet=>{const p=packet as AnyRecord,from=Number(p.from)>>>0;addNode(from,{position:p.data,lastHeard:Math.floor(Date.now()/1000)});setNodeAction("position",from,`Пакет позиции получен ${new Date().toLocaleTimeString("ru-RU")}:\n${positionText(p.data)}\n\nЭто отдельный пакет; он не является результатом трассировки.`);if(isOwnNode(from))renderMobilityControls();renderNodes();scheduleMap()});
    device.events.onNeighborInfoPacket.subscribe(packet=>{const p=packet as AnyRecord,source=Number(p.data?.nodeId||p.from)>>>0;neighborInfos.set(source,p.data);localStorage.setItem("meshtastic-neighbors",JSON.stringify([...neighborInfos]));scheduleMap()});
    device.events.onTraceRoutePacket.subscribe(packet=>{const p=packet as AnyRecord,from=Number(p.from)>>>0,route=(p.data?.route||[]).map((n:number,i:number)=>`${i+1}. ${nodeName(n)} (${short(hex(n))})`).join("\n"),routeBack=(p.data?.routeBack||[]).map((n:number,i:number)=>`${i+1}. ${nodeName(n)} (${short(hex(n))})`).join("\n");setNodeAction("trace",from,`Трассировка получена ${new Date().toLocaleTimeString("ru-RU")}:\n\nПуть к ноде:\n${route||"промежуточные ноды не указаны"}\n\nОбратный путь:\n${routeBack||"не указан"}\n\nПолные данные:\n${json(p.data)}`)});
    await device.configure();initialNodeSync=false;scheduleLivePacketCapture();
    myNode=device.meshClient.myNodeNum>>>0;
    if(myNode){removeOwnAimSamples();if(!nodes.get(myNode)?.user)addNode(myNode,{user:fallbackOwner});const owner=nodes.get(myNode)?.user;if(owner){ownerConfig=owner;device.meshClient.config.editor.setBaselineOwner(owner)}renderOwnIdentity();renderMessages();renderNodes();renderAir();scheduleMap();refreshSettingsOptions();renderMobilityControls();void saveNodeCache()}
    hydrateSettingsFromEditor();
    markConnectionConfigured();
    renderMobilityControls();
    const requiredConfigs=[["device",Protobuf.Admin.AdminMessage_ConfigType.DEVICE_CONFIG],["position",Protobuf.Admin.AdminMessage_ConfigType.POSITION_CONFIG],["lora",Protobuf.Admin.AdminMessage_ConfigType.LORA_CONFIG]] as const;
    const requestMissingConfigs=()=>requiredConfigs.forEach(([key,type],index)=>window.setTimeout(()=>{if(device&&defaultRadioConfigs.has(key))void device.meshClient.config.getRadio(type)},index*700));
    requestMissingConfigs();window.setTimeout(requestMissingConfigs,5000);
    window.setTimeout(()=>void syncBoardClock(),10000);
  }catch(e){initialNodeSync=false;connectionConfigured=false;clearConnectionWarning();statusPill("нет API платы","bad");$("send-result").textContent=`Подключение: ${errorText(e)}`}
}

async function sendMessage(event:SubmitEvent){
  event.preventDefault();const field=$<HTMLTextAreaElement>("message"),text=field.value.trim();if(!text)return;
  if(sendInFlight)return;
  const result=$("send-result"),direct=selectedNode!==undefined;
  if(direct&&!hasUsablePublicKey(selectedNode!)){updateDestination();result.className="send-feedback bad";result.textContent="Отправка остановлена до эфира: публичный ключ получателя неизвестен. Выберите «Вернуться в общий» или дождитесь свежего NodeInfo.";return}
  sendInFlight=true;updateDestination();result.className="send-feedback muted";result.textContent="Отправка…";
  try{
    const target=selectedNode,channel=direct?0:Number(($<HTMLSelectElement>("channel")).value),id=await sendLoRa("text",text,direct?hex(target!):"^all",channel,direct?publicKeyBase64(target!):"");
    const m:Message={ts:Math.floor(Date.now()/1000),event:"tx",from:myNode?hex(myNode):"self",to:direct?hex(selectedNode!):"^all",channel,text,id,source:"этот браузер"};
    messages.push(m);if(direct)deliveryStates.set(id,{ts:m.ts,packetId:id,destination:m.to,status:"accepted",evidence:"Пакет принят локальным отправителем и поставлен в очередь с запросом ACK."});const sent=messages.filter(x=>x.event==="tx").slice(-200);localStorage.setItem("meshtastic-esp-sent",JSON.stringify(sent));renderMessages();field.value="";$("chars").textContent="0/200";
    let saved=true;try{await saveSentArchive()}catch{saved=false}
    result.className="send-feedback ok";result.textContent=(direct?`Пакет #${id} передан плате для личной доставки; ждём подтверждение ноды.`:`Пакет #${id} принят вашей платой. Для общего канала доставка получателям не подтверждается.`)+(saved?" Сохранено на ESP.":" Сохранено только в этом браузере.");
  }catch(e){result.className="send-feedback bad";result.textContent=sendFailureText(e,selectedNode)}finally{sendInFlight=false;updateDestination()}
}

document.querySelectorAll<HTMLButtonElement>(".tab").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll(".tab,.view").forEach(x=>x.classList.remove("active"));b.classList.add("active");$(b.dataset.tab!).classList.add("active");if(b.dataset.tab==="messages"){if(b.dataset.messageStream==="direct")selectedMessageChannel="direct";else if(b.dataset.messageStream==="all"){selectedMessageChannel="all";selectedNode=undefined;directContext="";$<HTMLButtonElement>("broadcast").hidden=true}if(selectedMessageChannel==="all")unreadChannels.clear();else if(selectedMessageChannel==="direct"){if(selectedDirectPeer==="all")unreadDirectPeers.clear();else unreadDirectPeers.delete(selectedDirectPeer)}else unreadChannels.delete(selectedMessageChannel);renderMessageChannelTabs();renderMessages();updateDestination();updateUnreadIndicators();void acknowledgeViewedMessages()}if(b.dataset.tab==="mqtt"&&hasBackend()){mqttLastSeenTs=Math.max(mqttLastSeenTs,...mqttMessages.map(message=>message.ts),0);localStorage.setItem("barbienode-mqtt-last-seen",String(mqttLastSeenTs));renderMqtt();void loadMqtt()}if(b.dataset.tab==="status")requestAnimationFrame(()=>{drawAirtime();if(hasBackend()){drawLinkQuality();renderModemPreset()}});if(b.dataset.tab==="map")scheduleMap();if(b.dataset.tab==="aim")requestAnimationFrame(()=>{renderAim();renderAimTracking();renderAimDirectNodes();renderAimNodeDirections();if(hasBackend())void loadPingSchedule()});if(b.dataset.tab==="settings"){renderChannelManager();refreshSettingsOptions();void Promise.all([loadDualBootStatus(),loadPingBotStatus()])}}));
$("refresh").addEventListener("click",()=>void Promise.all([loadArchive(),loadStatus(),loadNotificationStatus()]));
$("status-refresh").addEventListener("click",()=>void refreshStatus());
$("lbt-toggle").addEventListener("change",()=>void toggleListenBeforeTalk());
$("mark-read").addEventListener("click",()=>void acknowledgeViewedMessages());
$("node-search").addEventListener("input",renderNodes);
for(const [id,fallback,render] of [["node-sort","last-heard",renderNodes],["aim-target-sort","last-heard",renderAimTargets],["aim-direct-sort","last-heard",renderAimDirectNodes],["aim-directions-sort","hops",renderAimNodeDirections]] as const){const select=$<HTMLSelectElement>(id),key=`meshtastic-${id}`;select.value=localStorage.getItem(key)||fallback;select.addEventListener("change",()=>{localStorage.setItem(key,select.value);render()})}
$("map-active").addEventListener("change",renderMap);
$("map-links").addEventListener("change",renderMap);
$("map-zoom-in").addEventListener("click",()=>{mapRadiusIndex=Math.max(0,mapRadiusIndex-1);renderMap()});
$("map-zoom-out").addEventListener("click",()=>{mapRadiusIndex=Math.min(mapRadii.length-1,mapRadiusIndex+1);renderMap()});
$("aim-target").addEventListener("change",event=>selectAimTarget(Number((event.target as HTMLSelectElement).value)));
$("aim-heading").addEventListener("input",event=>{aimHeading=Number((event.target as HTMLInputElement).value);localStorage.setItem("meshtastic-aim-heading",String(aimHeading));renderAim();renderAimTracking();renderAimNodeDirections()});
$("aim-compass").addEventListener("click",()=>void enableAimCompass());
$("aim-refresh").addEventListener("click",()=>void refreshAimReadings());
$("aim-reset").addEventListener("click",()=>{aimStartedAt=Math.floor(Date.now()/1000);renderAim()});
$("aim-direct-refresh").addEventListener("click",()=>void refreshAimDirectNodes());
$("aim-monitor-reset").addEventListener("click",()=>{if(!confirm("Удалить сохранённые замеры наведения для всех направлений?"))return;aimSamples=[];persistAimSamples();void saveAimMeasurements();renderAimTracking()});
$("aim-trial-start").addEventListener("click",startAimTrial);
$("aim-trial-stop").addEventListener("click",stopAimTrial);
$("aim-trial-ping").addEventListener("click",()=>void sendAimTrialPing());
$("aim-own-location-save").addEventListener("click",()=>void saveOwnLocation());
$("aim-node-direction-search").addEventListener("input",renderAimNodeDirections);
$("aim-autoping-start").addEventListener("click",()=>void changePingSchedule("start"));
$("aim-autoping-cancel").addEventListener("click",()=>void changePingSchedule("cancel"));
$("mqtt-connect").addEventListener("click",()=>void configureMqtt("connect"));
$("mqtt-save").addEventListener("click",()=>void configureMqtt("save"));
$("mqtt-disconnect").addEventListener("click",()=>void configureMqtt("disconnect"));
$("mqtt-refresh").addEventListener("click",()=>void loadMqtt());
$<HTMLSelectElement>("mqtt-profile").addEventListener("change",selectMqttProfile);
$("mqtt-message").addEventListener("input",event=>$("mqtt-chars").textContent=`${(event.target as HTMLTextAreaElement).value.length}/500`);
$("mqtt-send-form").addEventListener("submit",event=>void sendMqtt(event));
$("preset-medium-fast").addEventListener("click",()=>void applyModemPreset(MODEM_PRESETS.MEDIUM_FAST,"MediumFast"));
$("preset-long-fast").addEventListener("click",()=>void applyModemPreset(MODEM_PRESETS.LONG_FAST,"LongFast"));
$("air-search").addEventListener("input",renderAir);
$("air-kind").addEventListener("change",renderAir);
$("clear-air").addEventListener("click",()=>{airEvents=[];seenAirPacketIds.clear();localStorage.removeItem("meshtastic-air-events");renderAir();renderAimDirectNodes()});
$("broadcast").addEventListener("click",setBroadcast);
$("direct-conversation-close").addEventListener("click",()=>{setBroadcast();selectedDirectPeer="all";renderMessageChannelTabs();renderMessages()});
$<HTMLSelectElement>("direct-recipient").addEventListener("change",event=>{const value=Number((event.target as HTMLSelectElement).value)>>>0;if(value)setDirect(value)});
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
$("role-client").addEventListener("click",()=>void setNodeRole(Protobuf.Config.Config_DeviceConfig_Role.CLIENT));
$("role-client-mute").addEventListener("click",()=>void setNodeRole(Protobuf.Config.Config_DeviceConfig_Role.CLIENT_MUTE));
$("phone-position-read").addEventListener("click",readPhonePosition);
$("phone-position-send").addEventListener("click",()=>void sendPhonePosition());
$("fixed-position-restore").addEventListener("click",()=>void restoreFixedPosition());
$("pingbot-toggle").addEventListener("click",()=>void togglePingBot());
$("boot-rnode").addEventListener("click",()=>void bootRNode());
$("boot-meshcore").addEventListener("click",()=>void bootMeshCore());
$("boot-portable").addEventListener("click",()=>void bootPortable());
$("boot-home").addEventListener("click",()=>void bootHome());
$("settings-boot-home").addEventListener("click",()=>void bootHome());
$("upload-rnode").addEventListener("click",()=>void uploadRNodeFirmware());
$("upload-meshcore").addEventListener("click",()=>void uploadMeshCoreFirmware());

const composer=$("send-form");
const reserveComposerSpace=()=>document.documentElement.style.setProperty("--composer-space",`${composer.getBoundingClientRect().height+28}px`);
new ResizeObserver(reserveComposerSpace).observe(composer);
reserveComposerSpace();
const mqttComposer=$("mqtt-send-form"),reserveMqttComposerSpace=()=>document.documentElement.style.setProperty("--mqtt-composer-space",`${mqttComposer.getBoundingClientRect().height+28}px`);new ResizeObserver(reserveMqttComposerSpace).observe(mqttComposer);reserveMqttComposerSpace();
addEventListener("resize",()=>{drawAirtime();drawLinkQuality();scheduleMap();renderAim();renderAimTracking()});

renderAir();drawAirtime();renderMap();renderAimTargets();renderAimTracking();renderChannelManager();renderMqtt();renderModemPreset();renderMobilityControls();
normalizeBrowserHistory(Math.floor(Date.now()/1000),0);
renderAimTrials();
async function initialize(){
  await detectRuntimeMode();
  if(hasBackend()&&sessionStorage.getItem("barbienode-pending-modem-preset"))document.querySelector<HTMLButtonElement>('button[data-tab="status"]')?.click();
  const tasks=[loadNodeCache(),loadAimMeasurements(),loadArchive(),loadStatus(),loadListenBeforeTalk(),loadDualBootStatus(),loadNotificationStatus(),loadPingBotStatus(),loadOwnLocation(),loadDeliveryStates(),connect()];
  if(hasBackend())tasks.push(loadPingSchedule(),loadMqtt());
  await Promise.all(tasks);
  setInterval(loadArchive,30000);setInterval(loadStatus,30000);setInterval(loadNotificationStatus,30000);setInterval(renderAimTrials,1000);
  if(hasBackend()){setInterval(loadPingSchedule,15000);setInterval(loadMqtt,10000);setInterval(loadDeliveryStates,5000)}
  setInterval(()=>void syncBoardClock(),5*60*1000);
  setInterval(()=>{if($("aim").classList.contains("active")){renderAim();renderAimTracking()}},1000);
}
void initialize();
