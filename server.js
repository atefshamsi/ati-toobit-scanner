'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { URL } = require('node:url');

const ROOT = __dirname;
const PUBLIC = path.join(ROOT, 'public');
const DATA_DIR = path.join(ROOT, 'data');
const PINS_FILE = path.join(DATA_DIR, 'pins.json');
const FAVORITES_FILE = path.join(DATA_DIR, 'favorites.json');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const PORT = Number(process.env.PORT || 4173);
const API_BASE = process.env.TOOBIT_API_BASE || 'https://api.toobit.com';
const MAX_CARDS = 5;
const SIGNAL_TTL_MS = 30_000;
const STALE_MS = 45_000;
const MIN_SCORE = 72;
const REQUEST_GAP_MS = 180;

fs.mkdirSync(DATA_DIR, { recursive: true });

function readPins() {
  try { return JSON.parse(fs.readFileSync(PINS_FILE, 'utf8')); } catch { return []; }
}
function writeJson(file, value) { const tmp = file + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(value, null, 2), 'utf8'); fs.renameSync(tmp, file); }
function writePins(pins) { writeJson(PINS_FILE, pins); }
function readJson(file, fallback) { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; } }
const initialSettings = readJson(SETTINGS_FILE, { scanMode: 'priority', horizonMode: 'fast', orderTypes: ['Market','Limit','Trigger'], useCaseFilterEnabled:false, useCaseGroups:[], marketFilterEnabled:false, marketGroups:[] });
function normalizeSettings(value = {}) {
  const validModes = ['priority','favorites','all'];
  const horizonMode = ['fast','normal'].includes(value.horizonMode) ? value.horizonMode : 'fast';
  const allowedTypes = ['Market','Limit','Trigger'];
  const scanMode = validModes.includes(value.scanMode) ? value.scanMode : 'priority';
  const orderTypes = Array.isArray(value.orderTypes) ? [...new Set(value.orderTypes.filter(t => allowedTypes.includes(t)))] : allowedTypes;
  const useCaseGroups = Array.isArray(value.useCaseGroups) ? [...new Set(value.useCaseGroups.filter(x => ['payment','network','infra','stable','meme'].includes(x)))] : [];
  const marketGroups = Array.isArray(value.marketGroups) ? [...new Set(value.marketGroups.filter(x => ['large','medium','volatile','btc'].includes(x)))] : [];
  return { scanMode, horizonMode, orderTypes: orderTypes.length ? orderTypes : allowedTypes, useCaseFilterEnabled: Boolean(value.useCaseFilterEnabled), useCaseGroups, marketFilterEnabled: Boolean(value.marketFilterEnabled), marketGroups };
}

const state = {
  running: false,
  activeTab: false,
  lastHeartbeat: 0,
  capital: Number(initialSettings.capital) > 0 ? Number(initialSettings.capital) : 100,
  leverage: Number(initialSettings.leverage) >= 1 ? Number(initialSettings.leverage) : 10,
  settings: normalizeSettings(initialSettings),
  favorites: readJson(FAVORITES_FILE, []),
  startedAt: null,
  lastDataAt: null,
  lastError: null,
  statusText: 'آماده برای شروع',
  totalContracts: 0,
  scannedContracts: 0,
  qualifiedCount: 0,
  round: 0,
  signals: [],
  pins: readPins(),
  currentSymbol: null,
  scanPromise: null,
  scanAbort: false,
  exchangeInfo: null,
  lastExchangeInfoAt: 0,
  lastTickers: new Map(),
  requestQueue: Promise.resolve(),
  lastRequestAt: 0,
  recentCandidates: new Map(),
  manualRechecks: new Set(),
  inFlightSymbols: new Set(),
};

const now = () => Date.now();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function apiGet(endpoint, params = {}) {
  // Pace request starts globally while allowing a small number of in-flight requests.
  let release;
  const previous = state.requestQueue;
  state.requestQueue = new Promise(resolve => { release = resolve; });
  await previous;
  const wait = Math.max(0, REQUEST_GAP_MS - (now() - state.lastRequestAt));
  if (wait) await sleep(wait);
  state.lastRequestAt = now();
  release();
  const url = new URL(endpoint, API_BASE);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 12_000);
  try {
    const response = await fetch(url, { signal: controller.signal, headers: { 'Accept': 'application/json', 'User-Agent': 'ToobitLocalScanner/0.1' } });
    if (!response.ok) throw new Error(`Toobit HTTP ${response.status} (${endpoint})`);
    const body = await response.json();
    if (body && typeof body === 'object' && body.code && Number(body.code) >= 400) throw new Error(`Toobit API ${body.code}: ${body.msg || body.message || 'خطای API'}`);
    return body;
  } finally { clearTimeout(timer); }
}

async function getExchangeInfo(force = false) {
  if (!force && state.exchangeInfo && now() - state.lastExchangeInfoAt < 10 * 60_000) return state.exchangeInfo;
  const info = await apiGet('/api/v1/exchangeInfo');
  if (!info || !Array.isArray(info.contracts)) throw new Error('پاسخ مشخصات قراردادهای Futures ساختار مورد انتظار را ندارد.');
  state.exchangeInfo = info;
  state.lastExchangeInfoAt = now();
  return info;
}

function parseKline(row) {
  if (Array.isArray(row)) return { time: Number(row[0]), open: Number(row[1]), high: Number(row[2]), low: Number(row[3]), close: Number(row[4]), volume: Number(row[5]), closeTime: Number(row[6]) };
  return { time: Number(row.t ?? row.time), open: Number(row.o ?? row.open), high: Number(row.h ?? row.high), low: Number(row.l ?? row.low), close: Number(row.c ?? row.close), volume: Number(row.v ?? row.volume), closeTime: Number(row.T ?? row.closeTime ?? row.st) };
}
function ema(values, period) {
  if (values.length < period) return [];
  const k = 2 / (period + 1);
  let prev = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  const out = Array(period - 1).fill(null);
  out.push(prev);
  for (let i = period; i < values.length; i++) { prev = values[i] * k + prev * (1 - k); out.push(prev); }
  return out;
}
function rollingMid(candles, period, endIndex) {
  const start = endIndex - period + 1;
  if (start < 0) return null;
  let hi = -Infinity, lo = Infinity;
  for (let i = start; i <= endIndex; i++) { hi = Math.max(hi, candles[i].high); lo = Math.min(lo, candles[i].low); }
  return (hi + lo) / 2;
}
function atr(candles, period = 14) {
  if (candles.length < period + 1) return null;
  const trs = [];
  for (let i = 1; i < candles.length; i++) trs.push(Math.max(candles[i].high - candles[i].low, Math.abs(candles[i].high - candles[i - 1].close), Math.abs(candles[i].low - candles[i - 1].close)));
  const last = trs.slice(-period);
  return last.reduce((a, b) => a + b, 0) / last.length;
}
function analyzeTimeframe(candles) {
  if (!Array.isArray(candles) || candles.length < 60) return { ready: false, trend: 'نامشخص', score: 0, reason: 'کندل کافی نیست' };
  candles = candles.map(parseKline).filter(c => [c.open,c.high,c.low,c.close].every(Number.isFinite)).sort((a,b) => a.time-b.time);
  // Exclude a still-forming final candle when the API provides its close timestamp.
  const lastRaw = candles.at(-1);
  if (lastRaw && Number.isFinite(lastRaw.closeTime)) {
    const closeTimeMs = lastRaw.closeTime < 1e12 ? lastRaw.closeTime * 1000 : lastRaw.closeTime;
    if (closeTimeMs >= now()) candles = candles.slice(0, -1);
  }
  if (candles.length < 60) return { ready: false, trend: 'نامشخص', score: 0, reason: 'کندل معتبر کافی نیست' };
  const closes = candles.map(c => c.close);
  const e9 = ema(closes, 9), e21 = ema(closes, 21), e26 = ema(closes, 26), e12 = ema(closes, 12), e26macd = ema(closes, 26);
  const macd = closes.map((_, i) => e12[i] != null && e26macd[i] != null ? e12[i] - e26macd[i] : null);
  const macdValues = macd.filter(Number.isFinite);
  const signalLineCompact = ema(macdValues, 9);
  const macdLine = macd.at(-1), signalLine = signalLineCompact.at(-1);
  const last = candles.at(-1), prev = candles.at(-2);
  const cloudIndex = candles.length - 1 - 26;
  const tenkan = rollingMid(candles, 9, candles.length - 1);
  const kijun = rollingMid(candles, 26, candles.length - 1);
  // The cloud visible at the current bar was calculated 26 bars earlier.
  const cloudTenkan = cloudIndex >= 0 ? rollingMid(candles, 9, cloudIndex) : null;
  const cloudKijun = cloudIndex >= 0 ? rollingMid(candles, 26, cloudIndex) : null;
  const spanB = cloudIndex >= 0 ? rollingMid(candles, 52, cloudIndex) : null;
  const spanA = cloudTenkan != null && cloudKijun != null ? (cloudTenkan + cloudKijun) / 2 : null;
  const cloudTop = spanA != null && spanB != null ? Math.max(spanA, spanB) : null;
  const cloudBottom = spanA != null && spanB != null ? Math.min(spanA, spanB) : null;
  const trendUp = e9.at(-1) > e21.at(-1) && e21.at(-1) > e26.at(-1);
  const trendDown = e9.at(-1) < e21.at(-1) && e21.at(-1) < e26.at(-1);
  const cloudUp = cloudTop != null && last.close > cloudTop;
  const cloudDown = cloudBottom != null && last.close < cloudBottom;
  const macdUp = Number.isFinite(macdLine) && Number.isFinite(signalLine) && macdLine > signalLine;
  const macdDown = Number.isFinite(macdLine) && Number.isFinite(signalLine) && macdLine < signalLine;
  const bullishCandle = last.close > last.open && last.close > prev.high;
  const bearishCandle = last.close < last.open && last.close < prev.low;
  let up = 0, down = 0;
  if (trendUp) up += 2; if (trendDown) down += 2;
  if (cloudUp) up += 2; if (cloudDown) down += 2;
  if (macdUp) up += 1; if (macdDown) down += 1;
  if (bullishCandle) up += 1; if (bearishCandle) down += 1;
  const trend = up > down ? 'LONG' : down > up ? 'SHORT' : 'NEUTRAL';
  const range = candles.slice(-21, -1);
  const resistance = Math.max(...range.map(c => c.high));
  const support = Math.min(...range.map(c => c.low));
  const volatility = atr(candles, 14);
  const aligned = Math.max(up, down);
  const recent = candles.slice(-12);
  const recentRangeHigh = Math.max(...recent.map(c => c.high));
  const recentRangeLow = Math.min(...recent.map(c => c.low));
  const rangeWidth = recentRangeHigh - recentRangeLow;
  const rangeMarket = volatility > 0 && rangeWidth <= volatility * 3.2;
  const previous = candles.slice(-25, -5);
  const previousAtr = atr(previous, 14) || volatility;
  const recentMove = previous.length ? Math.abs(last.close - previous.at(-1).close) : 0;
  const suddenPump = previousAtr > 0 && recentMove >= previousAtr * 3.5 && rangeMarket;
  const bullishRejection = last.close > last.open && (last.close - last.low) > (last.high - last.low) * 0.60;
  const bearishRejection = last.close < last.open && (last.high - last.close) > (last.high - last.low) * 0.60;
  return { ready: true, trend, up, down, last: last.close, candleTime: last.time, atr: volatility, support, resistance, e9: e9.at(-1), e21: e21.at(-1), macdUp, macdDown, cloudUp, cloudDown, bullishCandle, bearishCandle, bullishRejection, bearishRejection, rangeMarket, rangeHigh: recentRangeHigh, rangeLow: recentRangeLow, suddenPump, score: Math.min(100, Math.round(aligned / 6 * 100)), reason: trend === 'LONG' ? 'هم‌راستایی روند/اندیکاتورها به سمت صعود' : trend === 'SHORT' ? 'هم‌راستایی روند/اندیکاتورها به سمت نزول' : 'نشانه‌ها هم‌جهت نیستند' };
}
function getFilter(contract, type) { return (contract.filters || []).find(f => f.filterType === type) || {}; }
function roundDownStep(value, step) { if (!(step > 0)) return value; const precision = Math.max(0, (String(step).split('.')[1] || '').length); return Number((Math.floor(value / step + 1e-10) * step).toFixed(precision)); }
function makeCandidate(contract, price, frames, capital, leverage = 10) {
  const [h1, m15, m5] = frames;
  if (frames.some(f => !f.ready) || !(price > 0)) return null;
  const sides = ['LONG', 'SHORT'];
  let best = null;
  for (const side of sides) {
    const dir = side === 'LONG' ? 1 : -1;
    const tfScore = f => f.trend === side ? 1 : f.trend === 'NEUTRAL' ? 0.4 : 0;
    const tfPoints = 40 * tfScore(h1) + 30 * tfScore(m15) + 30 * tfScore(m5);
    const macdPoints = 5 * [h1,m15,m5].filter(f => side === 'LONG' ? f.macdUp : f.macdDown).length;
    const cloudPoints = 4 * [h1,m15,m5].filter(f => side === 'LONG' ? f.cloudUp : f.cloudDown).length;
    const paPoints = 4 * [h1,m15,m5].filter(f => side === 'LONG' ? f.bullishCandle : f.bearishCandle).length;
    const indicatorScore = Math.min(100, Math.round(tfPoints * 0.78 + macdPoints + cloudPoints + paPoints));
    const atrVal = m5.atr || m15.atr || price * 0.01;
    const rangeHigh = Math.max(m5.resistance, m15.resistance);
    const rangeLow = Math.min(m5.support, m15.support);
    const nearResistance = price >= m5.resistance - atrVal * 0.25;
    const nearSupport = price <= m5.support + atrVal * 0.25;
    const breakoutLong = price > m5.resistance + atrVal * 0.15 && m5.bullishCandle && m5.macdUp;
    const breakoutShort = price < m5.support - atrVal * 0.15 && m5.bearishCandle && m5.macdDown;
    const rangeMid = (rangeHigh + rangeLow) / 2;
    const inRangeMiddle = (m5.rangeMarket || m15.rangeMarket) && Math.abs(price - rangeMid) < Math.max(atrVal * 0.7, (rangeHigh-rangeLow) * 0.18);
    const pumpFlag = Boolean(m5.suddenPump || m15.suddenPump);
    const structure = side === 'LONG' ? Math.min(m5.support, m15.support) : Math.max(m5.resistance, m15.resistance);
    const atResistance = side === 'LONG' && nearResistance;
    const atSupport = side === 'SHORT' && nearSupport;
    const atLongSupport = side === 'LONG' && nearSupport;
    const atShortResistance = side === 'SHORT' && nearResistance;
    const nearMean = Math.abs(price - m5.e21) / price < 0.003;
    const confirmedLongReversal = atLongSupport && m5.bullishRejection && m5.macdUp;
    const confirmedShortReversal = atShortResistance && m5.bearishRejection && m5.macdDown;
    const confirmedBreakout = (side === 'LONG' && breakoutLong) || (side === 'SHORT' && breakoutShort);
    const confirmedReversal = (side === 'LONG' && confirmedLongReversal) || (side === 'SHORT' && confirmedShortReversal);
    // Avoid chasing an extended move: distance from EMA21 is measured in ATR units.
    const extensionAtr = atrVal > 0 ? Math.abs(price - m5.e21) / atrVal : 0;
    const lateEntry = extensionAtr >= 1.8 && !confirmedBreakout && !confirmedReversal;
    // Hard entry-quality gates: indicator alignment alone is not an entry signal.
    if (inRangeMiddle && !breakoutLong && !breakoutShort) continue;
    if (lateEntry) continue;
    if (atResistance && !breakoutLong) continue;
    if (atSupport && !breakoutShort) continue;
    if (side === 'LONG' && atLongSupport && !confirmedLongReversal && !breakoutLong) continue;
    if (side === 'SHORT' && atShortResistance && !confirmedShortReversal && !breakoutShort) continue;
    if (side === 'LONG' && nearResistance && !breakoutLong) continue;
    if (side === 'SHORT' && nearSupport && !breakoutShort) continue;
    if (pumpFlag && side === 'SHORT' && !confirmedShortReversal && !breakoutShort) continue;
    if (pumpFlag && side === 'LONG' && !breakoutLong && !confirmedLongReversal) continue;
    const entryQuality = confirmedBreakout || confirmedReversal;
    if (!entryQuality) continue;
    const score = Math.max(0, Math.min(100, Math.round(indicatorScore * 0.72 + 18 + (nearMean ? 4 : 0))));
    let orderType = 'Market';
    let entry = price;
    if (nearResistance || nearSupport) {
      orderType = 'Trigger';
      entry = side === 'LONG' ? Math.max(price, m5.resistance + atrVal * 0.05) : Math.min(price, m5.support - atrVal * 0.05);
    } else if (nearMean && ((side === 'LONG' && m5.e21 < price) || (side === 'SHORT' && m5.e21 > price))) {
      orderType = 'Limit'; entry = m5.e21;
    }
    let sl = side === 'LONG' ? Math.min(entry - atrVal * 1.25, structure - atrVal * 0.12) : Math.max(entry + atrVal * 1.25, structure + atrVal * 0.12);
    const distance = Math.abs(entry - sl);
    if (!(distance > 0) || !Number.isFinite(distance) || distance / price > 0.08) continue;
    const disagreement = [h1,m15,m5].some(f => f.trend !== side);
    const countertrend = h1.trend !== side;
    if (countertrend && !((side === 'LONG' && confirmedLongReversal) || (side === 'SHORT' && confirmedShortReversal))) continue;
    if (score < MIN_SCORE) continue;
    const qtyRisk = capital > 0 ? capital * 0.005 / distance : null;
    const maxMarginQty = capital > 0 && leverage > 0 ? capital * leverage / entry : null;
    const lot = getFilter(contract, 'LOT_SIZE');
    const minQty = Number(lot.minQty || 0);
    const maxQty = Number(lot.maxQty || Infinity);
    const step = Number(lot.stepSize || 0);
    const quantity = qtyRisk == null ? null : roundDownStep(Math.min(maxQty, qtyRisk, maxMarginQty || Infinity), step);
    if (!(quantity > 0)) continue;
    const target1Distance = capital * 0.04 / quantity;
    const target2Distance = capital * 0.16 / quantity;
    const tp1 = entry + dir * target1Distance;
    const tp2 = entry + dir * target2Distance;
    const rr = Math.abs(tp2 - entry) / distance;
    if (score < MIN_SCORE || rr < 4.0) continue;
    const minNotional = Number(getFilter(contract, 'MIN_NOTIONAL').minNotional || 0);
    const notional = quantity == null ? null : quantity * price;
    const riskAmount = quantity == null ? null : quantity * distance;
    const qtyOk = quantity != null && quantity >= minQty && (!minNotional || notional >= minNotional);
    if (!qtyOk) continue;
    const reasons = [];
    reasons.push(`روند ۱ساعته: ${h1.trend}`);
    reasons.push(`ساختار ۱۵دقیقه: ${m15.trend}`);
    reasons.push(`زمان‌بندی ۵دقیقه: ${m5.trend}`);
    if (disagreement) reasons.push('تایم‌فریم‌ها کاملاً هم‌جهت نیستند');
    if (countertrend) reasons.push('برگشت کوتاه‌مدت خلاف روند ۱ساعته؛ ریسک بالاتر');
    if (pumpFlag) reasons.push('ورود پرریسک پس از جهش');
    if (breakoutLong || breakoutShort) reasons.push('شکست محدوده با تأیید قیمت و MACD');
    if (confirmedLongReversal || confirmedShortReversal) reasons.push('برگشت از ناحیه کلیدی با تأیید کندل و MACD');
    const candidate = {
      id: `${contract.symbol}:${side}`, symbol: contract.symbol, displayName: contract.symbolName || contract.symbol,
      side, score, entry, sl, tp1, tp2, rr, orderType, capital, leverage, tp1TargetProfit: capital * 0.04, tp2TargetProfit: capital * 0.16, riskTarget: capital * 0.005,
      quantity, riskEstimate: riskAmount, notional, minQty, stepSize: step,
      timestamp: now(), dataTimestamp: now(), candleTimes: { h1: h1.candleTime, m15: m15.candleTime, m5: m5.candleTime },
      status: pumpFlag ? 'معتبر با هشدار' : countertrend || disagreement ? 'معتبر با هشدار' : 'معتبر', warning: pumpFlag ? 'ورود پرریسک پس از جهش' : countertrend ? 'برگشت کوتاه‌مدت خلاف روند ۱ساعته؛ ریسک بالاتر.' : disagreement ? 'تایم‌فریم‌ها اختلاف دارند؛ قبل از تصمیم دوباره بررسی کن.' : '',
      reasons, timeframe: { h1: h1.trend, m15: m15.trend, m5: m5.trend },
      atr: atrVal, priceAtSignal: price, pinned: false, history: []
    };
    if (!best || candidate.score > best.score) best = candidate;
  }
  return best;
}

function freshSignal(candidate) {
  return candidate && candidate.dataTimestamp && now() - candidate.dataTimestamp <= STALE_MS;
}
function upsertPin(candidate) {
  const idx = state.pins.findIndex(p => p.id === candidate.id);
  if (idx < 0) return;
  const old = state.pins[idx];
  const at = now();
  const levelKeys = ['entry','sl','tp1','tp2'];
  const levelsChanged = levelKeys.some(k => Math.abs(Number(old[k]) - Number(candidate[k])) > Math.max(1e-10, Math.abs(Number(old[k])) * 0.0001));
  const scoreDelta = Number(candidate.score || 0) - Number(old.score || 0);
  const wasInvalid = /نامعتبر|ضعیف|نیازمند|تأیید مجدد/.test(old.status || '');
  const timeframeDisagrees = Boolean(candidate.warning) || [candidate.timeframe?.h1, candidate.timeframe?.m15, candidate.timeframe?.m5].filter(t => t && t !== candidate.side).length > 0;
  let status = 'معتبر';
  let warning = '';
  if (wasInvalid) {
    status = 'شرایط دوباره مناسب';
    warning = 'شرایط دوباره از فیلترهای اصلی عبور کرده است؛ سطوح جدید را قبل از تصمیم بررسی کن.';
  } else if (timeframeDisagrees || scoreDelta <= -8) {
    status = 'در حال تضعیف';
    warning = candidate.warning || (scoreDelta <= -8 ? 'امتیاز سیگنال نسبت به بررسی قبل کاهش محسوسی داشته است.' : 'تایم‌فریم‌ها کاملاً همسو نیستند؛ ورود را با احتیاط بررسی کن.');
  } else if (candidate.warning) {
    status = 'معتبر با هشدار';
    warning = candidate.warning;
  }
  const history = Array.isArray(old.history) ? old.history.slice(-19) : [];
  const statusChanged = status !== old.status;
  const scoreChanged = Math.abs(scoreDelta) >= 3;
  if (levelsChanged || statusChanged || scoreChanged) {
    history.push({
      at,
      type: levelsChanged ? 'سطوح تغییر کرد' : statusChanged ? 'وضعیت تغییر کرد' : 'امتیاز تغییر کرد',
      oldScore: Number(old.score || 0), newScore: Number(candidate.score || 0),
      oldLevels: { entry: old.entry, sl: old.sl, tp1: old.tp1, tp2: old.tp2 },
      newLevels: { entry: candidate.entry, sl: candidate.sl, tp1: candidate.tp1, tp2: candidate.tp2 },
      note: levelsChanged ? 'سطوح جدید جایگزین سطوح قبلی شدند.' : statusChanged ? `${old.status || 'نامشخص'} ← ${status}` : 'امتیاز تحلیل تغییر کرده است.'
    });
  }
  state.pins[idx] = {
    ...old, ...candidate,
    timestamp: old.timestamp || candidate.timestamp,
    lastUpdatedAt: at, lastCheckedAt: at, previousScore: Number(old.score || 0), scoreDelta,
    currentPrice: candidate.currentPrice || candidate.priceAtSignal,
    pinned: true, history, status, warning,
  };
  writePins(state.pins);
}

function expireSignals() {
  state.signals = state.signals.filter(s => s.pinned || now() < s.expiresAt);
}
function addOrUpdateSignal(candidate) {
  if (!freshSignal(candidate)) return;
  const pinnedIdx = state.pins.findIndex(p => p.id === candidate.id);
  if (pinnedIdx >= 0) { upsertPin(candidate); return; }
  const existingIdx = state.signals.findIndex(s => s.id === candidate.id);
  if (existingIdx >= 0) {
    const old = state.signals[existingIdx];
    state.signals[existingIdx] = { ...candidate, timestamp: old.timestamp || old.firstSeen || candidate.timestamp, lastUpdatedAt: now(), firstSeen: old.firstSeen, expiresAt: now() + SIGNAL_TTL_MS, pinned: false };
    return;
  }
  expireSignals();
  const pinnedCount = state.pins.length;
  const previous = state.recentCandidates.get(candidate.id);
  if (previous && now() - previous.at < 10 * 60_000) {
    const materialMove = Number.isFinite(candidate.atr) && Math.abs(candidate.entry - previous.entry) >= candidate.atr * 0.8;
    const meaningfulImprovement = candidate.score >= previous.score + 8;
    if (!materialMove && !meaningfulImprovement) return;
  }
  const current = [...state.pins, ...state.signals];
  if (current.length >= MAX_CARDS) {
    const weakest = state.signals.reduce((w, s) => !w || s.score < w.score ? s : w, null);
    if (!weakest || weakest.score >= candidate.score) return;
    state.signals = state.signals.filter(s => s.id !== weakest.id);
  }
  state.signals.push({ ...candidate, pinned: false, firstSeen: now(), expiresAt: now() + SIGNAL_TTL_MS });
  state.recentCandidates.set(candidate.id, { at: now(), score: candidate.score, entry: candidate.entry, atr: candidate.atr });
  if (pinnedCount >= MAX_CARDS) state.signals = [];
}

async function scanContract(contract, tickerMap, options = {}) {
  const { force = false, manual = false } = options;
  if (!force && (!state.running || state.scanAbort)) return false;
  // Avoid two concurrent analyses of the same contract. A manual request waits briefly for an active scan.
  if (state.inFlightSymbols.has(contract.symbol)) {
    if (!manual) return false;
    const waitStarted = now();
    while (state.inFlightSymbols.has(contract.symbol) && now() - waitStarted < 15_000) await sleep(100);
    if (state.inFlightSymbols.has(contract.symbol)) throw new Error('بررسی قبلی این قرارداد هنوز تمام نشده است؛ چند ثانیه دیگر دوباره امتحان کن.');
  }
  state.inFlightSymbols.add(contract.symbol);
  if (!manual) state.currentSymbol = contract.symbol;
  try {
    const [k1, k15, k5] = await Promise.all([
      apiGet('/quote/v1/klines', { symbol: contract.symbol, interval: '1h', limit: 120 }),
      apiGet('/quote/v1/klines', { symbol: contract.symbol, interval: '15m', limit: 120 }),
      apiGet('/quote/v1/klines', { symbol: contract.symbol, interval: '5m', limit: 120 }),
    ]);
    const frames = [analyzeTimeframe(k1), analyzeTimeframe(k15), analyzeTimeframe(k5)];
    const tickerFresh = await apiGet('/quote/v1/contract/ticker/price', { symbol: contract.symbol });
    const tickerRow = Array.isArray(tickerFresh) ? tickerFresh.find(t => (t.s || t.symbol) === contract.symbol) || tickerFresh[0] : (tickerFresh.data || tickerFresh);
    const price = Number(tickerRow?.p || tickerRow?.price || tickerMap.get(contract.symbol)?.c || NaN);
    if (!(price > 0) || now() - state.lastRequestAt > STALE_MS) throw new Error(`قیمت تازه برای ${contract.symbol} دریافت نشد؛ از قیمت قدیمی استفاده نشد.`);
    const candidate = makeCandidate(contract, price, frames, state.capital, state.leverage);
    // Revalidate a pinned card on every completed market check, even if it no longer qualifies.
    const pinned = state.pins.find(p => p.symbol === contract.symbol);
    if (pinned) {
      const idx = state.pins.findIndex(p => p.id === pinned.id);
      if (idx >= 0 && candidate && candidate.id === pinned.id) {
        candidate.currentPrice = price;
        upsertPin(candidate);
      } else if (idx >= 0) {
        const old = state.pins[idx];
        const at = now();
        const sameSideCount = frames.filter(f => f.ready && f.trend === old.side).length;
        const framesReady = frames.every(f => f.ready);
        const stillAligned = framesReady && sameSideCount >= 2;
        const directionalScore = framesReady ? Math.round(
          (frames[0].trend === old.side ? 40 : frames[0].trend === 'NEUTRAL' ? 16 : 0) +
          (frames[1].trend === old.side ? 30 : frames[1].trend === 'NEUTRAL' ? 12 : 0) +
          (frames[2].trend === old.side ? 30 : frames[2].trend === 'NEUTRAL' ? 12 : 0)
        ) : 0;
        const status = stillAligned && directionalScore >= MIN_SCORE ? 'در حال تضعیف' : 'نامعتبر';
        const warning = status === 'نامعتبر'
          ? 'شرایط دیگر از فیلترهای اصلی عبور نمی‌کند. کارت پین‌شده حفظ شده؛ فعلاً ورود جدید پیشنهاد نمی‌شود.'
          : 'نشانه‌ها هنوز تا حدی همسو هستند، اما سیگنال معیارهای کامل ورود را ندارد. منتظر بررسی بعدی بمان.';
        const history = Array.isArray(old.history) ? old.history.slice(-19) : [];
        if (old.status !== status) history.push({ at, type: 'وضعیت تغییر کرد', oldScore: Number(old.score || 0), newScore: directionalScore, oldLevels: { entry: old.entry, sl: old.sl, tp1: old.tp1, tp2: old.tp2 }, newLevels: { entry: old.entry, sl: old.sl, tp1: old.tp1, tp2: old.tp2 }, note: `${old.status || 'نامشخص'} ← ${status}` });
        const updated = { ...old, currentPrice: price, lastCheckedAt: at, previousScore: Number(old.score || 0), scoreDelta: directionalScore - Number(old.score || 0), observedScore: directionalScore, status, warning, history };
        state.pins[idx] = updated;
        writePins(state.pins);
      }
    } else if (candidate && state.settings.orderTypes.includes(candidate.orderType)) {
      addOrUpdateSignal({ ...candidate, currentPrice: price, lastCheckedAt: now() });
    }
    if (!manual) state.scannedContracts++;
    state.lastDataAt = now();
    state.lastError = null;
    return true;
  } catch (error) {
    state.lastError = error.message || 'خطای نامشخص در دریافت داده بازار';
    return false;
  } finally {
    state.inFlightSymbols.delete(contract.symbol);
  }
}

// Category tags are intentionally transparent, maintainable symbol lists; many crypto assets overlap in real-world use.
const USE_CASE_SYMBOLS = {
  payment: new Set('BTC LTC BCH DASH XMR ZEC DGB RVN XRP XLM'.split(' ')),
  network: new Set('ETH SOL ADA BNB AVAX DOT ATOM TRX TON NEAR APT SUI SEI INJ ALGO HBAR ICP FTM KAS ETC EOS XTZ ARB OP MATIC POL'.split(' ')),
  infra: new Set('LINK UNI AAVE MKR CRV COMP SNX DYDX RUNE FIL AR RNDR GRT THETA LDO PENDLE JUP PYTH ENS'.split(' ')),
  stable: new Set('USDT USDC DAI FDUSD TUSD USDE PYUSD PAXG XAUT ONDO'.split(' ')),
  meme: new Set('DOGE SHIB PEPE FLOKI BONK WIF BRETT BOME MEME TURBO APE AXS SAND MANA GALA IMX'.split(' ')),
};
const LARGE_CAP_SYMBOLS = new Set('BTC ETH BNB SOL XRP USDT USDC DOGE ADA TRX TON AVAX SHIB DOT BCH LINK'.split(' '));
const MEDIUM_CAP_SYMBOLS = new Set('LTC UNI NEAR APT ICP ETC XLM FIL INJ HBAR ARB OP SUI ATOM RENDER AAVE ALGO VET'.split(' '));
function baseAsset(contract) { return String(contract.baseAsset || contract.baseCoin || contract.symbolName || contract.symbol || '').toUpperCase().split(/[-_/]/)[0].replace(/USDT$|USDC$/,''); }
function tickerChange(ticker) { const v = Number(ticker?.P ?? ticker?.priceChangePercent ?? ticker?.changePercent ?? NaN); return Number.isFinite(v) ? v : NaN; }
function applyCategoryFilters(contracts, tickerMap, settings) {
  let result = contracts;
  if (settings.useCaseFilterEnabled && settings.useCaseGroups.length) {
    result = result.filter(c => {
      const base = baseAsset(c);
      return settings.useCaseGroups.some(group => USE_CASE_SYMBOLS[group]?.has(base));
    });
  }
  if (settings.marketFilterEnabled && settings.marketGroups.length) {
    const btcTicker = [...tickerMap.entries()].find(([symbol]) => /^BTC(?:-SWAP)?-USDT$/i.test(symbol) || /^BTCUSDT$/i.test(symbol));
    const btcChange = btcTicker ? tickerChange(btcTicker[1]) : NaN;
    result = result.filter(c => {
      const base = baseAsset(c), change = tickerChange(tickerMap.get(c.symbol));
      return settings.marketGroups.some(group => {
        if (group === 'large') return LARGE_CAP_SYMBOLS.has(base);
        if (group === 'medium') return MEDIUM_CAP_SYMBOLS.has(base);
        if (group === 'volatile') return Number.isFinite(change) && Math.abs(change) >= 8;
        if (group === 'btc') return Number.isFinite(change) && Number.isFinite(btcChange) && Math.sign(change) === Math.sign(btcChange) && Math.abs(change) >= 1;
        return false;
      });
    });
  }
  return result;
}

async function scanLoop() {
  if (state.scanPromise) return state.scanPromise;
  state.scanPromise = (async () => {
    while (state.running && state.activeTab && !state.scanAbort) {
      state.statusText = 'دریافت فهرست قراردادهای Futures…';
      try {
        const info = await getExchangeInfo();
        let contracts = info.contracts.filter(c => c.status === 'TRADING' && c.symbol && (c.symbol.endsWith('-USDT') || c.marginToken === 'USDT'));
        const marketContracts = contracts;
        if (state.settings.scanMode === 'favorites') { const keep = new Set([...state.favorites, ...state.pins.map(p => p.symbol)]); contracts = contracts.filter(c => keep.has(c.symbol)); }
        else if (state.settings.scanMode === 'priority') contracts = [...contracts.filter(c => state.favorites.includes(c.symbol)), ...contracts.filter(c => !state.favorites.includes(c.symbol))];
        if (!contracts.length) { state.totalContracts = 0; state.statusText = state.settings.scanMode === 'favorites' ? 'ابتدا از روی کارت‌ها چند ارز را به Favorites اضافه کن یا فیلترها را تغییر بده.' : 'با فیلترهای فعلی قراردادی پیدا نشد؛ فیلترها را تغییر بده یا خاموش کن.'; await sleep(2000); continue; }
        const tickersRaw = await apiGet('/quote/v1/contract/ticker/24hr');
        const tickerRows = Array.isArray(tickersRaw) ? tickersRaw : (tickersRaw.data || []);
        const tickerMap = new Map(tickerRows.map(t => [t.s || t.symbol, t]));
        state.lastTickers = tickerMap;
        contracts = applyCategoryFilters(contracts, tickerMap, state.settings);
        if (state.settings.scanMode === 'favorites') { const keep = new Set([...state.favorites, ...state.pins.map(p => p.symbol)]); contracts = contracts.filter(c => keep.has(c.symbol)); }
        else if (state.settings.scanMode === 'priority') contracts = [...contracts.filter(c => state.favorites.includes(c.symbol)), ...contracts.filter(c => !state.favorites.includes(c.symbol))];
        state.totalContracts = contracts.length;
        state.scannedContracts = 0;
        state.round++;
        state.qualifiedCount = 0;
        state.statusText = state.settings.scanMode === 'favorites' ? `اسکن دور ${state.round}: فقط Favorites (${contracts.length} ارز)` : state.settings.scanMode === 'priority' ? `اسکن دور ${state.round}: ابتدا Favorites، سپس بازار (${contracts.length} قرارداد)` : `اسکن دور ${state.round}: تمام بازار (${contracts.length} قرارداد)`;
        expireSignals();
        const favoriteSet = new Set(state.favorites);
        const groups = state.settings.scanMode === 'priority'
          ? [contracts.filter(c => favoriteSet.has(c.symbol)), contracts.filter(c => !favoriteSet.has(c.symbol))].filter(group => group.length)
          : [contracts];
        for (let groupIndex = 0; groupIndex < groups.length; groupIndex++) {
          if (!state.running || !state.activeTab || state.scanAbort) break;
          const group = groups[groupIndex];
          if (state.settings.scanMode === 'priority') state.statusText = groupIndex === 0 ? `اولویت اسکن: Favorites (${group.length} ارز)` : `Favorites تمام شد؛ ادامه اسکن بازار (${group.length} قرارداد باقی‌مانده)`;
          let next = 0;
          const worker = async () => {
            while (state.running && state.activeTab && !state.scanAbort) {
              const idx = next++;
              if (idx >= group.length) return;
              await scanContract(group[idx], tickerMap);
              await sleep(35);
            }
          };
          await Promise.all(Array.from({ length: Math.min(3, group.length) }, worker));
        }
        state.qualifiedCount = new Set([...state.signals, ...state.pins].map(s => s.id)).size;
        if (!state.running || !state.activeTab || state.scanAbort) break;
        state.statusText = `دور ${state.round} کامل شد؛ شروع دور بعدی`;
        await sleep(1500);
      } catch (error) {
        state.lastError = error.message || 'خطای اتصال';
        state.statusText = 'اتصال یا دریافت داده ناموفق است؛ تلاش مجدد با فاصله';
        await sleep(5000);
      }
    }
    state.statusText = 'اسکن متوقف است';
  })().finally(() => { state.scanPromise = null; });
  return state.scanPromise;
}

function publicState() {
  expireSignals();
  const cards = [...state.pins.map(p => ({ ...p, pinned: true })), ...state.signals.filter(s => !state.pins.some(p => p.id === s.id))]
    .sort((a,b) => (b.pinned === a.pinned ? b.score-a.score : (b.pinned ? 1 : -1)))
    .slice(0, MAX_CARDS);
  return {
    running: state.running && state.activeTab,
    capital: state.capital,
    leverage: state.leverage,
    settings: state.settings,
    favorites: state.favorites,
    startedAt: state.startedAt,
    lastDataAt: state.lastDataAt,
    lastError: state.lastError,
    statusText: state.statusText,
    totalContracts: state.totalContracts,
    scannedContracts: state.scannedContracts,
    qualifiedCount: state.qualifiedCount,
    round: state.round,
    currentSymbol: state.currentSymbol,
    cards,
    pinCount: state.pins.length,
    stale: !state.lastDataAt || now() - state.lastDataAt > STALE_MS,
    minScore: MIN_SCORE,
  };
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': 'http://127.0.0.1:' + PORT });
  res.end(body);
}
async function readBody(req) {
  let body = '';
  for await (const chunk of req) { body += chunk; if (body.length > 1_000_000) throw new Error('درخواست بیش از حد بزرگ است.'); }
  return body ? JSON.parse(body) : {};
}
function contentType(file) {
  return ({ '.html':'text/html; charset=utf-8', '.css':'text/css; charset=utf-8', '.js':'text/javascript; charset=utf-8', '.json':'application/json; charset=utf-8', '.svg':'image/svg+xml' })[path.extname(file)] || 'application/octet-stream';
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || '127.0.0.1'}`);
  try {
    if (req.method === 'GET' && url.pathname === '/api/state') return sendJson(res, 200, publicState());
    if (req.method === 'GET' && url.pathname === '/api/health') return sendJson(res, 200, { ok: true, version: '0.8.0', node: process.version });
    if (req.method === 'GET' && url.pathname === '/api/market-review') {
      const raw = await apiGet('/quote/v1/contract/ticker/24hr');
      const rows = (Array.isArray(raw) ? raw : (raw.data || [])).filter(t => (t.s || t.symbol || '').endsWith('-USDT'));
      if (!rows.length) throw new Error('اطلاعات کافی برای گزارش سریع بازار دریافت نشد.');
      const change = t => Number(t.P ?? t.priceChangePercent ?? t.changePercent ?? 0);
      const valid = rows.filter(t => Number.isFinite(change(t)));
      const up = valid.filter(t => change(t) > 0).length, down = valid.filter(t => change(t) < 0).length;
      const topUp = [...valid].sort((a,b) => change(b)-change(a)).slice(0,3).map(t => ({symbol:t.s||t.symbol, change:change(t)}));
      const topDown = [...valid].sort((a,b) => change(a)-change(b)).slice(0,3).map(t => ({symbol:t.s||t.symbol, change:change(t)}));
      const mood = up > down * 1.15 ? 'در بیشتر ارزها قیمت رو به افزایش است؛ فرصت‌های LONG را با تأیید روند بررسی کن.' : down > up * 1.15 ? 'در بیشتر ارزها قیمت رو به کاهش است؛ فرصت‌های SHORT را با تأیید روند بررسی کن.' : 'بازار یک‌دست نیست؛ جهت کلی واضح نیست و باید ارزها جداگانه بررسی شوند.';
      const btc = valid.find(t => /BTC-SWAP-USDT|^BTCUSDT$/i.test(t.s || t.symbol || ''));
      const btcChange = btc ? change(btc) : null;
      const quoteVolume = valid.reduce((sum, t) => sum + Number(t.qv ?? t.quoteVolume ?? 0), 0);
      return sendJson(res, 200, { at: now(), count: valid.length, up, down, neutral: valid.length-up-down, mood, topUp, topDown, btcChange, quoteVolume });
    }
    if (req.method === 'POST' && url.pathname === '/api/settings') {
      const body = await readBody(req); const next = normalizeSettings(body.settings || {});
      if (!Array.isArray(body.settings?.orderTypes) || !body.settings.orderTypes.some(t => ['Market','Limit','Trigger'].includes(t))) return sendJson(res, 400, { error: 'حداقل یک نوع سفارش را انتخاب کن.' });
      state.settings = next; writeJson(SETTINGS_FILE, { ...state.settings, capital: state.capital, leverage: state.leverage }); return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && (url.pathname === '/api/favorite' || url.pathname === '/api/unfavorite')) {
      const body = await readBody(req); const symbol = String(body.symbol || '').trim();
      if (!symbol || symbol.length > 80) return sendJson(res, 400, { error: 'نماد ارز معتبر نیست.' });
      if (url.pathname === '/api/favorite') { if (!state.favorites.includes(symbol)) state.favorites.push(symbol); }
      else state.favorites = state.favorites.filter(s => s !== symbol);
      state.favorites.sort(); writeJson(FAVORITES_FILE, state.favorites); return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && url.pathname === '/api/start') {
      const body = await readBody(req);
      // Wait for a previously stopped scan loop to finish before starting another one.
      if (state.scanPromise) {
        state.running = false; state.activeTab = false; state.scanAbort = true;
        try { await state.scanPromise; } catch {}
      }
      const capital = Number(body.capital); const leverage = Number(body.leverage);
      if (!Number.isFinite(capital) || capital <= 0) return sendJson(res, 400, { error: 'سرمایه هر معامله را به عددی بزرگ‌تر از صفر وارد کن.' });
      if (!Number.isFinite(leverage) || leverage < 1 || leverage > 200) return sendJson(res, 400, { error: 'اهرم محاسباتی باید بین ۱ تا ۲۰۰ باشد.' });
      state.capital = capital; state.leverage = leverage;
      if (body.settings) state.settings = normalizeSettings(body.settings);
      writeJson(SETTINGS_FILE, { ...state.settings, capital: state.capital, leverage: state.leverage });
      state.running = true;
      state.activeTab = true;
      state.scanAbort = false;
      state.lastHeartbeat = now();
      state.startedAt = now();
      state.lastError = null;
      state.statusText = 'شروع اسکن بازار';
      scanLoop();
      return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && url.pathname === '/api/heartbeat') {
      state.lastHeartbeat = now(); state.activeTab = true;
      return sendJson(res, 200, { ok: true, state: publicState() });
    }
    if (req.method === 'POST' && url.pathname === '/api/stop') {
      state.running = false; state.activeTab = false; state.scanAbort = true; state.statusText = 'اسکن متوقف شد';
      return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && url.pathname === '/api/pin') {
      const body = await readBody(req);
      const id = String(body.id || '');
      const signal = state.signals.find(s => s.id === id);
      if (!signal && !state.pins.some(p => p.id === id)) return sendJson(res, 404, { error: 'سیگنال پیدا نشد؛ احتمالاً زمان نمایش آن تمام شده است.' });
      if (state.pins.some(p => p.id === id)) return sendJson(res, 200, publicState());
      if (state.pins.length >= MAX_CARDS) return sendJson(res, 400, { error: 'حداکثر پنج سیگنال را می‌توان هم‌زمان پین کرد.' });
      state.signals = state.signals.filter(s => s.id !== id);
      const pinned = { ...signal, pinned: true, pinnedAt: now(), status: 'نیازمند تأیید داده زنده', warning: 'پس از بازگشایی برنامه، قبل از معتبر دانستن سیگنال باید داده زنده بررسی شود.' };
      state.pins.push(pinned); writePins(state.pins);
      return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && url.pathname === '/api/unpin') {
      const body = await readBody(req); const id = String(body.id || '');
      const previous = state.pins.find(p => p.id === id);
      state.pins = state.pins.filter(p => p.id !== id);
      if (previous && state.pins.length + state.signals.length < MAX_CARDS) state.signals.push({ ...previous, pinned: false, firstSeen: now(), expiresAt: now() + SIGNAL_TTL_MS, status: 'از حالت پین خارج شده؛ شرایط را دوباره بررسی کن' });
      writePins(state.pins);
      return sendJson(res, 200, publicState());
    }
    if (req.method === 'POST' && url.pathname === '/api/recheck-pin') {
      const body = await readBody(req); const id = String(body.id || '');
      const pinned = state.pins.find(p => p.id === id);
      if (!pinned) return sendJson(res, 404, { error: 'کارت پین‌شده پیدا نشد؛ صفحه را تازه‌سازی کن.' });
      if (state.manualRechecks.has(id)) return sendJson(res, 409, { error: 'بررسی فوری این کارت در حال انجام است؛ لطفاً صبر کن.' });
      state.manualRechecks.add(id);
      try {
        const info = await getExchangeInfo();
        const contract = info.contracts.find(c => c.symbol === pinned.symbol && c.status === 'TRADING');
        if (!contract) return sendJson(res, 404, { error: 'این قرارداد در فهرست فعلی قراردادهای قابل‌معامله Toobit پیدا نشد.' });
        const previousCheck = Number(pinned.lastCheckedAt || 0);
        const ok = await scanContract(contract, state.lastTickers, { force: true, manual: true });
        const updated = state.pins.find(p => p.id === id);
        if (!ok || !updated || Number(updated.lastCheckedAt || 0) <= previousCheck) {
          return sendJson(res, 502, { error: state.lastError || 'داده تازه دریافت نشد؛ وضعیت قبلی به‌عنوان نتیجه تازه تأیید نشده است.' });
        }
        return sendJson(res, 200, publicState());
      } finally { state.manualRechecks.delete(id); }
    }
    if (req.method === 'GET' && url.pathname === '/api/contracts') {
      const info = await getExchangeInfo();
      return sendJson(res, 200, { contracts: info.contracts.filter(c => c.status === 'TRADING').map(c => ({ symbol: c.symbol, name: c.symbolName || c.symbol, marginToken: c.marginToken, filters: c.filters })) });
    }
    if (req.method === 'GET' && url.pathname.startsWith('/')) {
      let pathname = decodeURIComponent(url.pathname);
      if (pathname === '/') pathname = '/index.html';
      const file = path.normalize(path.join(PUBLIC, pathname));
      if (!file.startsWith(PUBLIC)) return sendJson(res, 403, { error: 'Forbidden' });
      fs.readFile(file, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, { 'Content-Type': contentType(file), 'Cache-Control': 'no-store' }); res.end(data);
      });
      return;
    }
    sendJson(res, 404, { error: 'Not found' });
  } catch (error) {
    sendJson(res, 500, { error: error.message || 'خطای داخلی برنامه' });
  }
});

setInterval(() => {
  if (state.activeTab && now() - state.lastHeartbeat > 30_000) {
    state.activeTab = false; state.running = false; state.scanAbort = true; state.statusText = 'برنامه در مرورگر فعال نیست؛ اسکن متوقف شد';
  }
  expireSignals();
}, 2000).unref();

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Toobit Futures Scanner is ready: http://127.0.0.1:${PORT}`);
  console.log('This app only analyzes public market data. It never places orders.');
});
