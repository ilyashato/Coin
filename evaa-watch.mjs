// EVAA (BSC) birleşik izleyici: havuz alım-satımları + büyük transferler + izleme listesi
// Gereksinim: Node 18+ (ek paket yok)
// Çalıştırma: MIN_USD=5000 node evaa-watch.mjs   (5 dk'da bir zamanlanabilir)
// Çıktı: evaa-feed.json (tek liste, zamana göre sıralı)
import fs from "node:fs";

const TOKEN = "0xaa036928c9c0df07d525b55ea8ee690bb5a628c1";
const POOL = "0x26deb24a2623cf54452ab5183e2c34551831d54d"; // EVAA/USDT havuzu
const RPC = process.env.BSC_RPC || "https://bsc-dataseed.binance.org";
const MIN_USD = Number(process.env.MIN_USD || 5000);        // genel eşik
const WATCH_MIN_USD = Number(process.env.WATCH_MIN_USD || 1000); // izleme listesi eşiği
const OUT = process.env.OUT || "evaa-feed.json";
const STATE = process.env.STATE || "evaa-state.json";
const STATUS = process.env.STATUS || OUT.replace(/[^/]*$/, "evaa-status.json");
const CHUNK = 1000;
const MAX_KEEP = 1000;
const TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// İzleme listesi (küçük harf). Yenilerini buraya ekle.
const WATCH = {
  "0xb8b370e5fe46c1e010023c8b2d7f4bc743f03426": "Proje dağıtıcı / ilk sahip (resmi token-meta)",
};

const readJson = (f, d) => { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return d; } };
const topicToAddr = (t) => ("0x" + t.slice(26)).toLowerCase();
const tagsFor = (...addrs) => addrs.filter((a) => WATCH[a]).map((a) => "izleme: " + WATCH[a]);

let rid = 0;
const RPCS = (process.env.BSC_RPC || "https://bsc-rpc.publicnode.com,https://bsc-dataseed1.binance.org,https://1rpc.io/bnb,https://bsc.drpc.org")
  .split(",").map((x) => x.trim()).filter(Boolean);
async function rpc(method, params) {
  let last;
  for (const url of RPCS) {
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: ++rid, method, params }),
      });
      const j = await r.json();
      if (j.error) throw new Error(`${method}: ${j.error.message}`);
      return j.result;
    } catch (e) { last = e; }
  }
  throw last;
}

async function getPrice() {
  const tries = [
    async () => {
      const j = await (await fetch(`https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/${TOKEN}`)).json();
      return j?.data?.attributes?.token_prices?.[TOKEN];
    },
    async () => (await (await fetch("https://fapi.binance.com/fapi/v1/ticker/price?symbol=EVAAUSDT")).json())?.price,
    async () => (await (await fetch("https://api.bybit.com/v5/market/tickers?category=linear&symbol=EVAAUSDT")).json())?.result?.list?.[0]?.lastPrice,
    async () => (await (await fetch("https://api.coingecko.com/api/v3/simple/price?ids=evaa-protocol&vs_currencies=usd")).json())?.["evaa-protocol"]?.usd,
  ];
  for (const t of tries) {
    try { const p = Number(await t()); if (p > 0) return p; } catch {}
  }
  throw new Error("Fiyat alınamadı (tüm kaynaklar)");
}

async function blockTime(hexBlock, cache) {
  if (cache[hexBlock]) return cache[hexBlock];
  const b = await rpc("eth_getBlockByNumber", [hexBlock, false]);
  const t = new Date(Number(BigInt(b.timestamp)) * 1000).toISOString();
  cache[hexBlock] = t;
  return t;
}

// 1) Havuz alım-satımları (GeckoTerminal)
async function poolTrades() {
  const url = `https://api.geckoterminal.com/api/v2/networks/bsc/pools/${POOL}/trades?trade_volume_in_usd_greater_than=${MIN_USD}`;
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`GeckoTerminal: ${r.status}`);
  const j = await r.json();
  return (j.data || []).map((t) => {
    const a = t.attributes;
    const toIsEvaa = String(a.to_token_address || "").toLowerCase() === TOKEN;
    const wallet = String(a.tx_from_address || "").toLowerCase();
    return {
      key: `${a.tx_hash}:swap`,
      type: "swap",
      chain: "bsc",
      time: a.block_timestamp,
      hash: a.tx_hash,
      wallet,
      side: toIsEvaa ? "buy" : "sell",
      evaa: Number(toIsEvaa ? a.to_token_amount : a.from_token_amount),
      usd: Math.round(Number(a.volume_in_usd)),
      tags: tagsFor(wallet),
    };
  });
}

// 2) Cüzdandan cüzdana transferler (RPC Transfer olayları)
// Dayanıklı tarama: aralık RPC limitini aşarsa küçültür, çok geride kalındıysa atlar,
// hata olsa da o ana kadarki ilerlemeyi kaydeder (takılı kalmaz).
const MAX_BACK = Number(process.env.MAX_BACK || 30000);  // en fazla bu kadar blok geriye bak
const MAX_CALLS = Number(process.env.MAX_CALLS || 40);   // bir çalışmada en fazla getLogs çağrısı

async function transfers(state, price) {
  const dec = Number(BigInt(await rpc("eth_call", [{ to: TOKEN, data: "0x313ce567" }, "latest"])));
  const latest = Number(BigInt(await rpc("eth_blockNumber", [])));
  let from = state.lastBlock ? state.lastBlock + 1 : latest - 2000;
  if (latest - from > MAX_BACK) {
    console.log(`Çok geride (${latest - from} blok), ${latest - MAX_BACK} bloğundan devam ediliyor`);
    from = latest - MAX_BACK;
  }
  const out = [];
  const cache = {};
  let chunk = Number(process.env.CHUNK || 5000);
  let s = from, calls = 0, done = from - 1, error = null;

  while (s <= latest && calls < MAX_CALLS) {
    const e = Math.min(s + chunk - 1, latest);
    let logs;
    calls++;
    try {
      logs = await rpc("eth_getLogs", [{
        address: TOKEN, topics: [TRANSFER],
        fromBlock: "0x" + s.toString(16), toBlock: "0x" + e.toString(16),
      }]);
    } catch (err) {
      if (/archive|too old|pruned|missing trie|historical/i.test(err.message)) {
        // bu RPC eski blokları vermiyor: başlangıcı yakın tarafa taşı (aralığın yarısı kadar)
        const ns = Math.max(s + 1, latest - Math.max(300, Math.floor((latest - s) / 2)));
        console.log(`Eski blok reddedildi, başlangıç ${s} -> ${ns}`);
        if (ns > latest - 300 && s >= latest - 300) { error = err.message; break; }
        s = ns; continue;
      }
      if (chunk > 250) { chunk = Math.max(250, Math.floor(chunk / 2)); continue; }  // aralığı küçült
      error = err.message;
      break;
    }
    for (const l of logs) {
      if (l.topics.length !== 3) continue;
      const f = topicToAddr(l.topics[1]);
      const t = topicToAddr(l.topics[2]);
      if (f === POOL || t === POOL) continue; // havuz hareketleri swap akışında
      const amount = Number(BigInt(l.data)) / 10 ** dec;
      const usd = amount * price;
      const tags = tagsFor(f, t);
      const min = tags.length ? WATCH_MIN_USD : MIN_USD;
      if (usd < min) continue;
      out.push({
        key: `${l.transactionHash}:${Number(BigInt(l.logIndex))}`,
        type: "transfer",
        chain: "bsc",
        time: await blockTime(l.blockNumber, cache),
        hash: l.transactionHash,
        from: f, to: t,
        evaa: amount,
        usd: Math.round(usd),
        tags,
      });
    }
    done = e;
    s = e + 1;
  }
  return { events: out, from, latest, done, calls, error };
}

async function main() {
  const feed = readJson(OUT, []);
  const state = readJson(STATE, {});
  const seen = new Set(feed.map((e) => e.key));
  let added = 0;
  const push = (list) => {
    for (const e of list) if (!seen.has(e.key)) { feed.push(e); seen.add(e.key); added++; }
  };

  const status = { time: new Date().toISOString(), errors: [] };
  const fail = (where, e) => { console.error(where + ":", e.message); status.errors.push(where + ": " + e.message); };

  let price = null;
  try { price = await getPrice(); } catch (e) { fail("Fiyat", e); }
  status.price = price;

  try { push(await poolTrades()); } catch (e) { fail("Havuz", e); }

  if (!price) console.log("Fiyat yok, transfer taraması atlandı");
  if (price) {
    try {
      const r = await transfers(state, price);
      push(r.events);
      if (r.done >= r.from) state.lastBlock = r.done;
      status.scan = { from: r.from, done: r.done, latest: r.latest, calls: r.calls, error: r.error || null };
      if (r.error) status.errors.push("Transfer: " + r.error);
      console.log(`Transfer taraması: ${r.from}-${r.done} (hedef ${r.latest}, ${r.calls} çağrı)` + (r.error ? " · durdu: " + r.error : ""));
    } catch (e) { fail("Transfer", e); }
  }

  feed.sort((a, b) => new Date(a.time) - new Date(b.time));
  fs.writeFileSync(OUT, JSON.stringify(feed.slice(-MAX_KEEP), null, 2));
  fs.writeFileSync(STATE, JSON.stringify(state));
  status.added = added; status.total = feed.length; status.lastBlock = state.lastBlock || null; status.rpc = RPCS[0];
  fs.writeFileSync(STATUS, JSON.stringify(status, null, 1));
  console.log(`Yeni: ${added}, toplam: ${feed.length}${price ? `, fiyat: $${price}` : ""}`);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
