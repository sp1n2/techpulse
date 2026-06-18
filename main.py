import hashlib
import json
import logging
import re
import time
import urllib.request
from asyncio import get_running_loop
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import AsyncGenerator, Optional

import anthropic
import feedparser
from fastapi import FastAPI, Header, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("techpulse")

# ─── Config ───────────────────────────────────────────────────────────────────

HN_BASE   = "https://hacker-news.firebaseio.com/v0"
CACHE_TTL = 1800  # 30 min

RSS_SOURCES = [
    {"name": "Ars Technica",      "url": "https://feeds.arstechnica.com/arstechnica/index"},
    {"name": "The Verge",         "url": "https://www.theverge.com/rss/index.xml"},
    {"name": "TechCrunch",        "url": "https://techcrunch.com/feed/"},
    {"name": "Bleeping Computer", "url": "https://www.bleepingcomputer.com/feed/"},
]

# Browser-like UA — many feeds block python-feedparser's default UA
RSS_UA = "Mozilla/5.0 (compatible; TechPulse/2.0; +https://github.com/sp1n2/techpulse)"

CATEGORY_KW: dict[str, list[str]] = {
    "ai":       ["ai", "artificial intelligence", "machine learning", "llm", "gpt", "chatgpt",
                 "openai", "deep learning", "neural", "nlp", "anthropic", "gemini", "mistral",
                 "deepseek", "generative", "large language", "claude", "stable diffusion"],
    "security": ["security", "hack", "vulnerability", "breach", "malware", "ransomware",
                 "exploit", "cve", "phishing", "cyber", "backdoor", "zero-day", "trojan",
                 "password", "encryption", "attack", "patch", "threat", "spyware"],
    "crypto":   ["bitcoin", "ethereum", "crypto", "blockchain", "nft", "defi", "web3",
                 "btc", "eth", "solana", "coinbase", "binance", "token", "mining", "wallet"],
    "hardware": ["cpu", "gpu", "chip", "processor", "silicon", "hardware", "semiconductor",
                 "intel", "amd", "nvidia", "arm", "risc", "fpga", "asic", "transistor",
                 "memory", "ssd", "storage", "raspberry"],
    "linux":    ["linux", "ubuntu", "debian", "kernel", "gnu", "fedora", "arch",
                 "distro", "open source", "bsd", "unix", "systemd", "bash", "shell"],
}

ALERT_KW = {
    "critical", "breach", "zero-day", "zero day", "actively exploited",
    "major attack", "mass attack", "emergency patch", "data leak",
    "ransomware attack", "hacked", "exploit in the wild",
}

_SKIP_PREFIXES = ("Ask HN:", "Show HN:", "Who is hiring", "Tell HN:", "Launch HN:", "Hiring:")
_SKIP_DOMAINS  = ("reddit.com", "old.reddit.com", "i.redd.it", "v.redd.it")
_SKIP_PATHS    = ("news.ycombinator.com/item",)

_cache: dict = {
    "articles":      [],
    "alerts":        [],
    "ts":            0.0,
    "errors":        [],
    "fetch_count":   0,
    "source_counts": {},
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": RSS_UA})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _is_valid(a: dict) -> bool:
    title = a.get("title", "")
    url   = a.get("url", "").lower()
    return (
        not any(title.startswith(p) for p in _SKIP_PREFIXES)
        and not any(d in url for d in _SKIP_DOMAINS)
        and not any(p in url for p in _SKIP_PATHS)
    )


def _categorize(a: dict) -> list[str]:
    text = (a["title"] + " " + a.get("url", "")).lower()
    return [cat for cat, kws in CATEGORY_KW.items() if any(kw in text for kw in kws)]


def _is_alert(a: dict) -> bool:
    return any(kw in a["title"].lower() for kw in ALERT_KW)


# ─── HN ───────────────────────────────────────────────────────────────────────

def _hn_item(sid: int) -> Optional[dict]:
    try:
        item = _fetch_json(f"{HN_BASE}/item/{sid}.json")
        if not item or item.get("type") != "story" or not item.get("title"):
            return None
        return {
            "id":          f"hn-{item['id']}",
            "source":      "Hacker News",
            "source_type": "hn",
            "title":       item["title"],
            "url":         item.get("url") or f"https://news.ycombinator.com/item?id={item['id']}",
            "description": "",
            "score":       item.get("score", 0),
            "by":          item.get("by", ""),
            "time":        item.get("time", 0),
            "descendants": item.get("descendants", 0),
            "hn_url":      f"https://news.ycombinator.com/item?id={item['id']}",
            "read_time":   None,
        }
    except Exception:
        return None


# ─── RSS ──────────────────────────────────────────────────────────────────────

def _rss_source(source: dict) -> tuple[list[dict], Optional[str]]:
    """Fetch one RSS feed. Returns (articles, error_message_or_None)."""
    name = source["name"]
    try:
        req = urllib.request.Request(
            source["url"],
            headers={
                "User-Agent": RSS_UA,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            raw_bytes = r.read()

        feed = feedparser.parse(raw_bytes)

        if not feed.entries:
            bozo_msg = str(feed.get("bozo_exception", "")) if feed.get("bozo") else ""
            detail = f"0 entries — bozo: {bozo_msg}" if bozo_msg else "0 entries returned"
            log.warning(f"[RSS] {name}: {detail}")
            return [], detail

        out = []
        for e in feed.entries[:15]:
            raw   = e.get("summary") or next((c.get("value", "") for c in e.get("content", [])), "")
            clean = re.sub(r"<[^>]+>", " ", raw).strip()
            words = len(clean.split())
            desc  = re.sub(r"\s+", " ", clean)[:200]
            pub   = e.get("published_parsed") or e.get("updated_parsed")
            ts    = int(time.mktime(pub)) if pub else int(time.time())
            url   = e.get("link", "")
            out.append({
                "id":          f"rss-{hashlib.md5(url.encode()).hexdigest()[:10]}",
                "source":      name,
                "source_type": "rss",
                "title":       e.get("title", "Untitled"),
                "url":         url,
                "description": desc,
                "score":       None,
                "by":          e.get("author", name),
                "time":        ts,
                "descendants": None,
                "hn_url":      None,
                "read_time":   max(1, round(words / 200)),
            })

        log.info(f"[RSS] {name}: {len(out)} articles OK")
        return out, None

    except Exception as ex:
        msg = f"{type(ex).__name__}: {ex}"
        log.error(f"[RSS] {name} FAILED — {msg}")
        return [], msg


# ─── Refresh ──────────────────────────────────────────────────────────────────

def _refresh() -> None:
    """Fetch fresh data from all sources. Keeps previous articles if fetch returns nothing."""
    now = time.time()
    log.info("[Refresh] Starting fetch from all sources...")

    errors: list[str] = []
    source_counts: dict[str, int] = {}
    articles: list[dict] = []

    # Hacker News top stories
    try:
        hn_ids = _fetch_json(f"{HN_BASE}/topstories.json")[:30]
        log.info(f"[HN] Got {len(hn_ids)} story IDs")
    except Exception as ex:
        msg = f"HN topstories: {type(ex).__name__}: {ex}"
        log.error(f"[HN] FAILED — {ex}")
        errors.append(msg)
        hn_ids = []

    with ThreadPoolExecutor(max_workers=15) as pool:
        hn_futs  = {pool.submit(_hn_item, sid): sid for sid in hn_ids}
        rss_futs = {pool.submit(_rss_source, src): src["name"] for src in RSS_SOURCES}

        hn_ok = 0
        for f in as_completed(hn_futs):
            r = f.result()
            if r:
                articles.append(r)
                hn_ok += 1
        source_counts["Hacker News"] = hn_ok
        log.info(f"[HN] {hn_ok}/{len(hn_ids)} valid stories")

        for f in as_completed(rss_futs):
            src_name = rss_futs[f]
            batch, err = f.result()
            articles.extend(batch)
            source_counts[src_name] = len(batch)
            if err:
                errors.append(f"{src_name}: {err}")

    articles = [a for a in articles if _is_valid(a)]
    for a in articles:
        a["categories"] = _categorize(a)
        a["is_alert"]   = _is_alert(a)
    articles.sort(key=lambda a: a["time"], reverse=True)

    cutoff = now - 86400
    alerts = [a for a in articles if a["is_alert"] and a["time"] > cutoff]

    if articles:
        _cache["articles"] = articles
        _cache["alerts"]   = alerts
        log.info(
            f"[Refresh] Done — {len(articles)} articles, {len(alerts)} alerts. "
            f"Counts: {source_counts}"
        )
    else:
        # Keep old articles so the UI doesn't go blank on a bad refresh
        log.warning(
            f"[Refresh] 0 articles fetched — retaining {len(_cache['articles'])} cached. "
            f"Errors: {errors}"
        )

    # Always update metadata so health endpoint stays accurate
    _cache["ts"]            = now
    _cache["errors"]        = errors
    _cache["source_counts"] = source_counts
    _cache["fetch_count"]   = _cache["fetch_count"] + 1


def _load() -> dict:
    """Return cached articles, refreshing if stale."""
    if _cache["articles"] and time.time() - _cache["ts"] < CACHE_TTL:
        return _cache
    _refresh()
    return _cache


# ─── Startup ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Kick off the first fetch in a background thread so startup is non-blocking
    loop = get_running_loop()
    loop.run_in_executor(None, _refresh)
    yield


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="TechPulse", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ─── Models ───────────────────────────────────────────────────────────────────

class ChatMsg(BaseModel):
    role:    str
    content: str

class ChatRequest(BaseModel):
    messages:   list[ChatMsg]
    system:     Optional[str] = None
    max_tokens: Optional[int] = 1500


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return FileResponse("static/index.html")


@app.get("/manifest.json")
def manifest():
    return FileResponse("static/manifest.json", media_type="application/manifest+json")


@app.get("/sw.js")
def service_worker():
    return FileResponse(
        "static/sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )


@app.get("/api/health")
def health():
    age = round(time.time() - _cache["ts"]) if _cache["ts"] else None
    return {
        "status":          "ok",
        "articles_cached": len(_cache["articles"]),
        "alerts_cached":   len(_cache["alerts"]),
        "last_fetch_ts":   _cache["ts"],
        "last_fetch_age_s": age,
        "cache_ttl_s":     CACHE_TTL,
        "cache_fresh":     age is not None and age < CACHE_TTL,
        "fetch_count":     _cache["fetch_count"],
        "source_counts":   _cache["source_counts"],
        "last_errors":     _cache["errors"],
    }


@app.get("/api/news")
def news(category: str = Query("all")):
    data = _load()
    arts = data["articles"]
    if category != "all":
        arts = [a for a in arts if category in a.get("categories", [])]

    resp: dict = {"articles": arts, "total": len(arts), "fetched_at": data["ts"]}
    if not data["articles"]:
        # No articles at all — surface debug info so we can diagnose
        resp["debug"] = {
            "message":      "No articles cached. Check /api/health for details.",
            "source_counts": data["source_counts"],
            "errors":       data["errors"],
            "fetch_count":  data["fetch_count"],
            "cache_age_s":  round(time.time() - data["ts"]) if data["ts"] else None,
        }
    return resp


@app.get("/api/alerts")
def alerts():
    data = _load()
    return {"alerts": data["alerts"], "count": len(data["alerts"])}


@app.post("/api/chat")
async def chat_proxy(
    body: ChatRequest,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    if not x_api_key or x_api_key in ("", "undefined", "null"):
        return JSONResponse({"error": "No API key"}, status_code=401)

    client = anthropic.AsyncAnthropic(api_key=x_api_key)

    async def _stream() -> AsyncGenerator[str, None]:
        try:
            kwargs: dict = dict(
                model="claude-opus-4-8",
                max_tokens=body.max_tokens or 1500,
                messages=[{"role": m.role, "content": m.content} for m in body.messages],
            )
            if body.system:
                kwargs["system"] = body.system
            async with client.messages.stream(**kwargs) as s:
                async for text in s.text_stream:
                    yield f"data: {json.dumps({'text': text})}\n\n"
            yield "data: [DONE]\n\n"
        except anthropic.AuthenticationError:
            yield f"data: {json.dumps({'error': 'Invalid API key — check Settings ⚙.'})}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")
