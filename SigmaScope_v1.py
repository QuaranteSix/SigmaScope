# ============================================================
# SIGMASCOPE — Version Supabase
# Données stockées en base de données (Supabase / PostgreSQL)
# Cache yfinance partagé entre tous les utilisateurs
# ============================================================
import json
import os
import uuid as _uuid
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import re
from collections import defaultdict

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import time

# ── curl_cffi : simule un vrai navigateur Chrome pour éviter le rate limit Yahoo ──
try:
    from curl_cffi import requests as curl_requests
    _YF_SESSION = curl_requests.Session(impersonate="chrome")
except Exception:
    _YF_SESSION = None

def _yf_ticker(symbol: str):
    """Crée un Ticker yfinance avec session curl_cffi si disponible."""
    if _YF_SESSION is not None:
        return yf.Ticker(symbol, session=_YF_SESSION)
    return yf.Ticker(symbol)

def _yf_call(fn, retries: int = 3, delay: float = 1.5):
    """Exécute fn() avec retry automatique sur rate limit."""
    for attempt in range(retries):
        try:
            result = fn()
            return result
        except Exception as e:
            msg = str(e).lower()
            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                if attempt < retries - 1:
                    time.sleep(delay * (attempt + 1))
                    continue
            raise
    return None
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from supabase import create_client, Client

# ── RÈGLE ABSOLUE : set_page_config EN TOUT PREMIER appel st.* ──
st.set_page_config(page_title="SigmaScope", layout="wide")

# ── Système de traduction i18n ────────────────────────────────────
try:
    from lang_fr import TRANSLATIONS as _LANG_FR
    from lang_en import TRANSLATIONS as _LANG_EN
    from lang_tr import TRANSLATIONS as _LANG_TR
except ImportError as _ie:
    _LANG_FR = {}
    _LANG_EN = {}
    _LANG_TR = {}

def t(key: str, **kwargs) -> str:
    """
    Retourne la traduction d'une clé selon la langue active.
    Supporte les variables : t("clé", n=5, name="AAPL")
    """
    lang = st.session_state.get("lang", "fr")
    if lang == "en":
        dico = _LANG_EN
    elif lang == "tr":
        dico = _LANG_TR
    else:
        dico = _LANG_FR
    text = dico.get(key, _LANG_FR.get(key, key))
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, ValueError):
            pass
    return text

# ── Connexion Supabase (après set_page_config) ──────────────────
@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

try:
    supabase = get_supabase()
except Exception as _e:
    st.error(f"❌ Connexion Supabase impossible : {_e}")
    st.stop()

# ============================================================
# IDENTIFIANT UTILISATEUR ANONYME
# ============================================================
def get_user_id() -> str:
    """
    Retourne le token anonyme de l'utilisateur.
    Généré à la première visite et stocké dans l'URL (?uid=...).
    L'utilisateur doit conserver son URL/marque-page pour retrouver ses watchlists.
    """
    uid = st.query_params.get("uid", None)
    if not uid:
        uid = str(_uuid.uuid4())
        st.query_params["uid"] = uid
    return uid

def touch_user_session(user_id: str = None):
    """
    Met à jour last_seen pour toutes les watchlists de l'utilisateur.
    Appelé à chaque visite pour réinitialiser le compteur d'expiration 30 jours.
    """
    if user_id is None:
        user_id = get_user_id()
    try:
        supabase.table("watchlists")            .update({"last_seen": datetime.now(timezone.utc).isoformat()})            .eq("user_id", user_id).execute()
    except Exception:
        pass  # best-effort, on ne bloque pas l'app

def purge_inactive_watchlists(days: int = 30):
    """
    Supprime les watchlists (+ leurs items via CASCADE) dont le last_seen
    est plus vieux que `days` jours. À appeler depuis la page Configuration.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        # Récupérer les IDs à supprimer
        res = supabase.table("watchlists")            .select("id, user_id, name, last_seen")            .lt("last_seen", cutoff).execute()
        if res.data:
            ids = [r["id"] for r in res.data]
            for wl_id in ids:
                supabase.table("watchlists").delete().eq("id", wl_id).execute()
            return len(ids)
        return 0
    except Exception:
        return 0

# ============================================================
# MULTI-WATCHLIST — FONCTIONS SUPABASE
# ============================================================
WATCHLIST_COLS = ["ticker", "company", "ajout_date", "note", "prix_achat"]

def _get_wl_id(name: str, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    res = (supabase.table("watchlists")
           .select("id").eq("user_id", user_id).eq("name", name).execute())
    return res.data[0]["id"] if res.data else None

def load_wl_index(user_id: str = None, _creating: bool = False):
    if user_id is None:
        user_id = get_user_id()
    res = (supabase.table("watchlists")
           .select("name").eq("user_id", user_id).order("created_at").execute())
    names = [r["name"] for r in res.data] if res.data else []
    if not names and not _creating:
        create_watchlist("Ma Watchlist", user_id=user_id)
        return ["Ma Watchlist"]
    return names

def save_wl_index(names, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    existing = load_wl_index(user_id=user_id, _creating=True)
    for name in names:
        if name not in existing:
            create_watchlist(name, user_id=user_id)

def create_watchlist(name: str, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    existing = load_wl_index(user_id=user_id, _creating=True)
    if name not in existing:
        supabase.table("watchlists").insert({"user_id": user_id, "name": name}).execute()
        existing.append(name)
    return existing

def delete_watchlist(name: str, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id:
        supabase.table("watchlists").delete().eq("id", wl_id).execute()
    remaining = load_wl_index(user_id=user_id)
    return [n for n in remaining if n != name]

def load_watchlist(name: str = None, user_id: str = None) -> pd.DataFrame:
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        return pd.DataFrame(columns=WATCHLIST_COLS)
    res = (supabase.table("watchlist_items")
           .select("ticker, company, ajout_date, note, prix_achat")
           .eq("watchlist_id", wl_id).execute())
    if not res.data:
        return pd.DataFrame(columns=WATCHLIST_COLS)
    df = pd.DataFrame(res.data)
    for col in WATCHLIST_COLS:
        if col not in df.columns:
            df[col] = ""
    return df[WATCHLIST_COLS].fillna("")

def save_watchlist(df: pd.DataFrame, name: str = None, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        create_watchlist(name, user_id=user_id)
        wl_id = _get_wl_id(name, user_id=user_id)
    supabase.table("watchlist_items").delete().eq("watchlist_id", wl_id).execute()
    if not df.empty:
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "watchlist_id": wl_id,
                "ticker":       str(row.get("ticker", "")).strip().upper(),
                "company":      str(row.get("company", "")),
                "ajout_date":   str(row.get("ajout_date", "")),
                "note":         str(row.get("note", "")),
                "prix_achat":   str(row.get("prix_achat", "")),
            })
        supabase.table("watchlist_items").insert(rows).execute()

def add_to_watchlist(ticker: str, company: str = "", name: str = None, user_id: str = None) -> bool:
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        create_watchlist(name, user_id=user_id)
        wl_id = _get_wl_id(name, user_id=user_id)
    res = (supabase.table("watchlist_items").select("id")
           .eq("watchlist_id", wl_id).eq("ticker", ticker).execute())
    if res.data:
        return False
    supabase.table("watchlist_items").insert({
        "watchlist_id": wl_id, "ticker": ticker, "company": company,
        "ajout_date": datetime.now().strftime("%Y-%m-%d"), "note": "", "prix_achat": "",
    }).execute()
    return True

def remove_from_watchlist(ticker: str, name: str = None, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id:
        supabase.table("watchlist_items").delete()\
            .eq("watchlist_id", wl_id).eq("ticker", ticker).execute()

def is_in_watchlist(ticker: str, name: str = None, user_id: str = None) -> bool:
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    wl_id = _get_wl_id(name, user_id=user_id)
    if not wl_id:
        return False
    res = (supabase.table("watchlist_items").select("id")
           .eq("watchlist_id", wl_id).eq("ticker", ticker).execute())
    return bool(res.data)

def get_pru(ticker: str, user_id: str = None):
    if user_id is None:
        user_id = get_user_id()
    tkr_up = ticker.strip().upper()
    for wl_name in load_wl_index(user_id=user_id):
        df = load_watchlist(wl_name, user_id=user_id)
        row = df[df["ticker"].str.upper() == tkr_up]
        if not row.empty:
            pru_val = row.iloc[0].get("prix_achat", "")
            try:
                pru = float(str(pru_val).replace(",", ".").strip())
                if pru > 0:
                    return pru
            except (ValueError, TypeError):
                pass
    return None

# ============================================================
# INDICES BOURSIERS — SUPABASE
# ============================================================
def load_all_indices():
    result = {}
    try:
        res = supabase.table("index_components").select("index_key, ticker, company").execute()
        if res.data:
            df = pd.DataFrame(res.data)
            df.columns = ["index_key", "Ticker", "Company"]
            for key in df["index_key"].unique():
                result[key] = df[df["index_key"] == key][["Ticker", "Company"]].reset_index(drop=True)
    except Exception:
        pass
    try:
        res_c = supabase.table("index_components_custom").select("index_key, ticker, company").execute()
        if res_c.data:
            df_custo = pd.DataFrame(res_c.data)
            df_custo.columns = ["index_key", "Ticker", "Company"]
            for key in df_custo["index_key"].unique():
                df_key = df_custo[df_custo["index_key"] == key][["Ticker", "Company"]].reset_index(drop=True)
                if key in result:
                    existing_tickers = set(result[key]["Ticker"].str.upper())
                    df_new = df_key[~df_key["Ticker"].str.upper().isin(existing_tickers)]
                    result[key] = pd.concat([result[key], df_new], ignore_index=True)
                else:
                    result[key] = df_key
    except Exception:
        pass
    return result

def load_indices_list():
    try:
        res = supabase.table("index_list_custom").select("ticker, company").execute()
        if not res.data:
            return None
        df = pd.DataFrame(res.data)
        df.columns = ["Ticker", "Company"]
        return df.dropna().reset_index(drop=True)
    except Exception:
        return None

def save_index_to_master_csv(index_key: str, df_new: pd.DataFrame):
    """Remplace les composants d'un indice dans Supabase."""
    supabase.table("index_components").delete().eq("index_key", index_key).execute()
    rows = [
        {"index_key": index_key, "ticker": str(row["Ticker"]).strip(),
         "company": str(row["Company"]).strip(), "source": "wikipedia",
         "updated_at": datetime.now(timezone.utc).isoformat()}
        for _, row in df_new.iterrows()
    ]
    if rows:
        for i in range(0, len(rows), 500):
            supabase.table("index_components").insert(rows[i:i+500]).execute()

# ============================================================
# CACHE YFINANCE PARTAGÉ — SUPABASE
# ============================================================
_CACHE_TTL = {"history": 60, "info": 60, "financials": 60, "live": 2, "fx": 60}

def _cache_get(cache_key: str, ttl_minutes: int):
    try:
        res = (supabase.table("market_cache").select("data_json, updated_at")
               .eq("ticker", cache_key).eq("period", "meta").execute())
        if not res.data:
            return None
        row = res.data[0]
        updated = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        if age > ttl_minutes:
            return None
        return json.loads(row["data_json"])
    except Exception:
        return None

def _cache_set(cache_key: str, data):
    try:
        supabase.table("market_cache").upsert({
            "ticker": cache_key, "period": "meta",
            "data_json": json.dumps(data, default=str),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass

def _history_cache_get(ticker: str, period: str, ttl_minutes: int = 60):
    try:
        res = (supabase.table("market_cache").select("data_json, updated_at")
               .eq("ticker", ticker).eq("period", period).execute())
        if not res.data:
            return None
        row = res.data[0]
        updated = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        if age > ttl_minutes:
            return None
        data = json.loads(row["data_json"])
        df = pd.DataFrame(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        return df
    except Exception:
        return None

def _history_cache_set(ticker: str, period: str, df: pd.DataFrame):
    try:
        supabase.table("market_cache").upsert({
            "ticker": ticker, "period": period,
            "data_json": df.reset_index().to_json(date_format="iso"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass

def get_history(ticker: str, period: str) -> pd.DataFrame:
    """Historique cours — cache Supabase 60 min + curl_cffi anti rate-limit."""
    cached = _history_cache_get(ticker, period, ttl_minutes=60)
    if cached is not None:
        return cached
    df = _yf_call(lambda: _yf_ticker(ticker).history(period=period))
    if df is not None and not df.empty:
        _history_cache_set(ticker, period, df)
        return df
    return pd.DataFrame()

_INTRADAY_MAX_PERIOD = {
    "1m": "7d", "2m": "60d", "5m": "60d",
    "15m": "60d", "30m": "60d", "60m": "730d", "1h": "730d",
}

@st.cache_data(ttl=300, show_spinner=False)
def get_history_intraday(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """Intraday — cache session 5 min + curl_cffi."""
    try:
        return _yf_ticker(ticker).history(period=period, interval=interval)
    except Exception:
        return pd.DataFrame()

def get_info(ticker: str) -> dict:
    """Infos fondamentales — cache Supabase 60 min + curl_cffi."""
    cache_key = f"info|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["info"])
    if cached is not None:
        return cached
    try:
        info = _yf_call(lambda: _yf_ticker(ticker).info) or {}
        if info:
            _cache_set(cache_key, info)
        return info
    except Exception:
        return {}

def get_financials(ticker: str):
    return _get_financials_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_financials_cached(ticker: str):
    try:
        t = _yf_ticker(ticker)
        return t.financials, t.balance_sheet, t.cashflow
    except Exception:
        import pandas as pd
        empty = pd.DataFrame()
        return empty, empty, empty

def get_recommendations(ticker: str):
    return _get_recommendations_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_recommendations_cached(ticker: str):
    try:
        t = _yf_ticker(ticker)
        rec = t.recommendations
        if rec is not None and not rec.empty:
            return rec
    except Exception:
        pass
    return None

def get_calendar(ticker: str):
    return _get_calendar_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_calendar_cached(ticker: str):
    try:
        return _yf_ticker(ticker).calendar
    except Exception:
        return None

def get_live_quote(ticker: str):
    """Quote live — cache Supabase 2 min + curl_cffi."""
    cache_key = f"live|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["live"])
    if cached is not None:
        return cached
    try:
        info = _yf_call(lambda: _yf_ticker(ticker).info) or {}
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        prev  = info.get("previousClose") or info.get("regularMarketPreviousClose")
        name  = info.get("longName") or info.get("shortName") or ticker
        curr  = info.get("currency", "")
        change_pct = (price - prev) / prev * 100 if price and prev and prev != 0 else None
        result = {"name": name, "price": price, "change_pct": change_pct, "currency": curr}
        if price:
            _cache_set(cache_key, result)
        return result
    except Exception:
        return None

def get_ticker_currency(ticker: str) -> str:
    """Devise — cache Supabase 60 min + curl_cffi."""
    cache_key = f"currency|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["fx"])
    if cached is not None:
        return cached if isinstance(cached, str) else "EUR"
    try:
        info = _yf_call(lambda: _yf_ticker(ticker).info) or {}
        curr = info.get("currency", "EUR") or "EUR"
        _cache_set(cache_key, curr)
        return curr
    except Exception:
        return "EUR"

def get_eur_to_currency_rate(target_currency: str) -> float:
    """Taux de change EUR→devise — cache Supabase 60 min + curl_cffi."""
    if not target_currency or target_currency.upper() == "EUR":
        return 1.0
    cache_key = f"fx|EUR{target_currency.upper()}"
    cached = _cache_get(cache_key, _CACHE_TTL["fx"])
    if cached is not None:
        return float(cached)
    try:
        symbol = f"EUR{target_currency.upper()}=X"
        t = _yf_ticker(symbol)
        info = _yf_call(lambda: t.info) or {}
        rate = info.get("regularMarketPrice") or info.get("currentPrice")
        if not rate or rate <= 0:
            hist = _yf_call(lambda: t.history(period="5d"))
            if hist is not None and not hist.empty:
                rate = float(hist["Close"].iloc[-1])
        if rate and rate > 0:
            _cache_set(cache_key, rate)
            return float(rate)
    except Exception:
        pass
    return 1.0

def purge_old_cache(max_age_hours: int = 2):
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
        supabase.table("market_cache").delete().lt("updated_at", cutoff).execute()
    except Exception:
        pass

# ── Stubs pour éviter NameError sur les anciennes références CSV ──
ASSETS_DIR = WATCHLIST_DIR = EXPORTS_DIR = ""
COMPONENTS_CSV = INDICES_LIST_CSV = CUSTO_CSV = ""
WATCHLIST_CSV = WATCHLIST_INDEX = ""
SCRIPT_DIR = ""

# ============================================================
# STYLE CSS
# ============================================================
st.markdown("""
<style>
    /* ── Scorecard ── */
    .metric-card { padding: 15px; border-radius: 10px; margin-bottom: 10px; color: black; text-align: center; height: 130px; }
    .status-ok  { background-color: #d4edda; border: 2px solid #28a745; }
    .status-ko  { background-color: #f8d7da; border: 2px solid #dc3545; }
    .status-neu { background-color: #fff3cd; border: 2px solid #ffc107; }
    .metric-title  { font-size: 0.85em; font-weight: bold; margin-bottom: 5px; }
    .metric-value  { font-size: 1.5em;  font-weight: bold; }
    .metric-target { font-size: 0.75em; opacity: 0.8; }

    /* ── Score global ── */
    .score-box {
        border-radius: 14px; padding: 18px 24px; text-align: center;
        margin-bottom: 12px; color: white;
    }
    .score-A { background: linear-gradient(135deg,#1a7a3c,#28a745); }
    .score-B { background: linear-gradient(135deg,#1a6a1a,#5cb85c); }
    .score-C { background: linear-gradient(135deg,#7a5a00,#ffc107); color:#222; }
    .score-D { background: linear-gradient(135deg,#7a2a00,#fd7e14); }
    .score-F { background: linear-gradient(135deg,#6a0000,#dc3545); }
    .score-label { font-size:3.5rem; font-weight:900; line-height:1; }
    .score-sub   { font-size:0.85rem; opacity:0.85; }

    /* ── Réduire la largeur du bandeau latéral ── */
    section[data-testid="stSidebar"] {
        min-width: 200px !important;
        max-width: 220px !important;
        width: 220px !important;
    }

    /* ── Réduire l'espace en haut du contenu principal ── */
    .block-container {
        padding-top: 2rem !important;
    }

    /* ── Sidebar compacte ── */
    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 0.5rem !important; padding-bottom: 0.4rem !important;
    }
    section[data-testid="stSidebar"] h2 {
        margin-top:0 !important; margin-bottom:0.15rem !important;
        padding-top:0 !important; font-size:1.05rem !important;
    }
    section[data-testid="stSidebar"] h3 {
        margin-top:0.25rem !important; margin-bottom:0.1rem !important; font-size:0.9rem !important;
    }
    section[data-testid="stSidebar"] label {
        font-size:0.78rem !important; margin-bottom:0 !important; line-height:1.3 !important;
    }
    section[data-testid="stSidebar"] .stTextInput,
    section[data-testid="stSidebar"] .stSelectbox,
    section[data-testid="stSidebar"] .stButton,
    section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
        margin-bottom:0.15rem !important; gap:0.15rem !important;
    }
    section[data-testid="stSidebar"] hr   { margin-top:0.3rem !important; margin-bottom:0.3rem !important; }
    section[data-testid="stSidebar"] input {
        min-height:32px !important; font-size:0.8rem !important;
        padding-top:4px !important; padding-bottom:4px !important;
    }
    section[data-testid="stSidebar"] .stSelectbox > div > div {
        min-height:32px !important; font-size:0.8rem !important;
    }
    section[data-testid="stSidebar"] .stButton > button {
        padding:0.18rem 0.55rem !important; font-size:0.78rem !important; line-height:1.4 !important;
    }
    section[data-testid="stSidebar"] .stCaptionContainer,
    section[data-testid="stSidebar"] small,
    section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
        font-size:0.7rem !important; line-height:1.2 !important;
        margin-top:0.05rem !important; margin-bottom:0.05rem !important;
    }
    section[data-testid="stSidebar"] .stMarkdown p {
        margin-bottom:0.1rem !important; font-size:0.8rem !important; line-height:1.35 !important;
    }
    section[data-testid="stSidebar"] .stAlert {
        padding:0.3rem 0.5rem !important; font-size:0.75rem !important; margin-bottom:0.2rem !important;
    }

    /* ── Carte sigma ── */
    .sigma-header {
        background: linear-gradient(90deg,#1a1a2e 0%,#16213e 100%);
        border-radius:10px; padding:10px 16px; margin-bottom:6px; border-left:4px solid #4C9BE8;
    }
    .sigma-header h4 { margin:0; color:#e0e0e0; font-size:1rem; }
    .sigma-header span { color:#aaa; font-size:0.8rem; }

    /* ── Screener ── */
    .screener-hit {
        background:linear-gradient(90deg,#0d2137 0%,#0a1a2e 100%);
        border-radius:8px; padding:8px 14px; margin-bottom:4px; border-left:3px solid #28a745;
    }

    /* ── Navigation active ── */
    .nav-active {
        background: linear-gradient(90deg,#1a3a5c,#1e4a6e);
        border-radius: 8px; padding: 2px 8px;
        border-left: 3px solid #4C9BE8;
    }

    /* ── Live quote banner ── */
    .live-banner {
        background: linear-gradient(90deg,#0d1b2a 0%,#1a2a3a 100%);
        border-radius: 10px; padding: 12px 20px; margin-bottom: 10px;
        border-left: 4px solid #4C9BE8;
        display: flex; align-items: center; gap: 24px;
    }
    .live-name  { font-size: 1.1rem; font-weight: 700; color: #e0e0e0; }
    .live-price { font-size: 1.6rem; font-weight: 900; color: #ffffff; }
    .live-up    { font-size: 1rem; font-weight: 700; color: #28a745; }
    .live-down  { font-size: 1rem; font-weight: 700; color: #dc3545; }
    .live-neu   { font-size: 1rem; font-weight: 700; color: #ffc107; }
    .live-dot   { width: 8px; height: 8px; border-radius: 50%; background: #28a745;
                  display: inline-block; margin-right: 5px; animation: blink 1.2s infinite; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.2} }

    /* ── Yahoo link ── */
    .yahoo-link a {
        color: #4C9BE8 !important; font-size: 0.82rem; text-decoration: none;
    }
    .yahoo-link a:hover { text-decoration: underline; }

    /* ── Configuration table ── */
    .config-table { width: 100%; border-collapse: collapse; margin-top: 4px; }
    .config-table th {
        background: #1a2a3a; color: #aad4f5; font-size: 0.82rem;
        padding: 8px 12px; text-align: left; border-bottom: 2px solid #2a3a4a;
    }
    .config-table td {
        padding: 7px 12px; font-size: 0.85rem; border-bottom: 1px solid #1e2e3e; color: #ddd;
    }
    .config-table tr:hover td { background: #1a2a3a; }
    .badge-wiki   { background:#1a3a6a; color:#7ad4f5; border-radius:4px; padding:1px 7px; font-size:0.75rem; }
    .badge-custom { background:#2a1a4a; color:#c084fc; border-radius:4px; padding:1px 7px; font-size:0.75rem; }

    /* ── Page Présentation ── */
    .pres-hero {
        background: linear-gradient(135deg, #0d1b2a 0%, #1a2a4a 50%, #0d2137 100%);
        border-radius: 16px; padding: 40px 48px; margin-bottom: 28px;
        border: 1px solid #2a3a5a; text-align: center;
        box-shadow: 0 4px 32px rgba(76,155,232,0.12);
    }
    .pres-hero h1 { font-size: 3rem; font-weight: 900; color: #ffffff; margin: 0 0 8px 0;
        background: linear-gradient(90deg,#4C9BE8,#7ad4f5,#FFD700); -webkit-background-clip:text;
        -webkit-text-fill-color:transparent; }
    .pres-hero .tagline { font-size: 1.15rem; color: #aad4f5; margin-bottom: 18px; }
    .pres-hero .version-badge {
        display:inline-block; background:#1a3a6a; color:#7ad4f5;
        border-radius:20px; padding:3px 14px; font-size:0.82rem; font-weight:600;
        border: 1px solid #2a5a9a;
    }

    .feat-card {
        background: linear-gradient(135deg, #0d1b2a 0%, #0f2035 100%);
        border-radius: 12px; padding: 20px 18px; margin-bottom: 16px; height: 100%;
        border-left: 4px solid #4C9BE8; border-top: 1px solid #1e3a5a;
        transition: all 0.2s ease;
    }
    .feat-card.green  { border-left-color: #28a745; }
    .feat-card.gold   { border-left-color: #FFD700; }
    .feat-card.purple { border-left-color: #a855f7; }
    .feat-card.red    { border-left-color: #dc3545; }
    .feat-card.teal   { border-left-color: #20c997; }
    .feat-card.orange { border-left-color: #fd7e14; }
    .feat-icon { font-size: 2rem; margin-bottom: 8px; }
    .feat-title { font-size: 1rem; font-weight: 700; color: #e0e0e0; margin-bottom: 6px; }
    .feat-desc  { font-size: 0.82rem; color: #9ab; line-height: 1.5; }
    .feat-tag   { display:inline-block; background:#0d2137; color:#4C9BE8;
        border-radius:4px; padding:1px 8px; font-size:0.72rem; margin-top:8px; margin-right:4px;
        border:1px solid #1e3a5a; }
    .feat-tag.g { color:#28a745; border-color:#1a4a2a; background:#0a1f10; }
    .feat-tag.y { color:#FFD700; border-color:#3a3000; background:#1a1500; }
    .feat-tag.p { color:#c084fc; border-color:#3a1a6a; background:#150a2a; }

    .stat-badge {
        background: linear-gradient(135deg,#0d1b2a,#1a2a4a);
        border-radius:10px; padding:16px; text-align:center;
        border:1px solid #2a3a5a; margin-bottom:12px;
        height: 90px; display:flex; flex-direction:column;
        align-items:center; justify-content:center;
    }
    .stat-num  { font-size:2rem; font-weight:900; color:#4C9BE8; }
    .stat-lbl  { font-size:0.78rem; color:#7a9ab8; margin-top:2px; }

    .quickstart-step {
        background:#0d1b2a; border-radius:8px; padding:10px 14px; margin-bottom:8px;
        border-left:3px solid #FFD700; display:flex; align-items:center; gap:12px;
    }
    .qs-num { font-size:1.3rem; font-weight:900; color:#FFD700; min-width:28px; }
    .qs-text { font-size:0.85rem; color:#ccc; line-height:1.4; }

    /* ── Watchlist table ── */
    .wl-table { width:100%; border-collapse:collapse; margin-top:8px; font-size:0.87rem; }

    /* ── Encart sélection watchlist (pages avec checkboxes) ── */
    .wl-banner {
        background: linear-gradient(90deg,#0d1f30 0%,#0a1828 100%);
        border-radius: 10px; padding: 8px 16px; margin-bottom: 14px;
        border-left: 4px solid #FFD700;
        display: flex; align-items: center; gap: 14px;
    }
    .wl-banner-label { font-size: 0.82rem; color: #aaa; white-space: nowrap; }
    .wl-banner-name  { font-size: 0.95rem; font-weight: 700; color: #FFD700; }
    .wl-table th {
        background:#0f1f2e; color:#7ab8e8; font-size:0.78rem; font-weight:600;
        padding:9px 12px; text-align:left; border-bottom:2px solid #1e3a52;
        white-space:nowrap;
    }
    .wl-table td { padding:9px 12px; border-bottom:1px solid #152030; color:#ddd; vertical-align:middle; }
    .wl-table tr:hover td { background:#0d1d2d; }
    .wl-up   { color:#28a745; font-weight:700; }
    .wl-down { color:#dc3545; font-weight:700; }
    .wl-neu  { color:#ffc107; font-weight:700; }
    .wl-grade-A { background:#1a4a2a; color:#4ade80; border-radius:4px; padding:2px 8px; font-weight:700; }
    .wl-grade-B { background:#1a3a1a; color:#86efac; border-radius:4px; padding:2px 8px; font-weight:700; }
    .wl-grade-C { background:#3a2a00; color:#fbbf24; border-radius:4px; padding:2px 8px; font-weight:700; }
    .wl-grade-D { background:#3a1a00; color:#fb923c; border-radius:4px; padding:2px 8px; font-weight:700; }
    .wl-grade-F { background:#3a0a0a; color:#f87171; border-radius:4px; padding:2px 8px; font-weight:700; }
    .wl-note-cell { max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#aaa; font-size:0.8rem; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURATION
# ============================================================
INDICES_CONFIG = {
    # ── USA ───────────────────────────────────────────────────
    "SP500":       {"label": "S&P 500",            "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"},
    "NASDAQ":      {"label": "NASDAQ-100",          "url": "https://en.wikipedia.org/wiki/Nasdaq-100"},
    "DOWJONES":    {"label": "Dow Jones",            "url": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"},
    # ── Europe ────────────────────────────────────────────────
    "CAC40":       {"label": "CAC 40",               "url": "https://en.wikipedia.org/wiki/CAC_40"},
    "EUROSTOXX50": {"label": "EURO STOXX 50",        "url": "https://en.wikipedia.org/wiki/Euro_Stoxx_50"},
    "DAX":         {"label": "DAX (Allemagne)",      "url": "https://en.wikipedia.org/wiki/DAX"},
    "AEX":         {"label": "AEX (Pays-Bas)",       "url": "https://en.wikipedia.org/wiki/AEX_index"},
    "IBEX35":      {"label": "IBEX 35 (Espagne)",    "url": "https://en.wikipedia.org/wiki/IBEX_35"},
    "SMI":         {"label": "SMI (Suisse)",          "url": "https://en.wikipedia.org/wiki/Swiss_Market_Index"},
    "FTSEMIB":     {"label": "FTSE MIB (Italie)",    "url": "https://en.wikipedia.org/wiki/FTSE_MIB"},
    # ── Canada ────────────────────────────────────────────────
    "TSX60":       {"label": "S&P/TSX 60 (Canada)",  "url": "https://en.wikipedia.org/wiki/S%26P/TSX_60"},
}

SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))

# ── Dossiers ───────────────────────────────────────────────────
ASSETS_DIR       = os.path.join(SCRIPT_DIR, "SigmaScope_Assets")
WATCHLIST_DIR    = os.path.join(SCRIPT_DIR, "SigmaScope_Watchlist")
EXPORTS_DIR      = os.path.join(SCRIPT_DIR, "SigmaScope_Exports")

# ── Fichiers indices ───────────────────────────────────────────
COMPONENTS_CSV   = os.path.join(ASSETS_DIR, "Indice_Ticker_Wikipedia.csv")
INDICES_LIST_CSV = os.path.join(ASSETS_DIR, "Indice_List_Customization.csv")
CUSTO_CSV        = os.path.join(ASSETS_DIR, "Indice_Ticker_Customization.csv")

# ── Fichiers watchlist ─────────────────────────────────────────
WATCHLIST_CSV    = os.path.join(SCRIPT_DIR,    "watchlist.csv")   # legacy
WATCHLIST_INDEX  = os.path.join(WATCHLIST_DIR, "watchlists_index.csv")

# ── Créer les dossiers si absents ─────────────────────────────
os.makedirs(ASSETS_DIR,    exist_ok=True)
os.makedirs(WATCHLIST_DIR, exist_ok=True)
os.makedirs(EXPORTS_DIR,   exist_ok=True)

# Periods: internal labels → yfinance codes
_PERIODS_FR = {
    "1 Mois": "1mo", "3 Mois": "3mo", "6 Mois": "6mo",
    "1 An":   "1y",  "2 Ans":  "2y",  "5 Ans":  "5y",
    "10 Ans": "10y", "Max":    "max",
}
_PERIODS_EN = {
    "1 Month": "1mo", "3 Months": "3mo", "6 Months": "6mo",
    "1 Year":  "1y",  "2 Years":  "2y",  "5 Years":  "5y",
    "10 Years":"10y", "Max":      "max",
}
_PERIODS_TR = {
    "1 Ay":   "1mo", "3 Ay":   "3mo", "6 Ay":   "6mo",
    "1 Yıl":  "1y",  "2 Yıl":  "2y",  "5 Yıl":  "5y",
    "10 Yıl": "10y", "Maks":   "max",
}

def _get_periods():
    lang = st.session_state.get("lang", "fr")
    if lang == "en": return _PERIODS_EN
    if lang == "tr": return _PERIODS_TR
    return _PERIODS_FR

PERIODS = _PERIODS_FR  # fallback global (remplacé dynamiquement)


# Sigma zones: internal FR keys → (min, max, psycho_FR)
_SIGMA_DATA = [
    ("sz1",  1.75,  99.0, "sz1_psycho"),
    ("sz2",  1.25,  1.75, "sz2_psycho"),
    ("sz3",  0.75,  1.25, "sz3_psycho"),
    ("sz4",  0.25,  0.75, "sz4_psycho"),
    ("sz5", -0.25,  0.25, "sz5_psycho"),
    ("sz6", -0.75, -0.25, "sz6_psycho"),
    ("sz7", -1.25, -0.75, "sz7_psycho"),
    ("sz8", -1.75, -1.25, "sz8_psycho"),
    ("sz9", -99.0, -1.75, "sz9_psycho"),
]

def _get_sigma_criteria():
    """Build SIGMA_CRITERIA dict with translated labels."""
    _labels_fr = [
        "📈📈 Zone d'Excès Haut         (> +1,75σ)",
        "📈   Zone de Transition Haute  (+1,25 à +1,75σ)",
        "🚀   Zone de Tendance Forte    (+0,75 à +1,25σ)",
        "➕   Zone d'Attraction Positive (+0,25 à +0,75σ)",
        "〰️   Zone Neutre / Régression   (-0,25 à +0,25σ)",
        "➖   Zone d'Attraction Négative (-0,25 à -0,75σ)",
        "📉   Zone de Tendance Faible    (-0,75 à -1,25σ)",
        "🔻   Zone de Transition Basse   (-1,25 à -1,75σ)",
        "📉📉 Zone d'Excès Bas           (< -1,75σ)",
    ]
    _labels_en = [
        "📈📈 High Excess Zone           (> +1.75σ)",
        "📈   High Transition Zone       (+1.25 to +1.75σ)",
        "🚀   Strong Trend Zone          (+0.75 to +1.25σ)",
        "➕   Positive Attraction Zone   (+0.25 to +0.75σ)",
        "〰️   Neutral / Regression Zone  (-0.25 to +0.25σ)",
        "➖   Negative Attraction Zone   (-0.25 to -0.75σ)",
        "📉   Weak Trend Zone            (-0.75 to -1.25σ)",
        "🔻   Low Transition Zone        (-1.25 to -1.75σ)",
        "📉📉 Low Excess Zone             (< -1.75σ)",
    ]
    _labels_tr = [
        "📈📈 Yüksek Aşırılık Bölgesi     (> +1.75σ)",
        "📈   Yüksek Geçiş Bölgesi        (+1.25 ile +1.75σ)",
        "🚀   Güçlü Trend Bölgesi          (+0.75 ile +1.25σ)",
        "➕   Pozitif Çekim Bölgesi        (+0.25 ile +0.75σ)",
        "〰️   Nötr / Regresyon Bölgesi     (-0.25 ile +0.25σ)",
        "➖   Negatif Çekim Bölgesi        (-0.25 ile -0.75σ)",
        "📉   Zayıf Trend Bölgesi          (-0.75 ile -1.25σ)",
        "🔻   Düşük Geçiş Bölgesi          (-1.25 ile -1.75σ)",
        "📉📉 Düşük Aşırılık Bölgesi       (< -1.75σ)",
    ]
    _psycho_fr = [
        "Surchauffe, recherche de retour à la moyenne.",
        "Perte de souffle du mouvement haussier.",
        'Le "canal de hausse" idéal (Bull Run).',
        "Retour progressif vers la neutralité.",
        "Équilibre parfait, pas de direction claire.",
        "Dérive lente sous la moyenne.",
        "Canal de baisse (Bear market sain).",
        "Le pessimisme s'accentue avant l'excès.",
        'Panique, zone de "soldes" statistiques.',
    ]
    _psycho_en = [
        "Overheating, seeking mean reversion.",
        "Bullish momentum losing steam.",
        "Ideal upward channel (Bull Run).",
        "Gradual return toward neutrality.",
        "Perfect balance, no clear direction.",
        "Slow drift below the average.",
        "Downward channel (healthy Bear market).",
        "Pessimism deepening before excess.",
        "Panic, statistical bargain zone.",
    ]
    _psycho_tr = [
        "Aşırı ısınma, ortalamaya dönüş arayışı.",
        "Yükseliş momentumu zayıflıyor.",
        "İdeal yükseliş kanalı (Bull Run).",
        "Nötraliteye doğru kademeli dönüş.",
        "Mükemmel denge, net yön yok.",
        "Ortalamanın altında yavaş sürüklenme.",
        "Aşağı kanal (sağlıklı Bear market).",
        "Aşırılık öncesinde derinleşen karamsarlık.",
        "Panik, istatistiksel fırsat bölgesi.",
    ]
    lang = st.session_state.get("lang", "fr")
    labels = _labels_en if lang == "en" else _labels_tr if lang == "tr" else _labels_fr
    psychos = _psycho_en if lang == "en" else _psycho_tr if lang == "tr" else _psycho_fr
    return {
        lbl: (mn, mx, psy)
        for (_, mn, mx, __), lbl, psy
        in zip(_SIGMA_DATA, labels, psychos)
    }

SIGMA_CRITERIA = _get_sigma_criteria()  # fallback


# ============================================================
# STATS D'UTILISATION — FONCTIONS SUPABASE
# ============================================================

def record_session_start(user_id: str = None):
    """Enregistre le début d'une session utilisateur."""
    if user_id is None:
        user_id = get_user_id()
    try:
        now = datetime.now(timezone.utc).isoformat()
        supabase.table("usage_sessions").insert({
            "user_id":       user_id,
            "session_start": now,
        }).execute()
        st.session_state["_session_start"] = now
    except Exception:
        pass

def record_session_end(user_id: str = None):
    """Met à jour la durée de la session en cours."""
    if user_id is None:
        user_id = get_user_id()
    start_str = st.session_state.get("_session_start")
    if not start_str:
        return
    try:
        start  = datetime.fromisoformat(start_str)
        end    = datetime.now(timezone.utc)
        dur    = round((end - start).total_seconds() / 60, 1)
        supabase.table("usage_sessions")            .update({"session_end": end.isoformat(), "duration_min": dur})            .eq("user_id", user_id)            .eq("session_start", start_str)            .execute()
    except Exception:
        pass

def get_usage_stats() -> dict:
    """Retourne les statistiques globales d'utilisation."""
    try:
        res_users = supabase.table("usage_sessions")            .select("user_id", count="exact").execute()
        nb_sessions = res_users.count or 0

        res_distinct = supabase.table("usage_sessions")            .select("user_id").execute()
        distinct_users = len(set(r["user_id"] for r in res_distinct.data)) if res_distinct.data else 0

        res_dur = supabase.table("usage_sessions")            .select("duration_min").not_.is_("duration_min", "null").execute()
        durations = [r["duration_min"] for r in res_dur.data if r["duration_min"]]
        avg_dur = round(sum(durations) / len(durations), 1) if durations else 0

        return {
            "nb_sessions":    nb_sessions,
            "distinct_users": distinct_users,
            "avg_duration":   avg_dur,
        }
    except Exception:
        return {"nb_sessions": 0, "distinct_users": 0, "avg_duration": 0}

def get_user_rating(user_id: str = None):
    """Retourne la note et le vote SaaS de l'utilisateur, ou None."""
    if user_id is None:
        user_id = get_user_id()
    try:
        res = supabase.table("user_ratings")            .select("rating, vote_saas")            .eq("user_id", user_id).execute()
        return res.data[0] if res.data else None
    except Exception:
        return None

def save_user_rating(rating: int, vote_saas: bool, user_id: str = None):
    """Sauvegarde ou met à jour la note de l'utilisateur."""
    if user_id is None:
        user_id = get_user_id()
    try:
        supabase.table("user_ratings").upsert({
            "user_id":    user_id,
            "rating":     rating,
            "vote_saas":  vote_saas,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        return True
    except Exception:
        return False

def get_ratings_stats() -> dict:
    """Retourne les statistiques globales de notation."""
    try:
        res = supabase.table("user_ratings").select("rating, vote_saas").execute()
        if not res.data:
            return {"nb_ratings": 0, "avg_rating": 0, "nb_saas_yes": 0, "nb_saas_no": 0}
        ratings   = [r["rating"]   for r in res.data]
        votes     = [r["vote_saas"] for r in res.data if r["vote_saas"] is not None]
        return {
            "nb_ratings":  len(ratings),
            "avg_rating":  round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "nb_saas_yes": sum(1 for v in votes if v is True),
            "nb_saas_no":  sum(1 for v in votes if v is False),
        }
    except Exception:
        return {"nb_ratings": 0, "avg_rating": 0, "nb_saas_yes": 0, "nb_saas_no": 0}

def get_feedback_messages(limit: int = 50, include_private: bool = False) -> list:
    """
    Retourne les derniers messages de feedback.
    include_private=True : retourne tous les messages (admin uniquement).
    include_private=False : retourne uniquement les messages publics.
    """
    try:
        q = supabase.table("user_feedback")            .select("message, created_at, is_private")            .order("created_at", desc=True)            .limit(limit)
        if not include_private:
            q = q.eq("is_private", False)
        res = q.execute()
        return res.data or []
    except Exception:
        return []

def save_feedback(message: str, is_private: bool = False, user_id: str = None) -> bool:
    """Sauvegarde un message de feedback (public ou privé)."""
    if user_id is None:
        user_id = get_user_id()
    try:
        supabase.table("user_feedback").insert({
            "user_id":    user_id,
            "message":    message.strip(),
            "is_private": is_private,
        }).execute()
        return True
    except Exception:
        return False


# ============================================================
# GET_LABEL — doit être défini AVANT la sidebar
# ============================================================
def get_label(key):
    if key in INDICES_CONFIG:
        return INDICES_CONFIG[key]["label"]
    # Grands indices libres
    if key == "Indice":
        return "📊 Grands Indices"
    # Custom / ETF / Crypto / Matières premières
    if key == "@ETF@":
        return "📦 ETF"
    if key == "@Crypto@":
        return "🪙 Crypto"
    if key == "@Matière Première@":
        return "🪨 Matières Premières"
    # Indices français custom
    if key == "SBF120":
        return "SBF 120"
    if key == "CACMID60":
        return "CAC Mid 60"
    if key == "CACSMAL" or key == "CACSMILL" or key == "CACSMAL" or key == "CACSMALL":
        return "CAC Small"
    return key

# ============================================================
# IMPORT PORTFOLIO PERFORMANCE XML
# ============================================================

def parse_portfolio_performance_xml(xml_bytes):
    """
    Analyse un fichier XML Portfolio Performance.
    Retourne un dict :
      { portfolio_name: [ {ticker, name, shares, pru, total_cost}, ... ] }
    Les valeurs sont calculées en agrégeant les transactions BUY/SELL/TRANSFER.
    """
    import xml.etree.ElementTree as ET
    import re
    from collections import defaultdict

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f"Fichier XML invalide : {e}")

    # Index des titres (1-based, comme les références XPath dans PP)
    securities_list = root.findall('.//securities/security')

    def resolve_ref(ref):
        """Résout une référence XPath PP vers l'élément security."""
        m = re.search(r'security\[(\d+)\]', ref)
        idx = int(m.group(1)) - 1 if m else 0
        if 0 <= idx < len(securities_list):
            return securities_list[idx]
        return None

    def find_real_portfolios(root):
        """Trouve les vrais éléments <portfolio> (pas les références)."""
        result = []
        seen_names = set()
        for p in root.iter('portfolio'):
            if 'reference' in p.attrib:
                continue
            name = p.findtext('name', '').strip()
            if name and name not in seen_names:
                seen_names.add(name)
                result.append(p)
        return result

    real_portfolios = find_real_portfolios(root)
    if not real_portfolios:
        raise ValueError("Aucun compte titre trouvé dans ce fichier XML.")

    result = {}
    for port in real_portfolios:
        port_name = port.findtext('name', '').strip()
        holdings = defaultdict(lambda: {
            'shares': 0.0, 'total_cost': 0.0,
            'name': '', 'ticker': '', 'isin': ''
        })

        for ptx in port.findall('.//portfolio-transaction'):
            tx_type = ptx.findtext('type', '')
            if tx_type not in ('BUY', 'SELL', 'TRANSFER_IN', 'TRANSFER_OUT'):
                continue
            sec_el = ptx.find('security')
            if sec_el is None:
                continue
            ref = sec_el.get('reference', '')
            sec = resolve_ref(ref) if ref else None
            if sec is None:
                continue

            ticker = sec.findtext('tickerSymbol', '').strip()
            sec_name = sec.findtext('n', '').strip()
            isin = sec.findtext('isin', '').strip()
            if not ticker:
                continue

            # Portfolio Performance : shares * 1e8, montants en centimes
            sh = int(ptx.findtext('shares', '0') or 0) / 1e8
            am_total = int(ptx.findtext('amount', '0') or 0) / 100.0

            # Extraire les frais depuis <units><unit type="FEE">
            # PP calcule le PRU sur le montant HORS frais (gross = amount - fees)
            fees = 0.0
            units_el = ptx.find('units')
            if units_el is not None:
                for unit in units_el.findall('unit'):
                    if unit.get('type', '') == 'FEE':
                        amt_el = unit.find('amount')
                        if amt_el is not None:
                            fees += int(amt_el.get('amount', 0)) / 100.0

            # Montant hors frais = gross utilisé par PP pour le calcul du PRU
            am_gross = am_total - fees

            if tx_type in ('BUY', 'TRANSFER_IN'):
                holdings[ticker]['shares']     += sh
                holdings[ticker]['total_cost'] += am_gross
                holdings[ticker]['name']        = sec_name
                holdings[ticker]['ticker']      = ticker
                holdings[ticker]['isin']        = isin
            elif tx_type in ('SELL', 'TRANSFER_OUT'):
                # Pour les ventes : on retire au PRU moyen pondéré courant
                # (comme le fait PP — les frais de vente n'impactent pas le PRU)
                if holdings[ticker]['shares'] > 0:
                    pru_before = holdings[ticker]['total_cost'] / holdings[ticker]['shares']
                    holdings[ticker]['total_cost'] -= pru_before * sh
                holdings[ticker]['shares'] -= sh

        # Ne conserver que les lignes avec position active
        active = []
        for ticker, h in holdings.items():
            if h['shares'] > 0.0001:
                pru = round(h['total_cost'] / h['shares'], 2) if h['shares'] > 0 else 0.0
                active.append({
                    'ticker':     ticker,
                    'name':       h['name'],
                    'isin':       h['isin'],
                    'shares':     round(h['shares'], 6),
                    'total_cost': round(h['total_cost'], 2),
                    'pru':        pru,
                })
        if active:
            result[port_name] = active

    return result


@st.dialog("📥 Importer depuis Portfolio Performance", width="large")
def dialog_import_portfolio():
    """
    Dialogue d'import d'un fichier XML Portfolio Performance.
    Étape 1 : upload + parsing
    Étape 2 : sélection du compte titre
    Étape 3 : nom watchlist + confirmation
    NB : aucun st.rerun() à l'intérieur — les étapes sont gérées
    par session_state mis à jour et relu dans le même rendu.
    """
    # ── Lecture de l'état courant ────────────────────────────────
    step   = st.session_state.get("import_step", 1)
    parsed = st.session_state.get("import_parsed", None)

    # ── Indicateur de progression ────────────────────────────────
    prog_labels = ["📂 Chargement", "🏦 Compte titre", "✅ Confirmation"]
    prog_html = "".join(
        f'<span style="'
        f'background:{"#4C9BE8" if i+1==step else "#1a2a3a"};'
        f'color:{"#fff" if i+1==step else "#666"};'
        f'border-radius:20px;padding:3px 12px;font-size:0.78rem;margin-right:6px">'
        f'{lbl}</span>'
        for i, lbl in enumerate(prog_labels)
    )
    st.markdown(f'<div style="margin-bottom:12px">{prog_html}</div>', unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════
    # ÉTAPE 1 — Upload du fichier
    # ════════════════════════════════════════════════════════════
    if step == 1:
        st.markdown(t("pp_upload_md"))
        st.caption(t("pp_upload_cap"))

        uploaded = st.file_uploader(
            "Fichier .xml",
            type=["xml"],
            key="import_xml_uploader",
            label_visibility="collapsed",
        )

        # Dès qu'un fichier est chargé, on le parse et on passe à l'étape 2
        if uploaded is not None:
            # Vérifier si c'est un nouveau fichier (différent du dernier)
            file_id = f"{uploaded.name}_{uploaded.size}"
            if st.session_state.get("import_last_file_id") != file_id:
                try:
                    xml_bytes = uploaded.read()
                    parsed = parse_portfolio_performance_xml(xml_bytes)
                    st.session_state.import_parsed       = parsed
                    st.session_state.import_step         = 2
                    st.session_state.import_last_file_id = file_id
                    step = 2   # on continue dans ce même rendu
                except Exception as e:
                    st.error(t("pp_parse_err", e=e))
                    uploaded = None

        if step == 1:  # toujours étape 1 (pas encore de fichier valide)
            st.info(t("pp_upload_info"))
            if st.button("❌ Fermer", use_container_width=True, key="import_cancel_1"):
                st.session_state.import_step         = 1
                st.session_state.import_parsed       = None
                st.session_state.import_last_file_id = None
                st.rerun()   # ici c'est OK : on ferme le dialogue volontairement

    # ════════════════════════════════════════════════════════════
    # ÉTAPE 2 — Sélection du compte titre
    # ════════════════════════════════════════════════════════════
    if step == 2 and parsed:
        st.markdown(t("pp_select_md"))
        port_names = list(parsed.keys())

        selected_port = st.selectbox(
            "Compte titre",
            options=port_names,
            key="import_port_select",
        )

        # Aperçu du compte sélectionné
        if selected_port and selected_port in parsed:
            holdings = parsed[selected_port]
            st.caption(t("pp_positions_cap", n=len(holdings), name=selected_port))
            st.caption(t("pp_pru_note"))
            preview_rows = []
            for h in holdings[:12]:
                preview_rows.append({
                    "Ticker":  h['ticker'],
                    "Nom":     h['name'] or "—",
                    "Parts":   f"{h['shares']:.2f}",
                    "PRU (€)": f"{h['pru']:.2f}",
                })
            st.dataframe(
                pd.DataFrame(preview_rows),
                use_container_width=True,
                hide_index=True,
                height=min(38 * len(preview_rows) + 38, 340),
            )
            if len(holdings) > 12:
                st.caption(t("pp_more_pos", n=len(holdings) - 12))

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            next2 = st.button("▶️ Suivant", type="primary", use_container_width=True, key="import_next_2")
        with c2:
            back2 = st.button("◀️ Retour",  use_container_width=True, key="import_back_2")
        with c3:
            cancel2 = st.button("❌ Annuler", use_container_width=True, key="import_cancel_2")

        if next2:
            st.session_state.import_selected_port = selected_port
            st.session_state.import_step = 3
            step = 3   # on tombe directement dans l'étape 3 ci-dessous
        if back2 or cancel2:
            st.session_state.import_step         = 1
            st.session_state.import_parsed       = None
            st.session_state.import_last_file_id = None
            st.rerun()   # ferme le dialogue → retour propre à étape 1

    # ════════════════════════════════════════════════════════════
    # ÉTAPE 3 — Nom watchlist + confirmation
    # ════════════════════════════════════════════════════════════
    if step == 3 and parsed:
        selected_port = st.session_state.get("import_selected_port", "")
        holdings      = parsed.get(selected_port, [])

        st.markdown(t("pp_confirm_md"))

        wl_name = st.text_input(
            "Nom de la watchlist",
            value=selected_port.strip(),
            key="import_wl_name",
            help=t("pp_wl_name_help"),
        )

        existing = load_wl_index()
        if wl_name.strip() in existing:
            st.warning(t("pp_wl_exists_warn", name=wl_name.strip()))
        else:
            st.info(t("pp_wl_new_info", name=wl_name.strip() or "…"))

        st.caption(
            f"📥 **{len(holdings)} position(s)** seront importées "
            f"depuis **{selected_port}** avec : Ticker, Nom, PRU en EUR."
        )

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            confirm3 = st.button("📥 Importer", type="primary", use_container_width=True, key="import_confirm_3")
        with c2:
            back3 = st.button("◀️ Retour",  use_container_width=True, key="import_back_3")
        with c3:
            cancel3 = st.button("❌ Annuler", use_container_width=True, key="import_cancel_3")

        if confirm3:
            name = wl_name.strip()
            if not name:
                st.error(t("pp_name_empty"))
            else:
                if name not in existing:
                    create_watchlist(name)
                rows = []
                for h in holdings:
                    # PRU conservé en EUR tel quel (devise Portfolio Performance)
                    prix_achat_str = f"{h['pru']:.4f}" if h['pru'] else ""
                    rows.append({
                        "ticker":     h['ticker'].strip().upper(),
                        "company":    h['name'],
                        "ajout_date": datetime.now().strftime("%Y-%m-%d"),
                        "note":       f"ISIN:{h['isin']}" if h['isin'] else "",
                        "prix_achat": prix_achat_str,
                    })
                df_import = pd.DataFrame(rows, columns=WATCHLIST_COLS)
                save_watchlist(df_import, name=name)
                st.session_state.active_watchlist    = name
                st.session_state.import_step         = 1
                st.session_state.import_parsed       = None
                st.session_state.import_last_file_id = None
                st.toast(t("pp_import_toast", n=len(rows), name=name), icon="📥")
                st.rerun()

        if back3:
            st.session_state.import_step = 2
            st.rerun()

        if cancel3:
            st.session_state.import_step         = 1
            st.session_state.import_parsed       = None
            st.session_state.import_last_file_id = None
            st.rerun()







# ============================================================
# WATCHLISTS COMME SOURCES D'INDICES
# Préfixe interne pour distinguer les watchlists des indices
# ============================================================
WL_KEY_PREFIX = "@WL@"

def wl_key(name):
    """Retourne la clé interne d'une watchlist (ex: '@WL@Ma Watchlist')."""
    return f"{WL_KEY_PREFIX}{name}"

def is_wl_key(key):
    return str(key).startswith(WL_KEY_PREFIX)

def wl_name_from_key(key):
    return str(key)[len(WL_KEY_PREFIX):]

def get_all_data_with_watchlists(all_data_base):
    """
    Retourne all_data enrichi des watchlists comme pseudo-indices.
    Les watchlists sont injectées avec la clé '@WL@<nom>'.
    """
    result = dict(all_data_base)
    for wl_name in load_wl_index():
        df_wl = load_watchlist(wl_name)
        if not df_wl.empty:
            df_source = df_wl[["ticker", "company"]].copy()
            df_source.columns = ["Ticker", "Company"]
            df_source["Ticker"]  = df_source["Ticker"].astype(str).str.strip()
            df_source["Company"] = df_source["Company"].astype(str).str.strip()
            result[wl_key(wl_name)] = df_source
    return result

def get_label_extended(key):
    """Version étendue de get_label qui gère aussi les clés watchlist."""
    if is_wl_key(key):
        return f"⭐ {wl_name_from_key(key)}"
    return get_label(key)



# ============================================================
# BOUTON WATCHLIST UNIVERSEL
# ============================================================

def watchlist_button(ticker, company="", key_suffix=""):
    """
    Bouton ⭐/✅ pour ajouter/retirer de la watchlist.
    - Ajout  → ouvre le dialogue de sélection de watchlist
    - Retrait → action directe (watchlist active)
    """
    active_wl = st.session_state.get("active_watchlist", "Ma Watchlist")
    in_wl  = is_in_watchlist(ticker, name=active_wl)
    label  = "✅ Watchlist" if in_wl else "⭐ Watchlist"
    if st.button(label, key=f"wl_btn_{ticker}_{key_suffix}",
                 help=t("wl_btn_remove_help" if in_wl else "wl_btn_add_help", name=active_wl),
                 use_container_width=False):
        if in_wl:
            remove_from_watchlist(ticker, name=active_wl)
            st.toast(t("wl_toast_removed", ticker=ticker), icon="🗑️")
            st.rerun()
        else:
            st.session_state.wl_pending_action = {
                "ticker":  ticker,
                "company": company,
                "action":  "add",
            }
            dialog_confirm_wl_add()


# ============================================================
# DIALOGUE SÉLECTION WATCHLIST  (décorateur @st.dialog)
# Ouverture automatique détectée au rerun quand une coche ⭐
# vient d'être activée dans un data_editor.
# ============================================================

@st.dialog("🗂️ Dans quelle watchlist ajouter ce ticker ?")
def dialog_confirm_wl_add():
    """
    Dialogue ouvert automatiquement après détection d'une coche ⭐.
    Permet de choisir la watchlist cible AVANT d'effectuer l'ajout.
    """
    pending = st.session_state.get("wl_pending_action")   # {"ticker": ..., "company": ..., "action": "add"|"remove"}
    if not pending:
        st.warning(t("wl_no_pending"))
        if st.button(t("wl_close_btn")):
            st.rerun()
        return

    tkr     = pending["ticker"]
    company = pending.get("company", "")
    action  = pending.get("action", "add")

    if action == "add":
        st.markdown(t("wl_dialog_add_label_inline", ticker=tkr, company=company))
        wl_names = load_wl_index()
        current  = st.session_state.get("active_watchlist", wl_names[0])
        chosen   = st.selectbox(
            "Watchlist cible",
            options=wl_names,
            index=wl_names.index(current) if current in wl_names else 0,
            key="dialog_wl_add_select",
        )
        # Aperçu du contenu
        df_prev = load_watchlist(chosen)
        n = len(df_prev)
        already = tkr.upper() in df_prev["ticker"].str.upper().values
        if already:
            st.warning(f"⚠️ `{tkr}` est déjà dans **{chosen}**.")
        else:
            tickers_prev = ", ".join(df_prev["ticker"].tolist()[:6])
            suffix = "…" if n > 6 else ""
            st.caption(f"📋 {n} action(s) : {tickers_prev}{suffix}" if n > 0 else "📋 Watchlist vide")

        col_ok, col_cancel = st.columns(2)
        with col_ok:
            if st.button(t("wl_dialog_add_btn"), use_container_width=True, type="primary", disabled=already):
                st.session_state.active_watchlist = chosen
                add_to_watchlist(tkr, company, name=chosen)
                st.session_state.wl_pending_action = None
                st.toast(t("wl_toast_added", ticker=tkr, name=chosen), icon="✅")
                st.rerun()
        with col_cancel:
            if st.button("❌ Annuler", use_container_width=True):
                st.session_state.wl_pending_action = None
                st.rerun()

    else:  # action == "remove"
        st.markdown(f"**Retirer `{tkr}`** de la watchlist **{st.session_state.active_watchlist}** ?")
        col_ok, col_cancel = st.columns(2)
        with col_ok:
            if st.button(t("wl_dialog_remove_btn"), use_container_width=True, type="primary"):
                remove_from_watchlist(tkr, name=st.session_state.active_watchlist)
                st.session_state.wl_pending_action = None
                st.toast(f"❌ {tkr} retiré de la watchlist", icon="🗑️")
                st.rerun()
        with col_cancel:
            if st.button("❌ Annuler", use_container_width=True):
                st.session_state.wl_pending_action = None
                st.rerun()


def _check_wl_toggle(edited_df, prev_key, ticker_col, company_col, wl_col):
    """
    Compare l'état actuel du data_editor avec l'état précédent (stocké en session_state).
    Si une coche ⭐ vient de changer, stocke l'action en attente et ouvre le dialogue.

    edited_df   : DataFrame retourné par st.data_editor
    prev_key    : clé session_state pour mémoriser l'état précédent
    ticker_col  : nom de la colonne Ticker
    company_col : nom de la colonne Société/Nom (peut être None)
    wl_col      : nom de la colonne checkbox watchlist (ex: "⭐ WL")
    """
    active_wl = st.session_state.get("active_watchlist", "Ma Watchlist")
    wl_set    = set(load_watchlist(active_wl)["ticker"].str.upper().tolist())

    # État précédent (dict ticker -> bool)
    prev_state = st.session_state.get(prev_key, {})

    changed_ticker  = None
    changed_company = ""
    changed_action  = None

    for _, row in edited_df.iterrows():
        tkr       = row[ticker_col]
        now_check = bool(row[wl_col])
        was_check = prev_state.get(tkr, tkr.upper() in wl_set)

        if now_check != was_check:
            changed_ticker  = tkr
            changed_company = row[company_col] if company_col and company_col in row else ""
            changed_action  = "add" if now_check else "remove"
            break   # on traite un changement à la fois

    # Mémoriser l'état courant pour le prochain rerun
    new_state = {row[ticker_col]: bool(row[wl_col]) for _, row in edited_df.iterrows()}
    st.session_state[prev_key] = new_state

    if changed_ticker:
        st.session_state.wl_pending_action = {
            "ticker":  changed_ticker,
            "company": changed_company,
            "action":  changed_action,
        }
        dialog_confirm_wl_add()

    return changed_ticker is None   # True = rien n'a changé, False = dialogue ouvert







def scrape_index(index_key):
    import requests
    from io import StringIO
    cfg = INDICES_CONFIG[index_key]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    response = requests.get(cfg["url"], headers=headers, timeout=15)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))

    def find_table(tables, ticker_cands, name_cands):
        for t in tables:
            cols_lower = [str(c).lower().strip() for c in t.columns]
            ct = next((t.columns[i] for i, c in enumerate(cols_lower) if c in ticker_cands), None)
            cn = next((t.columns[i] for i, c in enumerate(cols_lower) if c in name_cands), None)
            if ct and cn:
                df = t[[ct, cn]].copy().dropna()
                df.columns = ["Ticker", "Company"]
                df["Ticker"]  = df["Ticker"].astype(str).str.strip()
                df["Company"] = df["Company"].astype(str).str.strip()
                return df
        return None

    def _add_suffix(df, suffix):
        """Ajoute un suffixe Yahoo Finance aux tickers qui n'en ont pas."""
        df["Ticker"] = df["Ticker"].apply(
            lambda x: x if ("." in str(x) or str(x).startswith("^")) else str(x) + suffix
        )
        return df

    if index_key == "SP500":
        df = tables[0].rename(columns={"Symbol": "Ticker", "Security": "Company"})
        df = df[["Ticker", "Company"]].dropna()
        df["Ticker"]  = df["Ticker"].astype(str).str.strip()
        df["Company"] = df["Company"].astype(str).str.strip()
        return df

    elif index_key == "NASDAQ":
        df = find_table(tables, ["ticker","symbol"], ["company","name","security"])
        if df is None: raise ValueError("Table NASDAQ-100 introuvable.")
        return df

    elif index_key == "DOWJONES":
        df = find_table(tables, ["symbol","ticker"], ["company","name"])
        if df is None: raise ValueError("Table Dow Jones introuvable.")
        return df

    elif index_key == "CAC40":
        df = find_table(tables, ["ticker","symbol"], ["company","name","entreprise","société"])
        if df is None: raise ValueError("Table CAC 40 introuvable.")
        return _add_suffix(df, ".PA")

    elif index_key == "EUROSTOXX50":
        df = find_table(tables, ["ticker","symbol"], ["company","name"])
        if df is None: raise ValueError("Table EURO STOXX 50 introuvable.")
        return df

    elif index_key == "DAX":
        df = find_table(tables, ["ticker","symbol"], ["company","name","unternehmen"])
        if df is None: raise ValueError("Table DAX introuvable.")
        return _add_suffix(df, ".DE")

    elif index_key == "AEX":
        df = find_table(tables, ["ticker","symbol"], ["company","name","bedrijf"])
        if df is None: raise ValueError("Table AEX introuvable.")
        return _add_suffix(df, ".AS")

    elif index_key == "IBEX35":
        df = find_table(tables, ["ticker","symbol"], ["company","name","empresa","componente"])
        if df is None: raise ValueError("Table IBEX 35 introuvable.")
        return _add_suffix(df, ".MC")

    elif index_key == "SMI":
        df = find_table(tables, ["ticker","symbol"], ["company","name","unternehmen"])
        if df is None: raise ValueError("Table SMI introuvable.")
        return _add_suffix(df, ".SW")

    elif index_key == "FTSEMIB":
        df = find_table(tables, ["ticker","symbol"], ["company","name","azienda"])
        if df is None: raise ValueError("Table FTSE MIB introuvable.")
        return _add_suffix(df, ".MI")

    elif index_key == "TSX60":
        df = find_table(tables, ["ticker","symbol"], ["company","name"])
        if df is None: raise ValueError("Table S&P/TSX 60 introuvable.")
        return _add_suffix(df, ".TO")

    raise ValueError(f"Indice inconnu : {index_key}")


# ============================================================
# CALCUL RÉGRESSION & SIGMA
# ============================================================

def compute_regression(hist):
    df = hist['Close'].to_frame(name='Prix').reset_index()
    df['Days'] = np.arange(len(df)) + 1
    x, y    = np.log(df['Days']), np.log(df['Prix'])
    fit     = np.polyfit(x, y, 1)
    log_reg = fit[0] * x + fit[1]
    std_dev = (y - log_reg).std()
    df['Regression'] = np.exp(log_reg)
    df['Sigma_+2']   = np.exp(log_reg + 2*std_dev)
    df['Sigma_+1']   = np.exp(log_reg + std_dev)
    df['Sigma_-1']   = np.exp(log_reg - std_dev)
    df['Sigma_-2']   = np.exp(log_reg - 2*std_dev)
    sigma_position   = (np.log(df['Prix'].iloc[-1]) - log_reg.iloc[-1]) / std_dev
    return df, std_dev, sigma_position


def build_regression_chart(df, ticker, company="", yaxis_type="linear", display_mode="cours"):
    """
    Construit le graphique régression/sigma.
    yaxis_type   : "linear" | "log"   — échelle de l'axe Y (linéaire ou logarithmique)
    display_mode : "cours"  | "pct"   — valeur brute du cours ou variation en % depuis le début
    """
    title_suffix = ""
    if yaxis_type == "log":
        title_suffix += "  [log]"
    if display_mode == "pct":
        title_suffix += "  [%]"

    title    = f"{ticker}" + (f" — {company}" if company else "") + title_suffix
    date_col = df.columns[0]
    fig      = go.Figure()

    palette = {
        'Prix':       ('#FFFFFF', 2.5, 'solid'),
        'Regression': ('#FFD700', 1.5, 'dash'),
        'Sigma_+2':   ('#FF4C4C', 1,   'dot'),
        'Sigma_+1':   ('#FFA07A', 1,   'dot'),
        'Sigma_-1':   ('#90EE90', 1,   'dot'),
        'Sigma_-2':   ('#3CB371', 1,   'dot'),
    }

    # Référence pour le mode % = premier cours disponible
    if display_mode == "pct":
        base_price = float(df['Prix'].iloc[0]) or 1.0
    else:
        base_price = None

    for col, (color, width, dash) in palette.items():
        if display_mode == "pct":
            y_vals      = (df[col] / base_price - 1) * 100
            hover_fmt   = f"{col}: %{{y:.2f}}%<extra></extra>"
        else:
            y_vals      = df[col]
            hover_fmt   = f"{col}: %{{y:.2f}}<extra></extra>"

        fig.add_trace(go.Scatter(
            x=df[date_col], y=y_vals, mode='lines', name=col,
            line=dict(color=color, width=width, dash=dash),
            hovertemplate=hover_fmt
        ))

    # Axe Y : libellé et format selon le mode
    if display_mode == "pct":
        yaxis_cfg = dict(
            type=yaxis_type, showgrid=True, gridcolor='#333',
            title="Variation (%)", ticksuffix="%",
        )
    else:
        yaxis_cfg = dict(
            type=yaxis_type, showgrid=True, gridcolor='#333',
            title="Cours",
        )

    fig.update_layout(
        title=dict(text=title, font_size=14), height=300,
        margin=dict(l=55, r=20, t=38, b=30), template="plotly_dark",
        legend=dict(orientation="h", y=-0.15, font_size=10),
        hovermode="x unified",
        xaxis=dict(showgrid=False),
        yaxis=yaxis_cfg,
    )
    return fig

# ============================================================
# CALCUL FONDAMENTAUX COMPLETS + SCORE
# ============================================================

def compute_fundamentals(ticker):
    import math

    info = get_info(ticker)
    fin, bs, cf = get_financials(ticker)

    def row_first(df, labels):
        for lbl in labels:
            if lbl in df.index:
                s = df.loc[lbl].dropna()
                if len(s) > 0:
                    v = s.iloc[0]
                    if v is not None and not (isinstance(v, float) and math.isnan(v)):
                        return float(v)
        return None

    def row_series(df, labels):
        for lbl in labels:
            if lbl in df.index:
                s = df.loc[lbl].dropna()
                if len(s) >= 2:
                    return s
        return None

    def get_cagr(series):
        if series is None or len(series) < 2:
            return None
        try:
            v0, vn = float(series.iloc[-1]), float(series.iloc[0])
            if v0 <= 0 or vn <= 0:
                return None
            return ((vn / v0) ** (1 / len(series)) - 1) * 100
        except:
            return None

    def safe_info(key, default=None):
        v = info.get(key, default)
        if v is None: return default
        if isinstance(v, float) and math.isnan(v): return default
        return v

    REV_LABELS = [
        'Total Revenue', 'Operating Revenue', 'Net Interest Income',
        'Total Interest Income', 'Gross Profit', 'Net Premiums Written', 'Revenue',
    ]
    rev_series = row_series(fin, REV_LABELS)
    rev_growth = get_cagr(rev_series)
    rev_last   = row_first(fin, REV_LABELS)
    if rev_last is None:
        rev_last = safe_info('totalRevenue', 1) or 1
    if rev_last == 0:
        rev_last = 1

    FCF_LABELS = [
        'Free Cash Flow', 'Operating Cash Flow', 'Cash Flow From Operations',
        'Net Cash Provided By Operating Activities',
    ]
    fcf_series = row_series(cf, FCF_LABELS)
    fcf_growth = get_cagr(fcf_series)
    fcf        = row_first(cf, FCF_LABELS)
    if fcf is None:
        fcf = safe_info('freeCashflow') or safe_info('operatingCashflow') or 0

    fcf_margin = (fcf / rev_last) * 100 if rev_last and fcf else 0.0

    roic = None
    roe_val = None
    try:
        EBIT_LABELS   = ['EBIT', 'Operating Income', 'Net Income Before Taxes', 'Pretax Income', 'Income Before Tax']
        ASSETS_LABELS = ['Total Assets', 'Total Assets Net', 'Assets']
        LIAB_LABELS   = ['Current Liabilities', 'Current Liabilities Net', 'Total Current Liabilities', 'Payables And Accrued Expenses']
        ebit   = row_first(fin, EBIT_LABELS)
        assets = row_first(bs, ASSETS_LABELS)
        liab   = row_first(bs, LIAB_LABELS) or 0
        if ebit is not None and assets is not None:
            ic = assets - liab
            if ic > 0:
                roic = (ebit / ic) * 100
    except:
        pass
    if roic is None:
        roe_raw = safe_info('returnOnEquity')
        roa = safe_info('returnOnAssets')
        roi = safe_info('returnOnInvestment') or safe_info('returnOnCapital')
        if roi: roic = roi * 100
        elif roe_raw: roic = roe_raw * 100
        elif roa: roic = roa * 100

    try:
        roe_info = safe_info('returnOnEquity')
        if roe_info is not None:
            roe_val = roe_info * 100
        else:
            NI_LABELS  = ['Net Income', 'Net Income Common Stockholders', 'Net Income From Continuing Operations']
            EQ_LABELS  = ['Stockholders Equity', 'Total Stockholders Equity', 'Common Stock Equity',
                          'Total Equity Gross Minority Interest', 'Stockholders Equity Net Minority Interest']
            net_income = row_first(fin, NI_LABELS)
            equity     = row_first(bs,  EQ_LABELS)
            if net_income is not None and equity is not None and equity != 0:
                roe_val = (net_income / abs(equity)) * 100
    except:
        pass

    total_debt = safe_info('totalDebt', 0) or 0
    total_cash = safe_info('totalCash', 0) or 0
    if total_debt == 0:
        DEBT_LABELS = ['Long Term Debt', 'Total Debt', 'Long Term Debt And Capital Lease Obligation',
                       'Total Liabilities Net Minority Interest']
        total_debt = row_first(bs, DEBT_LABELS) or 0
    debt_fcf = (total_debt - total_cash) / fcf if fcf and fcf != 0 else None

    pe_ratio = safe_info('trailingPE') or safe_info('forwardPE')
    peg      = safe_info('pegRatio')

    if peg is None and pe_ratio is not None and pe_ratio > 0:
        eg = safe_info('earningsGrowth')
        rg = safe_info('revenueGrowth')
        growth_pct = None
        if eg is not None and eg > 0:
            growth_pct = eg * 100
        elif rg is not None and rg > 0:
            growth_pct = rg * 100
        elif rev_growth is not None and rev_growth > 0:
            growth_pct = rev_growth
        if growth_pct is None:
            eps_t = safe_info('trailingEps')
            eps_f = safe_info('forwardEps')
            if eps_t and eps_f and eps_t > 0:
                growth_pct = ((eps_f / eps_t) - 1) * 100
        if growth_pct is not None and growth_pct > 0:
            peg = round(pe_ratio / growth_pct, 2)

    pb   = safe_info('priceToBook')
    pfcf = None
    mktcap = safe_info('marketCap')
    if mktcap and fcf and fcf > 0:
        pfcf = mktcap / fcf

    hist_rev, hist_fcf = {}, {}
    try:
        if rev_series is not None:
            for col in rev_series.index:
                hist_rev[str(col)[:4]] = rev_series[col]
    except: pass
    try:
        if fcf_series is not None:
            for col in fcf_series.index:
                hist_fcf[str(col)[:4]] = fcf_series[col]
    except: pass

    points, max_pts = 0, 0
    def add(cond, w=1):
        nonlocal points, max_pts
        max_pts += w
        if cond: points += w

    add((rev_growth or 0) > 10,  2)
    add((fcf_growth or 0) > 10,  2)
    add((roic       or 0) > 15,  2)
    add(fcf_margin        > 10,  1)
    add((debt_fcf   or 99) < 3,  1)
    add(0 < (peg    or 99) < 2,  1)
    add(0 < (pb     or 99) < 5,  1)

    score_10 = round((points / max_pts * 10) if max_pts > 0 else 0, 1)
    if score_10 >= 8.5:   grade = "A"
    elif score_10 >= 7.0: grade = "B"
    elif score_10 >= 5.5: grade = "C"
    elif score_10 >= 4.0: grade = "D"
    else:                 grade = "F"

    return dict(
        rev_growth=rev_growth, fcf_growth=fcf_growth, roic=roic, roe=roe_val,
        fcf_margin=fcf_margin, debt_fcf=debt_fcf,
        pe_ratio=pe_ratio, peg=peg, pb=pb, pfcf=pfcf,
        score_10=score_10, grade=grade,
        hist_rev=hist_rev, hist_fcf=hist_fcf,
        _fin_raw=fin, _bs_raw=bs,
        info=info,
    )


# ============================================================
# PRIX JUSTE HISTORIQUE — 4 MÉTHODES
# ============================================================

@st.cache_data(ttl=3600, show_spinner=False)
def compute_fair_value_history(ticker, period, method, granularity,
                                wacc, g_perp, k_gs, horizon_dcf):
    """
    Calcule une série temporelle du prix juste selon la méthode choisie.
    Retourne DataFrame [Date, Prix_Juste, Prix_Reel].
    """
    import math
    ticker_obj = yf.Ticker(ticker)
    info       = ticker_obj.info
    shares     = (info.get("sharesOutstanding") or
                  info.get("impliedSharesOutstanding") or
                  info.get("floatShares") or 0)
    hist_price = ticker_obj.history(period=period, interval=granularity)
    if hist_price.empty:
        return None
    dates       = hist_price.index.tz_localize(None) if hist_price.index.tz else hist_price.index
    real_price  = hist_price["Close"].values
    fair_values = []

    try:
        fin_annual   = ticker_obj.financials
        bs_annual    = ticker_obj.balance_sheet
        cf_annual    = ticker_obj.cashflow
        fin_quarter  = ticker_obj.quarterly_financials
        bs_quarter   = ticker_obj.quarterly_balance_sheet
        cf_quarter   = ticker_obj.quarterly_cashflow
    except Exception:
        fin_annual = fin_quarter = bs_annual = bs_quarter = cf_annual = cf_quarter = None

    def _extract(df, labels):
        if df is None or df.empty:
            return {}
        for lbl in labels:
            if lbl in df.index:
                s = df.loc[lbl].dropna()
                result = {}
                for c, v in s.items():
                    if v is not None and not (isinstance(v, float) and math.isnan(v)):
                        result[pd.Timestamp(c).normalize()] = float(v)
                return result
        return {}

    def _last_before(sd, date):
        c = {k: v for k, v in sd.items() if k <= date}
        return c[max(c.keys())] if c else None

    FCF_LBL  = ["Free Cash Flow", "FreeCashFlow", "Operating Cash Flow"]
    EPS_LBL  = ["Diluted EPS", "Basic EPS", "EPS"]
    DIV_LBL  = ["Common Stock Dividends", "Dividends Paid", "Cash Dividends Paid"]
    BOOK_LBL = ["Common Stock Equity", "Stockholders Equity", "Total Stockholders Equity",
                "Stockholders Equity Net Minority Interest"]

    fcf_annual_s   = _extract(cf_annual,   FCF_LBL)
    fcf_quarter_s  = _extract(cf_quarter,  FCF_LBL)
    eps_quarter_s  = _extract(fin_quarter, EPS_LBL)
    eps_annual_s   = _extract(fin_annual,  EPS_LBL)
    div_annual_s   = _extract(cf_annual,   DIV_LBL)
    div_quarter_s  = _extract(cf_quarter,  DIV_LBL)
    book_annual_s  = _extract(bs_annual,   BOOK_LBL)
    book_quarter_s = _extract(bs_quarter,  BOOK_LBL)
    pe_hist = float(info.get("trailingPE") or info.get("forwardPE") or 20.0)

    for date, price in zip(dates, real_price):
        fv = None
        try:
            if method == "DCF":
                # Préférer FCF annuel (plus stable) au FCF trimestriel × 4
                fcf_annual_v = _last_before(fcf_annual_s, date)
                if not fcf_annual_v:
                    # Fallback : somme des 4 derniers trimestres
                    fcf_q_vals = sorted([(k, v) for k, v in fcf_quarter_s.items() if k <= date],
                                        key=lambda x: x[0])[-4:]
                    if fcf_q_vals:
                        fcf_annual_v = sum(v for _, v in fcf_q_vals)

                if fcf_annual_v and fcf_annual_v > 0 and shares > 0:
                    # Croissance FCF glissante sur les FCF annuels disponibles
                    fcf_hist = sorted([(k, v) for k, v in fcf_annual_s.items() if k <= date],
                                      key=lambda x: x[0])[-5:]
                    if len(fcf_hist) >= 2:
                        v0, vn = fcf_hist[0][1], fcf_hist[-1][1]
                        n = len(fcf_hist) - 1
                        if v0 > 0 and vn > 0:
                            g_fcf = (vn / v0) ** (1 / n) - 1
                        else:
                            g_fcf = 0.05
                        g_fcf = max(min(g_fcf, 0.25), -0.05)  # clamp -5% / +25%
                    else:
                        g_fcf = 0.05

                    pv = 0.0
                    for t_yr in range(1, horizon_dcf + 1):
                        pv += fcf_annual_v * ((1 + g_fcf) ** t_yr) / ((1 + wacc) ** t_yr)
                    fcf_terminal = fcf_annual_v * ((1 + g_fcf) ** horizon_dcf)
                    tv = fcf_terminal * (1 + g_perp) / max(wacc - g_perp, 0.001)
                    pv += tv / ((1 + wacc) ** horizon_dcf)
                    fv_raw = pv / shares
                    # Sanity check : le prix juste DCF ne doit pas dépasser 20× le cours réel
                    if fv_raw > 0 and fv_raw < price * 20:
                        fv = fv_raw

            elif method == "Multiples (P/E)":
                # Priorité : trailingEps de yfinance (BPA réel sur 12 mois glissants)
                eps_info = float(info.get("trailingEps") or 0)
                if eps_info > 0:
                    eps_ttm = eps_info
                else:
                    # Fallback : somme des 4 derniers trimestres EPS
                    eps_vals = sorted([(k, v) for k, v in eps_quarter_s.items() if k <= date],
                                      key=lambda x: x[0])[-4:]
                    if not eps_vals:
                        eps_vals = sorted([(k, v) for k, v in eps_annual_s.items() if k <= date],
                                          key=lambda x: x[0])[-1:]
                    eps_ttm = sum(v for _, v in eps_vals) if eps_vals else 0

                if eps_ttm > 0:
                    pe_used = max(min(pe_hist, 80), 5)
                    fv_raw = pe_used * eps_ttm
                    # Sanity check : max 15× le cours réel
                    if fv_raw > 0 and fv_raw < price * 15:
                        fv = fv_raw

            elif method == "Gordon-Shapiro (DDM)":
                # Priorité 1 : dividendRate de yfinance = dividende annuel PAR ACTION
                # C'est la source la plus fiable — déjà en unité monétaire par action
                div_rate_info = float(info.get("dividendRate") or
                                      info.get("trailingAnnualDividendRate") or 0)

                if div_rate_info > 0:
                    # Utiliser directement le dividende par action de yfinance
                    d0 = div_rate_info
                    # Croissance g estimée sur les dividendes annuels historiques
                    if len(div_annual_s) >= 2:
                        div_sorted = sorted([(k, abs(v)) for k, v in div_annual_s.items()
                                             if k <= date], key=lambda x: x[0])[-4:]
                        if len(div_sorted) >= 2 and shares > 0:
                            d_old_ps = div_sorted[0][1] / shares
                            d_new_ps = div_sorted[-1][1] / shares
                            n = len(div_sorted) - 1
                            if d_old_ps > 0 and d_new_ps > 0:
                                g_div = (d_new_ps / d_old_ps) ** (1 / n) - 1
                            else:
                                g_div = 0.03
                        else:
                            g_div = 0.03
                    else:
                        g_div = 0.03
                elif div_vals and shares > 0:
                    # Fallback : cashflow dividendes / nombre d'actions
                    div_total = abs(sum(v for _, v in div_vals))
                    d0 = div_total / shares
                    div_old = sorted([(k, v) for k, v in div_annual_s.items()
                                      if k <= date], key=lambda x: x[0])[-3:]
                    if len(div_old) >= 2:
                        d_old = abs(div_old[0][1]) / shares
                        d_new = abs(div_old[-1][1]) / shares
                        if d_old > 0 and d_new > 0:
                            g_div = (d_new / d_old) ** (1 / (len(div_old) - 1)) - 1
                        else:
                            g_div = 0.03
                    else:
                        g_div = 0.03
                else:
                    d0 = 0

                if d0 > 0:
                    # Clamp strict : g_div ne peut pas dépasser k_gs - 1%
                    # ni dépasser 8% (plafond économique raisonnable)
                    g_div = max(min(g_div, k_gs - 0.01, 0.08), 0.0)
                    d1 = d0 * (1 + g_div)
                    spread = k_gs - g_div
                    if spread > 0.005:
                        fv = d1 / spread

            elif method == "ANR (Book Value)":
                book = (_last_before(book_quarter_s, date) or
                        _last_before(book_annual_s,  date))
                if book and shares > 0:
                    fv = book / shares

        except Exception:
            fv = None
        fair_values.append(fv)

    df_out = pd.DataFrame({
        "Date":       dates,
        "Prix_Reel":  real_price,
        "Prix_Juste": fair_values,
    })
    return df_out.dropna(subset=["Prix_Reel"])



def render_scorecard(f):
    def safe(v): return v is not None and not (isinstance(v, float) and np.isnan(v))
    def fmt(v, u="", dec=1): return f"{v:.{dec}f}{u}" if safe(v) else "N/A"

    g = f["grade"]
    sc_col, sp_col = st.columns([1, 4])
    with sc_col:
        st.markdown(
            f'<div class="score-box score-{g}">'
            f'<div class="score-label">{g}</div>'
            f'<div class="score-sub">{f["score_10"]:.1f} / 10</div>'
            f'</div>',
            unsafe_allow_html=True
        )
    with sp_col:
        st.markdown(t("scorecard_global_label"))
        pct = f["score_10"] / 10
        color = "#28a745" if pct >= 0.7 else "#ffc107" if pct >= 0.4 else "#dc3545"
        st.markdown(
            f'<div style="background:#333;border-radius:8px;height:16px;width:100%">'
            f'<div style="background:{color};width:{pct*100:.0f}%;height:16px;border-radius:8px"></div>'
            f'</div>',
            unsafe_allow_html=True
        )

    st.markdown("---")

    CATEGORIES = [
        {
            "title": t("scorecard_cat_growth"),
            "border": "#28a745",
            "metrics": [
                {"t": t("scorecard_metric_rev"),  "v": f["rev_growth"], "tgt": "> 10%", "ok": (f["rev_growth"] or 0) > 10, "u": "%"},
                {"t": t("scorecard_metric_fcf"), "v": f["fcf_growth"], "tgt": "> 10%", "ok": (f["fcf_growth"] or 0) > 10, "u": "%"},
            ],
        },
        {
            "title": "💰 Indicateurs de Rentabilité",
            "border": "#4C9BE8",
            "metrics": [
                {"t": "Super ROIC", "v": f["roic"],       "tgt": "> 15%", "ok": (f["roic"] or 0) > 15, "u": "%"},
                {"t": "ROE",        "v": f["roe"],        "tgt": "> 15%", "ok": (f["roe"]  or 0) > 15, "u": "%", "neutral_if_na": True},
                {"t": "Marge FCF",  "v": f["fcf_margin"], "tgt": "> 10%", "ok": f["fcf_margin"] > 10,   "u": "%"},
            ],
        },
        {
            "title": "⚖️ Indicateurs de Valorisation",
            "border": "#ffc107",
            "metrics": [
                {"t": "P/E Ratio",    "v": f["pe_ratio"], "tgt": "Indicatif", "ok": True,                        "u": "x", "neutral_if_na": True},
                {"t": "PEG Ratio",    "v": f["peg"],      "tgt": "< 2",       "ok": 0 < (f["peg"]  or 99) < 2,  "u": ""},
                {"t": "Price / Book", "v": f["pb"],       "tgt": "< 5",       "ok": 0 < (f["pb"]   or 99) < 5,  "u": "x"},
                {"t": "Price / FCF",  "v": f["pfcf"],     "tgt": "< 25",      "ok": 0 < (f["pfcf"] or 99) < 25, "u": "x"},
            ],
        },
        {
            "title": "🏦 Indicateurs de Solidité Financière",
            "border": "#dc3545",
            "metrics": [
                {"t": "Dette / FCF", "v": f["debt_fcf"], "tgt": "< 3", "ok": (f["debt_fcf"] or 99) < 3, "u": "", "inv": True},
            ],
        },
    ]

    row1_left, row1_right = st.columns(2)
    row2_left, row2_right = st.columns(2)
    cat_containers = [row1_left, row1_right, row2_left, row2_right]

    for cat_idx, (cat, container) in enumerate(zip(CATEGORIES, cat_containers)):
        with container:
            border_color = cat["border"]
            st.markdown(
                f'<p style="font-size:0.9rem;font-weight:700;color:#e0e0e0;'
                f'border-left:4px solid {border_color};padding-left:8px;margin-bottom:6px;">'
                f'{cat["title"]}</p>',
                unsafe_allow_html=True
            )
            n = len(cat["metrics"])
            cols = st.columns(n)
            for i, m in enumerate(cat["metrics"]):
                v   = m["v"]
                ok  = m["ok"]
                na  = not safe(v)
                neutral_if_na = m.get("neutral_if_na", False)
                cls = "status-neu" if (na and neutral_if_na) else ("status-ok" if ok else "status-ko")
                val_str = fmt(v, m["u"]) if safe(v) else "N/A"
                icon = "✅" if ok else "❌"
                if na and neutral_if_na:
                    icon = "➖"
                cols[i].markdown(
                    f'<div class="metric-card {cls}">'
                    f'<div class="metric-title">{icon} {m["t"]}</div>'
                    f'<div class="metric-value">{val_str}</div>'
                    f'<div class="metric-target">{t("scorecard_obj")} {m["tgt"]}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )


def render_company_info(ticker, info):
    """Affiche l'encart Profil / Dividende / Recommandations analysts sous le graphique sigma."""
    import math

    def safe(v):
        return v is not None and not (isinstance(v, float) and math.isnan(v))

    # ── CSS encart ──────────────────────────────────────────
    st.markdown("""
    <style>
    .info-card {
        background: linear-gradient(135deg,#0d1b2a 0%,#1a2a3a 100%);
        border-radius: 12px; padding: 14px 18px; margin-bottom: 8px;
        border-left: 4px solid #4C9BE8;
    }
    .info-card.green  { border-left-color: #28a745; }
    .info-card.orange { border-left-color: #fd7e14; }
    .info-card h5 { color:#aad4f5; font-size:0.82rem; font-weight:700;
                    text-transform:uppercase; letter-spacing:0.04em;
                    margin:0 0 8px 0; }
    .info-line { font-size:0.83rem; color:#ccc; margin-bottom:3px; line-height:1.4; }
    .info-label { color:#888; font-size:0.78rem; }
    .info-value { color:#e0e0e0; font-weight:600; }
    .rec-badge {
        display:inline-block; border-radius:4px; padding:1px 8px;
        font-size:0.75rem; font-weight:700; margin:2px 3px;
    }
    .rec-buy    { background:#1a4a2a; color:#4ade80; }
    .rec-hold   { background:#3a2a00; color:#fbbf24; }
    .rec-sell   { background:#3a0a0a; color:#f87171; }
    .rec-sybuy  { background:#0d2a1a; color:#86efac; }
    .rec-sysell { background:#2a0a0a; color:#fca5a5; }
    </style>
    """, unsafe_allow_html=True)

    col_profil, col_div, col_rec = st.columns([3, 2, 3])

    # ── Profil ──────────────────────────────────────────────
    with col_profil:
        name        = info.get("longName") or info.get("shortName") or ticker
        sector      = info.get("sector", "N/A")
        industry    = info.get("industry", "N/A")
        country     = info.get("country", "N/A")
        employees   = info.get("fullTimeEmployees")
        summary     = info.get("longBusinessSummary", "")
        emp_str     = f"{employees:,}" if employees else "N/A"

        # Résumé tronqué
        if summary and len(summary) > 280:
            summary_short = summary[:280].rsplit(" ", 1)[0] + "…"
        else:
            summary_short = summary or "—"

        st.markdown(
            f'<div class="info-card">'
            f'<h5>{t("company_profile_title")}</h5>'
            f'<div class="info-line"><span class="info-label">{t("company_name_lbl")}</span> <span class="info-value">{name}</span></div>'
            f'<div class="info-line"><span class="info-label">{t("company_sector_lbl")}</span> {sector}</div>'
            f'<div class="info-line"><span class="info-label">{t("company_industry_lbl")}</span> {industry}</div>'
            f'<div class="info-line"><span class="info-label">{t("company_country_lbl")}</span> {country}</div>'
            f'<div class="info-line"><span class="info-label">{t("company_employees_lbl")}</span> {emp_str}</div>'
            f'<div class="info-line" style="margin-top:6px;color:#aaa;font-size:0.78rem;line-height:1.4;">{summary_short}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    # ── Dividende ───────────────────────────────────────────
    with col_div:
        div_rate    = info.get("dividendRate")
        div_yield   = info.get("dividendYield")
        ex_div_date = info.get("exDividendDate")
        pay_date    = None

        # Tentative de récupération de la date de paiement via calendar
        try:
            cal = get_calendar(ticker)
            if cal is not None:
                if isinstance(cal, dict):
                    pay_date = cal.get("Dividend Date") or cal.get("Ex-Dividend Date")
                elif isinstance(cal, pd.DataFrame) and not cal.empty:
                    if "Dividend Date" in cal.index:
                        pay_date = cal.loc["Dividend Date"].iloc[0]
                    elif "Ex-Dividend Date" in cal.index:
                        pay_date = cal.loc["Ex-Dividend Date"].iloc[0]
        except:
            pass

        div_rate_str  = f"{div_rate:.2f} {info.get('currency','')}" if safe(div_rate) else t("company_none")
        # yfinance retourne dividendYield tantôt en décimal (0.0312 = 3.12%)
        # tantôt déjà en pourcentage (3.12 = 3.12%) selon les sources.
        # Si la valeur est > 1 elle est déjà en %, sinon on multiplie par 100.
        if safe(div_yield) and div_yield:
            div_yield_pct = div_yield if div_yield > 1 else div_yield * 100
            div_yield_str = f"{div_yield_pct:.2f}%"
        else:
            div_yield_str = "—"

        # Formatage date ex-dividende
        if ex_div_date:
            try:
                if isinstance(ex_div_date, (int, float)):
                    ex_div_str = datetime.utcfromtimestamp(ex_div_date).strftime("%d/%m/%Y")
                else:
                    ex_div_str = pd.Timestamp(ex_div_date).strftime("%d/%m/%Y")
            except:
                ex_div_str = str(ex_div_date)
        else:
            ex_div_str = "—"

        # Formatage date paiement
        if pay_date:
            try:
                pay_str = pd.Timestamp(pay_date).strftime("%d/%m/%Y")
            except:
                pay_str = str(pay_date)
        else:
            pay_str = "—"

        st.markdown(
            f'<div class="info-card green">'
            f'<h5>{t("company_dividend_title")}</h5>'
            f'<div class="info-line"><span class="info-label">{t("company_annual_div")}</span> <span class="info-value">{div_rate_str}</span></div>'
            f'<div class="info-line"><span class="info-label">{t("company_yield")}</span> <span class="info-value">{div_yield_str}</span></div>'
            f'<div class="info-line"><span class="info-label">{t("company_exdate")}</span> {ex_div_str}</div>'
            f'<div class="info-line"><span class="info-label">{t("company_next_payment")}</span> <span class="info-value">{pay_str}</span></div>'
            f'</div>',
            unsafe_allow_html=True
        )

    # ── Recommandations analystes ────────────────────────────
    with col_rec:
        rec_html = f'<div class="info-card orange"><h5>{t("company_rec_title")}</h5>'
        try:
            rec_df = get_recommendations(ticker)
            if rec_df is not None and not rec_df.empty:
                # Normaliser les colonnes selon la version yfinance
                rec_df = rec_df.copy()
                if "period" in rec_df.columns:
                    # Nouveau format : columns = period, strongBuy, buy, hold, sell, strongSell
                    recent = rec_df.head(4)
                    rows_r = ""
                    for _, row in recent.iterrows():
                        period_label = str(row.get("period", ""))
                        sb  = int(row.get("strongBuy",  0) or 0)
                        b   = int(row.get("buy",        0) or 0)
                        h   = int(row.get("hold",       0) or 0)
                        s   = int(row.get("sell",       0) or 0)
                        ss  = int(row.get("strongSell", 0) or 0)
                        total = sb + b + h + s + ss
                        if total == 0:
                            continue
                        rows_r += (
                            f'<div class="info-line" style="margin-bottom:5px;">'
                            f'<span class="info-label" style="display:inline-block;width:42px;">{period_label}</span>'
                            f'<span class="rec-badge rec-sybuy" title="Strong Buy">SB {sb}</span>'
                            f'<span class="rec-badge rec-buy"   title="Buy">B {b}</span>'
                            f'<span class="rec-badge rec-hold"  title="Hold">H {h}</span>'
                            f'<span class="rec-badge rec-sell"  title="Sell">S {s}</span>'
                            f'<span class="rec-badge rec-sysell" title="Strong Sell">SS {ss}</span>'
                            f'</div>'
                        )
                    rec_html += rows_r if rows_r else '<div class="info-line" style="color:#666;">Aucune donnée récente</div>'
                else:
                    # Ancien format : index=date, columns=Firm, To Grade, From Grade, Action
                    if "To Grade" in rec_df.columns:
                        recent = rec_df.sort_index(ascending=False).head(8)
                        grade_map = {
                            "Buy": "rec-buy", "Strong Buy": "rec-sybuy",
                            "Outperform": "rec-buy", "Overweight": "rec-buy",
                            "Hold": "rec-hold", "Neutral": "rec-hold", "Equal-Weight": "rec-hold",
                            "Market Perform": "rec-hold", "Sector Perform": "rec-hold",
                            "Sell": "rec-sell", "Strong Sell": "rec-sysell",
                            "Underperform": "rec-sell", "Underweight": "rec-sell",
                        }
                        rows_r = ""
                        for idx_r, row_r in recent.iterrows():
                            grade = str(row_r.get("To Grade", ""))
                            firm  = str(row_r.get("Firm", ""))[:20]
                            try:
                                date_r = pd.Timestamp(idx_r).strftime("%m/%Y")
                            except:
                                date_r = str(idx_r)[:7]
                            badge_cls = grade_map.get(grade, "rec-hold")
                            rows_r += (
                                f'<div class="info-line">'
                                f'<span class="info-label" style="display:inline-block;min-width:38px;">{date_r}</span>'
                                f'<span class="rec-badge {badge_cls}">{grade}</span>'
                                f'<span style="color:#888;font-size:0.75rem;margin-left:4px;">{firm}</span>'
                                f'</div>'
                            )
                        rec_html += rows_r if rows_r else '<div class="info-line" style="color:#666;">Aucune donnée</div>'
                    else:
                        rec_html += f'<div class="info-line" style="color:#666;">{t("company_rec_unknown")}</div>'
            else:
                rec_html += f'<div class="info-line" style="color:#666;">{t("company_rec_none")}</div>'
        except Exception as e:
            rec_html += f'<div class="info-line" style="color:#666;">Erreur : {e}</div>'

        rec_html += "</div>"
        st.markdown(rec_html, unsafe_allow_html=True)


def render_historical_charts(f, ticker):
    """
    Affiche les histogrammes historiques pour TOUS les indicateurs,
    organisés par catégorie (Croissance, Rentabilité, Valorisation, Solidité).
    """
    import math

    hist_rev = f.get("hist_rev", {})
    hist_fcf = f.get("hist_fcf", {})
    info     = f.get("info", {})
    # Données financières brutes pour les séries temporelles
    _fin_raw  = f.get("_fin_raw", None)
    _bs_raw   = f.get("_bs_raw", None)

    def safe_v(v):
        return v is not None and not (isinstance(v, float) and math.isnan(v))

    # ── Toutes les années disponibles CA / FCF ──────────────
    years_all = sorted(set(list(hist_rev.keys()) + list(hist_fcf.keys())))

    def to_b(v): return round(v / 1e9, 2) if v is not None else None

    rev_b = [to_b(hist_rev.get(y)) for y in years_all]
    fcf_b = [to_b(hist_fcf.get(y)) for y in years_all]

    # Marge FCF par année
    margins = []
    for y in years_all:
        rv, fv = hist_rev.get(y), hist_fcf.get(y)
        if rv and fv and rv != 0:
            margins.append(round(fv / rv * 100, 1))
        else:
            margins.append(None)

    # ── Séries temporelles pour ROIC / ROE / Dette/FCF / valorisation ──
    def _extract_year_series(df_stmt, labels):
        """Extrait {année: valeur} depuis un DataFrame financier (colonnes = dates)."""
        if df_stmt is None:
            return {}
        for lbl in labels:
            if lbl in df_stmt.index:
                s = df_stmt.loc[lbl].dropna()
                result = {}
                for col, val in s.items():
                    try:
                        y = str(pd.Timestamp(col).year)
                        if val is not None and not (isinstance(val, float) and math.isnan(val)):
                            result[y] = float(val)
                    except:
                        pass
                if result:
                    return result
        return {}

    hist_roic, hist_roe, hist_debt_fcf = {}, {}, {}
    hist_marge_fcf_annual = {}

    if _fin_raw is not None and _bs_raw is not None:
        EBIT_LABELS   = ['EBIT', 'Operating Income', 'Net Income Before Taxes', 'Pretax Income', 'Income Before Tax']
        ASSETS_LABELS = ['Total Assets', 'Total Assets Net', 'Assets']
        LIAB_LABELS   = ['Current Liabilities', 'Current Liabilities Net', 'Total Current Liabilities', 'Payables And Accrued Expenses']
        NI_LABELS     = ['Net Income', 'Net Income Common Stockholders', 'Net Income From Continuing Operations']
        EQ_LABELS     = ['Stockholders Equity', 'Total Stockholders Equity', 'Common Stock Equity',
                         'Total Equity Gross Minority Interest', 'Stockholders Equity Net Minority Interest']
        DEBT_LABELS   = ['Long Term Debt', 'Total Debt', 'Long Term Debt And Capital Lease Obligation',
                         'Total Liabilities Net Minority Interest']

        hist_ebit    = _extract_year_series(_fin_raw,  EBIT_LABELS)
        hist_assets  = _extract_year_series(_bs_raw,   ASSETS_LABELS)
        hist_liab    = _extract_year_series(_bs_raw,   LIAB_LABELS)
        hist_ni      = _extract_year_series(_fin_raw,  NI_LABELS)
        hist_eq      = _extract_year_series(_bs_raw,   EQ_LABELS)
        hist_debt_bs = _extract_year_series(_bs_raw,   DEBT_LABELS)

        all_fin_years = sorted(set(
            list(hist_ebit.keys()) + list(hist_assets.keys()) +
            list(hist_ni.keys()) + list(hist_eq.keys())
        ))
        for y in all_fin_years:
            ebit   = hist_ebit.get(y)
            assets = hist_assets.get(y)
            liab   = hist_liab.get(y, 0)
            ni     = hist_ni.get(y)
            eq     = hist_eq.get(y)
            debt   = hist_debt_bs.get(y, 0)
            fcf_y  = f.get("hist_fcf", {}).get(y)

            if ebit is not None and assets is not None:
                ic = assets - liab
                if ic > 0:
                    hist_roic[y] = round((ebit / ic) * 100, 1)

            if ni is not None and eq is not None and eq != 0:
                hist_roe[y] = round((ni / abs(eq)) * 100, 1)

            if debt and fcf_y and fcf_y != 0:
                hist_debt_fcf[y] = round(debt / fcf_y, 2)

    # ── Helpers pour mini-histogrammes mono-valeur ────────────
    def _bar_single(title, value, unit, target_line=None, target_label="", color_fn=None, height=260):
        """Graphique à barre unique pour une valeur scalaire (pas de série temporelle)."""
        if not safe_v(value):
            fig = go.Figure()
            fig.update_layout(
                title=dict(text=title, font_size=12),
                template="plotly_dark", height=height,
                margin=dict(l=30, r=10, t=40, b=30),
                annotations=[dict(text="N/A", x=0.5, y=0.5, xref="paper", yref="paper",
                                  showarrow=False, font_size=22, font_color="#555")]
            )
            return fig
        color = color_fn(value) if color_fn else "#4C9BE8"
        fig = go.Figure(go.Bar(
            x=["Actuel"], y=[round(value, 1)],
            marker_color=color,
            text=[f"{value:.1f}{unit}"], textposition="outside",
            width=0.35,
        ))
        if target_line is not None:
            fig.add_hline(y=target_line, line_dash="dot", line_color="#FFD700", opacity=0.7,
                          annotation_text=target_label, annotation_font_color="#FFD700",
                          annotation_font_size=9, annotation_position="top right")
        fig.update_layout(
            title=dict(text=title, font_size=12),
            template="plotly_dark", height=height,
            margin=dict(l=30, r=10, t=40, b=30),
            yaxis=dict(title=unit, showgrid=True, gridcolor="#333"),
            xaxis=dict(type="category"), showlegend=False,
        )
        return fig

    def _bar_series(title, years, values, unit, color_fn=None, target_line=None, target_label="",
                    overlay_values=None, overlay_name="", overlay_color="#4CE87A", height=260):
        """Histogramme temporel (série par années)."""
        valid_pairs = [(y, v) for y, v in zip(years, values) if v is not None]
        if not valid_pairs:
            fig = go.Figure()
            fig.update_layout(
                title=dict(text=title, font_size=12), template="plotly_dark", height=height,
                margin=dict(l=30, r=10, t=40, b=30),
                annotations=[dict(text="N/A", x=0.5, y=0.5, xref="paper", yref="paper",
                                  showarrow=False, font_size=22, font_color="#555")]
            )
            return fig
        ys = [p[0] for p in valid_pairs]
        vs = [p[1] for p in valid_pairs]
        colors = [color_fn(v) for v in vs] if color_fn else ["#4C9BE8"] * len(vs)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name=title.split("—")[0].strip(), x=ys, y=vs,
            marker_color=colors,
            text=[f"{v:.1f}{unit}" for v in vs], textposition="outside",
        ))
        if overlay_values is not None:
            ov_pairs = [(y, v) for y, v in zip(years, overlay_values) if v is not None]
            if ov_pairs:
                fig.add_trace(go.Bar(
                    name=overlay_name, x=[p[0] for p in ov_pairs], y=[p[1] for p in ov_pairs],
                    marker_color=overlay_color,
                    text=[f"{v:.1f}{unit}" for v in [p[1] for p in ov_pairs]], textposition="outside",
                ))
        if target_line is not None:
            fig.add_hline(y=target_line, line_dash="dot", line_color="#FFD700", opacity=0.7,
                          annotation_text=target_label, annotation_font_color="#FFD700",
                          annotation_font_size=9, annotation_position="top right")
        fig.update_layout(
            title=dict(text=title, font_size=12),
            template="plotly_dark", height=height,
            barmode="group",
            margin=dict(l=30, r=10, t=40, b=30),
            legend=dict(orientation="h", y=-0.25, font_size=10),
            yaxis=dict(title=unit, showgrid=True, gridcolor="#333"),
            xaxis=dict(type="category"),
        )
        return fig

    # ── Couleurs par seuil ────────────────────────────────────
    def col_pct_pos(v):  return "#28a745" if v > 10 else "#ffc107" if v > 0 else "#dc3545"
    def col_roic(v):     return "#28a745" if v > 15 else "#ffc107" if v > 8  else "#dc3545"
    def col_marge(v):    return "#28a745" if v > 10 else "#ffc107" if v > 5  else "#dc3545"
    def col_pe(v):       return "#4C9BE8"
    def col_peg(v):      return "#28a745" if 0 < v < 2 else "#ffc107" if v <= 3 else "#dc3545"
    def col_pb(v):       return "#28a745" if 0 < v < 5 else "#ffc107" if v <= 10 else "#dc3545"
    def col_pfcf(v):     return "#28a745" if 0 < v < 25 else "#ffc107" if v <= 40 else "#dc3545"
    def col_debt(v):     return "#28a745" if v < 3  else "#ffc107" if v < 5  else "#dc3545"

    # ═══════════════════════════════════════════════════════════
    # SECTION 1 — CROISSANCE
    # ═══════════════════════════════════════════════════════════
    with st.expander(t("hist_growth_expander"), expanded=True):
        g_col1, g_col2 = st.columns(2)

        with g_col1:
            fig_rev = _bar_series(
                t("chart_revenue_name"),
                years_all, rev_b, "Mrd",
                color_fn=lambda v: "#4C9BE8",
                overlay_values=fcf_b, overlay_name="Free Cash Flow (Mrd)",
                overlay_color="#4CE87A", height=280
            )
            # Ajouter la courbe de tendance CA
            valid_rev = [(y, v) for y, v in zip(years_all, rev_b) if v is not None]
            if len(valid_rev) >= 2:
                fig_rev.add_trace(go.Scatter(
                    name=t("chart_trend_ca"), x=[r[0] for r in valid_rev], y=[r[1] for r in valid_rev],
                    mode="lines+markers", line=dict(color="#FFD700", width=2, dash="dot"),
                ))
            st.plotly_chart(fig_rev, use_container_width=True)

        with g_col2:
            # Croissance CA YoY
            cagr_rev_yoy = []
            for i in range(len(years_all)):
                if i == 0:
                    cagr_rev_yoy.append(None)
                else:
                    v0 = hist_rev.get(years_all[i-1])
                    v1 = hist_rev.get(years_all[i])
                    if v0 and v1 and v0 != 0:
                        cagr_rev_yoy.append(round((v1/v0 - 1)*100, 1))
                    else:
                        cagr_rev_yoy.append(None)
            # Croissance FCF YoY
            cagr_fcf_yoy = []
            for i in range(len(years_all)):
                if i == 0:
                    cagr_fcf_yoy.append(None)
                else:
                    v0 = hist_fcf.get(years_all[i-1])
                    v1 = hist_fcf.get(years_all[i])
                    if v0 and v1 and v0 != 0:
                        cagr_fcf_yoy.append(round((v1/v0 - 1)*100, 1))
                    else:
                        cagr_fcf_yoy.append(None)

            fig_growth = _bar_series(
                "📈 Croissance annuelle CA & FCF",
                years_all, cagr_rev_yoy, "%",
                color_fn=col_pct_pos,
                target_line=10, target_label="Obj 10%",
                overlay_values=cagr_fcf_yoy, overlay_name="Croiss. FCF %",
                overlay_color="#4CE87A", height=280
            )
            st.plotly_chart(fig_growth, use_container_width=True)

    # ═══════════════════════════════════════════════════════════
    # SECTION 2 — RENTABILITÉ
    # ═══════════════════════════════════════════════════════════
    with st.expander(t("hist_profit_expander"), expanded=True):
        r_col1, r_col2, r_col3 = st.columns(3)

        with r_col1:
            if hist_roic:
                yr = sorted(hist_roic.keys())
                fig_roic = _bar_series(
                    "🔁 ROIC par année",
                    yr, [hist_roic[y] for y in yr], "%",
                    color_fn=col_roic,
                    target_line=15, target_label="Obj 15%", height=260
                )
            else:
                fig_roic = _bar_single(
                    "🔁 ROIC", f.get("roic"), "%",
                    target_line=15, target_label="Obj 15%", color_fn=col_roic, height=260
                )
            st.plotly_chart(fig_roic, use_container_width=True)

        with r_col2:
            if hist_roe:
                yr = sorted(hist_roe.keys())
                fig_roe = _bar_series(
                    "💹 ROE par année",
                    yr, [hist_roe[y] for y in yr], "%",
                    color_fn=col_roic,
                    target_line=15, target_label="Obj 15%", height=260
                )
            else:
                fig_roe = _bar_single(
                    "💹 ROE", f.get("roe"), "%",
                    target_line=15, target_label="Obj 15%", color_fn=col_roic, height=260
                )
            st.plotly_chart(fig_roe, use_container_width=True)

        with r_col3:
            fig_marge = _bar_series(
                "📐 Marge FCF",
                years_all, margins, "%",
                color_fn=col_marge,
                target_line=10, target_label="Obj 10%", height=260
            )
            st.plotly_chart(fig_marge, use_container_width=True)

    # ═══════════════════════════════════════════════════════════
    # SECTION 3 — VALORISATION
    # ═══════════════════════════════════════════════════════════
    with st.expander(t("hist_valuation_expander"), expanded=True):
        v_col1, v_col2, v_col3, v_col4 = st.columns(4)

        with v_col1:
            fig_pe = _bar_single(
                "📊 P/E Ratio", f.get("pe_ratio"), "x",
                color_fn=col_pe, height=240
            )
            st.plotly_chart(fig_pe, use_container_width=True)

        with v_col2:
            fig_peg = _bar_single(
                "📊 PEG Ratio", f.get("peg"), "",
                target_line=2, target_label="Max 2", color_fn=col_peg, height=240
            )
            st.plotly_chart(fig_peg, use_container_width=True)

        with v_col3:
            fig_pb = _bar_single(
                "📚 Price/Book", f.get("pb"), "x",
                target_line=5, target_label="Max 5", color_fn=col_pb, height=240
            )
            st.plotly_chart(fig_pb, use_container_width=True)

        with v_col4:
            fig_pfcf = _bar_single(
                "💧 Price/FCF", f.get("pfcf"), "x",
                target_line=25, target_label="Max 25", color_fn=col_pfcf, height=240
            )
            st.plotly_chart(fig_pfcf, use_container_width=True)

    # ═══════════════════════════════════════════════════════════
    # SECTION 4 — SOLIDITÉ FINANCIÈRE
    # ═══════════════════════════════════════════════════════════
    with st.expander(t("hist_solidity_expander"), expanded=True):
        s_col1, s_col2 = st.columns([1, 3])

        with s_col1:
            if hist_debt_fcf:
                yr_d = sorted(hist_debt_fcf.keys())
                fig_debt = _bar_series(
                    "🏦 Dette / FCF par année",
                    yr_d, [hist_debt_fcf[y] for y in yr_d], "",
                    color_fn=col_debt,
                    target_line=3, target_label="Max 3", height=260
                )
            else:
                fig_debt = _bar_single(
                    "🏦 Dette / FCF", f.get("debt_fcf"), "",
                    target_line=3, target_label="Max 3", color_fn=col_debt, height=260
                )
            st.plotly_chart(fig_debt, use_container_width=True)

        with s_col2:
            # Waterfall CA → FCF → Marge pour la dernière année disponible
            last_year = years_all[-1] if years_all else None
            lrev = hist_rev.get(last_year) if last_year else None
            lfcf = hist_fcf.get(last_year) if last_year else None
            if lrev and lfcf and to_b(lrev) is not None:
                capex_approx = to_b(lrev - lfcf) if lfcf < lrev else None
                wf_x = ["CA", "FCF", "Marge FCF %"]
                wf_y = [to_b(lrev), to_b(lfcf), round(lfcf/lrev*100, 1) if lrev else None]
                wf_colors = ["#4C9BE8", "#4CE87A", col_marge(round(lfcf/lrev*100, 1)) if lrev else "#aaa"]
                wf_units  = ["Mrd", "Mrd", "%"]
                fig_wf = go.Figure()
                for xi, (label, val, col, unit) in enumerate(zip(wf_x, wf_y, wf_colors, wf_units)):
                    if val is not None:
                        fig_wf.add_trace(go.Bar(
                            name=label, x=[label], y=[val],
                            marker_color=col,
                            text=[f"{val:.1f}{unit}"], textposition="outside",
                            width=0.4,
                        ))
                fig_wf.update_layout(
                    title=dict(text=f"📋 Synthèse CA→FCF→Marge ({last_year})", font_size=12),
                    template="plotly_dark", height=260,
                    margin=dict(l=30, r=10, t=40, b=30),
                    yaxis=dict(showgrid=True, gridcolor="#333"),
                    xaxis=dict(type="category"), showlegend=False, barmode="group",
                )
                st.plotly_chart(fig_wf, use_container_width=True)
            else:
                st.caption(t("hist_no_synthesis"))


# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================
all_data = load_all_indices()
_df_list = load_indices_list()
if _df_list is not None and not _df_list.empty:
    all_data["Indice"] = _df_list

# Enrichir all_data avec les watchlists comme pseudo-indices
all_data_extended = get_all_data_with_watchlists(all_data)


# ============================================================
# INITIALISATION SESSION STATE
# ============================================================
if "lang" not in st.session_state:
    st.session_state.lang = "fr"

if "active_watchlist" not in st.session_state:
    st.session_state.active_watchlist = load_wl_index()[0]

# ── Mettre à jour last_seen + enregistrer session ──────────────
if not st.session_state.get("_session_touched"):
    touch_user_session()
    record_session_start()
    st.session_state["_session_touched"] = True

if "wl_pending_action" not in st.session_state:
    st.session_state.wl_pending_action = None

if "import_step" not in st.session_state:
    st.session_state.import_step = 1

if "import_parsed" not in st.session_state:
    st.session_state.import_parsed = None

if "import_last_file_id" not in st.session_state:
    st.session_state.import_last_file_id = None

if "page" not in st.session_state:
    st.session_state.page = t("page_presentation")

if "individuel_result" not in st.session_state:
    st.session_state.individuel_result = None

if "individuel_prefill" not in st.session_state:
    st.session_state.individuel_prefill = ""

if "comparaison_result" not in st.session_state:
    st.session_state.comparaison_result = None

if "sigma_result" not in st.session_state:
    st.session_state.sigma_result = None

if "screener_result" not in st.session_state:
    st.session_state.screener_result = None

if "chart_yaxis_type" not in st.session_state:
    st.session_state.chart_yaxis_type = "linear"

if "chart_display_mode" not in st.session_state:
    st.session_state.chart_display_mode = "cours"


# ============================================================
# SIDEBAR — NAVIGATION
# ============================================================
with st.sidebar:
    st.markdown(f"## {t('sidebar_title')}")
    st.markdown("---")

    # ── Sélecteur de langue ──────────────────────────────────
    _lang_opts = ["fr", "en", "tr"]
    _lang_choice = st.radio(
        t("lang_selector_label"),
        options=_lang_opts,
        format_func=lambda x: t(f"lang_{x}"),
        index=_lang_opts.index(st.session_state.get("lang", "fr")),
        horizontal=True,
        key="lang_radio",
        label_visibility="collapsed",
    )
    if _lang_choice != st.session_state.get("lang", "fr"):
        st.session_state.lang = _lang_choice
        st.rerun()

    st.markdown("---")
    st.markdown(t("sidebar_nav"))

    pages = [
        t("page_presentation"),
        t("page_analyse"),
        t("page_watchlists"),
        t("page_comparaison"),
        t("page_screener_sigma"),
        t("page_screener_multi"),
        t("page_explications"),
        t("page_configuration"),
    ]

    for p in pages:
        if st.button(p, key=f"nav_{p}", use_container_width=True):
            st.session_state.page = p

    st.markdown("---")

    if all_data:
        st.markdown(t("sidebar_indices"))
        for key, df_idx in all_data.items():
            n = len(df_idx)
            label = get_label(key)
            st.markdown(f"• {label}: **{n}** {t('sidebar_valeurs')}")
    else:
        st.caption(t("sidebar_no_index"))

    st.markdown("---")
    if st.button(t("sidebar_stop"), use_container_width=True, key="btn_stop",
                 help=t("sidebar_stop_help")):
        st.warning(t("sidebar_stopping"))
        import os as _os
        _os._exit(0)


# ============================================================
# PAGE COURANTE
# ============================================================
current_page = st.session_state.page


# ============================================================
# PAGE 0 — PRÉSENTATION
# ============================================================
if current_page == t("page_presentation"):

    # ── Hero Banner ──────────────────────────────────────────
    st.markdown(
        '<div class="pres-hero">'
        '<div class="feat-icon" style="font-size:3.5rem;">📡</div>'
        '<h1>SigmaScope</h1>'
        f'<p class="tagline">{t("hero_tagline")}<br>{t("hero_tagline2")}</p>'
        f'<span class="version-badge">{t("hero_version")}</span>'
        '<div style="margin-top:14px;padding-top:12px;border-top:1px solid rgba(76,155,232,0.25);">'
        f'<span style="font-size:0.82rem;color:#7ad4f5;">{t("hero_pp_compat")} </span>'
        '<a href="https://www.portfolio-performance.info/en/" target="_blank" '
        'style="color:#FFD700;font-weight:700;font-size:0.85rem;text-decoration:none;">'  
        'Portfolio Performance'
        '</a>'
        f'<span style="font-size:0.82rem;color:#7ad4f5;"> {t("hero_pp_desc_inline")}</span>'
        '</div>'
        '</div>',
        unsafe_allow_html=True
    )

    # ── Stats rapides (dynamiques) ───────────────────────────
    _nb_modules      = len(pages) - 1                                   # hors "Présentation"
    _nb_indices      = len(all_data)                                     # indices réellement chargés
    _nb_tickers      = sum(len(df) for df in all_data.values())          # total valeurs disponibles
    _nb_tickers_str  = f"{_nb_tickers:,}".replace(",", " ") if _nb_tickers > 0 else "∞"
    _nb_indices_lbl  = ", ".join(
        get_label_extended(k) for k in list(all_data.keys())[:3]
    ) + ("…" if _nb_indices > 3 else "") if _nb_indices > 0 else t("stat_none_loaded")
    _nb_val_methods  = len(["DCF", "Gordon-Shapiro", "Multiples P/E", "ANR", "Historique"])
    _nb_sigma_zones  = len(SIGMA_CRITERIA)

    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    stats = [
        (str(_nb_modules),      t("stat_modules")),
        (str(_nb_indices),      t("stat_indices_loaded", lbl=_nb_indices_lbl)),
        (_nb_tickers_str + "+",  t("stat_tickers")),
        (str(_nb_val_methods),  t("stat_methods")),
        (str(_nb_sigma_zones),  t("stat_sigma_zones")),
    ]
    for col, (num, lbl) in zip([sc1, sc2, sc3, sc4, sc5], stats):
        col.markdown(
            f'<div class="stat-badge"><div class="stat-num">{num}</div>'
            f'<div class="stat-lbl">{lbl}</div></div>',
            unsafe_allow_html=True
        )

    st.markdown("---")
    st.markdown(t("feat_title"))

    # ── Ligne 1 ──────────────────────────────────────────────
    f1, f2, f3 = st.columns(3)

    with f1:
        st.markdown(
            '<div class="feat-card">'
            '<div class="feat-icon">📈</div>'
            f'<div class="feat-title">{t("feat1_title")}</div>'
            f'<div class="feat-desc">{t("feat1_desc")}'
            '</div>'
            f'<span class="feat-tag">{t("feat1_tag1")}</span>'
            f'<span class="feat-tag">{t("feat1_tag2")}</span>'
            f'<span class="feat-tag">{t("feat1_tag3")}</span>'
            f'<span class="feat-tag">{t("feat1_tag4")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_analyse"), key="btn_feat1", use_container_width=True):
            st.session_state.page = t("page_analyse")
            st.rerun()

    with f2:
        st.markdown(
            '<div class="feat-card green">'
            '<div class="feat-icon">⭐</div>'
            f'<div class="feat-title">{t("feat2_title")}</div>'
            f'<div class="feat-desc">{t("feat2_desc")}'
            '</div>'
            f'<span class="feat-tag g">{t("feat2_tag1")}</span>'
            f'<span class="feat-tag g">{t("feat2_tag2")}</span>'
            f'<span class="feat-tag g">{t("feat2_tag3")}</span>'
            f'<span class="feat-tag g">{t("feat2_tag4")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_watchlist"), key="btn_feat2", use_container_width=True):
            st.session_state.page = t("page_watchlists")
            st.rerun()

    with f3:
        st.markdown(
            '<div class="feat-card gold">'
            '<div class="feat-icon">🔀</div>'
            f'<div class="feat-title">{t("feat3_title")}</div>'
            f'<div class="feat-desc">{t("feat3_desc")}'
            '</div>'
            f'<span class="feat-tag y">{t("feat3_tag1")}</span>'
            f'<span class="feat-tag y">{t("feat3_tag2")}</span>'
            f'<span class="feat-tag y">{t("feat3_tag3")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_comparaison"), key="btn_feat3", use_container_width=True):
            st.session_state.page = t("page_comparaison")
            st.rerun()

    # ── Ligne 2 ──────────────────────────────────────────────
    f4, f5, f6 = st.columns(3)

    with f4:
        st.markdown(
            '<div class="feat-card purple">'
            '<div class="feat-icon">🔭</div>'
            f'<div class="feat-title">{t("feat4_title")}</div>'
            f'<div class="feat-desc">{t("feat4_desc")}'
            '</div>'
            f'<span class="feat-tag p">{t("feat4_tag1")}</span>'
            f'<span class="feat-tag p">{t("feat4_tag2")}</span>'
            f'<span class="feat-tag p">{t("feat4_tag3")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_screener_s"), key="btn_feat4", use_container_width=True):
            st.session_state.page = t("page_screener_sigma")
            st.rerun()

    with f5:
        st.markdown(
            '<div class="feat-card teal">'
            '<div class="feat-icon">🎛️</div>'
            f'<div class="feat-title">{t("feat5_title")}</div>'
            f'<div class="feat-desc">{t("feat5_desc")}'
            '</div>'
            f'<span class="feat-tag" style="color:#20c997;border-color:#0a3a2a;background:#041a10;">{t("feat5_tag1")}</span>'
            f'<span class="feat-tag" style="color:#20c997;border-color:#0a3a2a;background:#041a10;">{t("feat5_tag2")}</span>'
            f'<span class="feat-tag" style="color:#20c997;border-color:#0a3a2a;background:#041a10;">{t("feat5_tag3")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_screener_m"), key="btn_feat5", use_container_width=True):
            st.session_state.page = t("page_screener_multi")
            st.rerun()

    with f6:
        st.markdown(
            '<div class="feat-card orange">'
            '<div class="feat-icon">💡</div>'
            f'<div class="feat-title">{t("feat6_title")}</div>'
            f'<div class="feat-desc">{t("feat6_desc")}'
            '</div>'
            f'<span class="feat-tag" style="color:#fd7e14;border-color:#3a2000;background:#1a0e00;">{t("feat6_tag1")}</span>'
            f'<span class="feat-tag" style="color:#fd7e14;border-color:#3a2000;background:#1a0e00;">{t("feat6_tag2")}</span>'
            f'<span class="feat-tag" style="color:#fd7e14;border-color:#3a2000;background:#1a0e00;">{t("feat6_tag3")}</span>'
            f'<span class="feat-tag" style="color:#fd7e14;border-color:#3a2000;background:#1a0e00;">{t("feat6_tag4")}</span>'
            '</div>',
            unsafe_allow_html=True
        )
        if st.button(t("feat_open_valorisation"), key="btn_feat6", use_container_width=True):
            st.session_state.page = t("page_analyse")
            st.rerun()

    st.markdown("---")

    # ── Guide de démarrage rapide + Principe sigma ───────────
    col_qs, col_sigma = st.columns([1, 1])

    with col_qs:
        st.markdown(t("quickstart_title"))
        steps = [
            ("1", t("qs_step1")),
            ("2", t("qs_step2")),
            ("3", t("qs_step3")),
            ("4", t("qs_step4")),
            ("5", t("qs_step5")),
        ]
        for num, text in steps:
            st.markdown(
                f'<div class="quickstart-step">'
                f'<span class="qs-num">{num}</span>'
                f'<span class="qs-text">{text}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    with col_sigma:
        st.markdown(t("sigma_zones_title"))
        sigma_zones = [
            ("📉📉", t("sigma_zone1_label"), "#3CB371", t("sigma_zone1_desc")),
            ("📉",   t("sigma_zone2_label"), "#90EE90", t("sigma_zone2_desc")),
            ("〰️",   t("sigma_zone3_label"), "#ffc107", t("sigma_zone3_desc")),
            ("🚀",   t("sigma_zone4_label"), "#FFA07A", t("sigma_zone4_desc")),
            ("📈📈", t("sigma_zone5_label"), "#FF4C4C", t("sigma_zone5_desc")),
        ]
        for icon, label, color, desc in sigma_zones:
            st.markdown(
                f'<div style="background:#0d1b2a;border-radius:7px;padding:7px 12px;'
                f'margin-bottom:6px;border-left:3px solid {color};">'
                f'<span style="font-size:1.1rem;">{icon}</span> '
                f'<strong style="color:{color};font-size:0.85rem;">{label}</strong><br>'
                f'<span style="color:#9ab;font-size:0.78rem;">{desc}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    st.markdown("---")

    # ── Expander 0 : Roadmap / Reste à faire ─────────────────────
    with st.expander(t("roadmap_expander"), expanded=False):
        st.markdown(t("roadmap_title"))

        todo_items = [
            ("🔴", t("roadmap_item1_title"), t("roadmap_item1_desc")),
            ("🟢", t("roadmap_item2_title"), t("roadmap_item2_desc")),
            ("🟢", t("roadmap_item3_title"), t("roadmap_item3_desc")),
            ("🟢", t("roadmap_item4_title"), t("roadmap_item4_desc")),
            ("🟢", t("roadmap_item5_title"), t("roadmap_item5_desc")),
            ("🟢", t("roadmap_item6_title"), t("roadmap_item6_desc")),
        ]

        legend_html = (
            '<div style="display:flex;gap:18px;margin-bottom:14px;font-size:0.78rem;color:#9ab;">'
            f'<span>{t("roadmap_high")}</span>'
            f'<span>{t("roadmap_study")}</span>'
            '</div>'
        )
        st.markdown(legend_html, unsafe_allow_html=True)

        for dot, title, desc in todo_items:
            border = "#dc3545" if dot == "🔴" else "#4C9BE8"
            st.markdown(
                f'<div style="background:#0d1b2a;border-radius:8px;padding:10px 16px;'
                f'margin-bottom:8px;border-left:3px solid {border};">'
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">'
                f'<span style="font-size:1rem;">{dot}</span>'
                f'<strong style="color:#e0e0e0;font-size:0.9rem;">{title}</strong>'
                f'</div>'
                f'<span style="color:#9ab;font-size:0.82rem;line-height:1.5;">{desc}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

        st.caption(t("roadmap_caption"))

    # ── Expander 1 : Vie & statistiques d'utilisation ────────────
    with st.expander(t("stats_expander"), expanded=False):
        stats_data   = get_usage_stats()
        ratings_data = get_ratings_stats()

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        metrics = [
            ("👥", str(stats_data["distinct_users"]),  t("stats_users")),
            ("🔄", str(stats_data["nb_sessions"]),     t("stats_sessions")),
            ("⏱️", f"{stats_data['avg_duration']} {t('min_suffix')}", t("stats_duration")),
            ("⭐", f"{ratings_data['avg_rating']}/5",  f"{ratings_data['nb_ratings']} {t('stats_rating')}"),
            ("🚀", f"{ratings_data['nb_saas_yes']} {t('stats_save_oui')} / {ratings_data['nb_saas_no']} {t('stats_save_non')}",
                   t("stats_saas_vote")),
        ]
        for col, (icon, val, lbl) in zip([sc1, sc2, sc3, sc4, sc5], metrics):
            col.markdown(
                f'<div class="stat-badge">'
                f'<div style="font-size:1.4rem;">{icon}</div>'
                f'<div class="stat-num" style="font-size:1.3rem;">{val}</div>'
                f'<div class="stat-lbl">{lbl}</div>'
                f'</div>',
                unsafe_allow_html=True
            )

        st.markdown("---")
        st.markdown(t("stats_rating_section"))
        existing = get_user_rating()
        c_note, c_saas = st.columns(2)

        with c_note:
            current_rating = existing["rating"] if existing else 3
            new_rating = st.radio(
                t("stats_rating_label"),
                options=[1, 2, 3, 4, 5],
                index=current_rating - 1,
                horizontal=True,
                format_func=lambda x: "⭐" * x,
                key="pres_rating"
            )

        with c_saas:
            current_vote = existing["vote_saas"] if existing else None
            vote_options = [
                t("stats_saas_yes_opt"),
                t("stats_saas_no_opt")
            ]
            vote_idx = 0 if current_vote is True else 1 if current_vote is False else 0
            new_vote_str = st.radio(
                t("stats_saas_label"),
                options=vote_options,
                index=vote_idx,
                key="pres_saas_vote"
            )
            new_vote = (new_vote_str == vote_options[0])

        if st.button(t("stats_save_btn"), key="pres_save_rating", type="primary"):
            ok = save_user_rating(new_rating, new_vote)
            if ok:
                st.success(t("stats_save_ok", stars="⭐" * new_rating, vote=t("stats_save_oui") if new_vote else t("stats_save_non")))
                st.rerun()
            else:
                st.error(t("stats_save_err"))

    # ── Expander 2 : Suggestions d'amélioration ──────────────────
    with st.expander(t("feedback_expander"), expanded=False):
        st.markdown(t("feedback_title"))

        # Choix public / privé
        fb_visibility = st.radio(
            t("feedback_visibility"),
            options=[t("feedback_public"), t("feedback_private")],
            horizontal=True,
            key="fb_visibility",
        )
        _is_private = fb_visibility == t("feedback_private")

        if _is_private:
            st.caption(t("feedback_caption_private"))
        else:
            st.caption(t("feedback_caption_public"))

        with st.form("form_feedback", clear_on_submit=True):
            msg = st.text_area(
                "Votre suggestion",
                placeholder=t("feedback_placeholder"),
                max_chars=500,
                height=100,
                label_visibility="collapsed"
            )
            submitted = st.form_submit_button(t("feedback_send_btn"), type="primary")
            if submitted:
                if msg.strip():
                    ok = save_feedback(msg, is_private=_is_private)
                    if ok:
                        if _is_private:
                            st.success(t("feedback_ok_private"))
                        else:
                            st.success(t("feedback_ok_public"))
                    else:
                        st.error(t("feedback_err"))
                else:
                    st.warning(t("feedback_empty"))

        st.markdown("---")
        st.markdown(t("feedback_community_title"))
        messages = get_feedback_messages(limit=30, include_private=False)
        if messages:
            for m in messages:
                try:
                    dt = datetime.fromisoformat(m["created_at"].replace("Z", "+00:00"))
                    date_str = dt.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    date_str = ""
                st.markdown(
                    f'<div style="background:#0d1b2a;border-radius:8px;padding:10px 14px;'
                    f'margin-bottom:6px;border-left:3px solid #4C9BE8;">'
                    f'<span style="color:#ddd;font-size:0.88rem;">{m["message"]}</span><br>'
                    f'<span style="color:#556;font-size:0.72rem;">📅 {date_str}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.caption(t("feedback_none"))

    st.markdown(
        '<div style="text-align:center;color:#556;font-size:0.78rem;padding:8px 0;">'
        '⚡ SigmaScope utilise <strong style="color:#4C9BE8;">Yahoo Finance</strong> comme source de données · '
        'Conçu pour l\'analyse quantitative · '
        '<em>Pas un conseil en investissement</em>'
        '</div>',
        unsafe_allow_html=True
    )


# ============================================================
# PAGE 1 — Analyse Individuelle & Scorecard
# ============================================================
if current_page == t("page_analyse"):
    st.title(t("analyse_title"))

    _prefill = st.session_state.get("individuel_prefill", "")

    # ── Catalogue autocomplete (tous tickers+noms de tous les indices) ──
    _ac_catalog = {}  # ticker_upper -> "TICKER — Company"
    for _k, _df_idx in all_data.items():
        for _, _row in _df_idx.iterrows():
            _t = str(_row.get("Ticker", "")).strip()
            _c = str(_row.get("Company", "")).strip()
            if _t:
                _ac_catalog[_t.upper()] = f"{_t} — {_c}" if _c else _t
    _ac_options = [""] + list(_ac_catalog.values())

    with st.expander(t("analyse_params_expander"), expanded=True):
        col_a, col_b = st.columns([2, 3])

        with col_a:
            if _ac_catalog:
                _prefill_val = ""
                if _prefill and _prefill.strip().upper() in _ac_catalog:
                    _prefill_val = _ac_catalog[_prefill.strip().upper()]

                _ac_key = f"autocomplete_ticker_{st.session_state.get('_ac_reset_count', 0)}"

                def _on_autocomplete_change():
                    st.session_state["_ticker_source"] = "autocomplete"

                _ac_selected = st.selectbox(
                    t("analyse_ticker_label"),
                    options=_ac_options,
                    index=_ac_options.index(_prefill_val) if _prefill_val in _ac_options else 0,
                    key=_ac_key,
                    placeholder=t("analyse_ticker_placeholder"),
                    help=t("analyse_ticker_help"),
                    on_change=_on_autocomplete_change,
                )
                ticker_manual = _ac_selected.split(" — ")[0].strip() if _ac_selected else ""
            else:
                ticker_manual = st.text_input(
                    t("analyse_ticker_manual"),
                    value=_prefill,
                    placeholder=t("analyse_ticker_manual_ph")
                )

        with col_b:
            ticker_from_index = None
            all_index_options  = [t("analyse_indice_none")]
            all_index_keys_map = [None]
            for k in all_data_extended:
                all_index_options.append(get_label_extended(k))
                all_index_keys_map.append(k)

            # Détermine la clé de l'indice — besoin avant d'afficher les composants
            selected_comp_label_tmp = st.session_state.get("sb_composants", "-- Aucun --")
            selected_comp_key_tmp   = all_index_keys_map[
                all_index_options.index(selected_comp_label_tmp)
                if selected_comp_label_tmp in all_index_options else 0
            ]

            if selected_comp_key_tmp is not None:
                df_components_tmp = all_data_extended.get(selected_comp_key_tmp)
                if df_components_tmp is not None and not df_components_tmp.empty:
                    # Pour les watchlists, le champ Company peut être vide → enrichi via yfinance (caché)
                    _is_wl_source = is_wl_key(selected_comp_key_tmp)
                    options_list = []
                    for _, row in df_components_tmp.iterrows():
                        _tkr = str(row.get("Ticker", "")).strip()
                        _cpy = str(row.get("Company", "")).strip()
                        if _is_wl_source and not _cpy:
                            try:
                                _info = get_info(_tkr)
                                _cpy = _info.get("longName") or _info.get("shortName") or ""
                            except:
                                _cpy = ""
                        options_list.append(f"{_tkr} — {_cpy}" if _cpy else _tkr)

                    def _on_component_change():
                        st.session_state["_ticker_source"] = "composant"
                        st.session_state["_ac_reset_count"] = st.session_state.get("_ac_reset_count", 0) + 1

                    selected_component = st.selectbox(
                        t("analyse_composant_label", n=len(df_components_tmp)),
                        options=options_list,
                        key="component_selector",
                        on_change=_on_component_change,
                    )
                    if selected_component:
                        ticker_from_index = selected_component.split(" — ")[0].strip()
                else:
                    st.info(t("analyse_no_index"))
            else:
                st.selectbox(t("analyse_composant_label", n=0), options=[t("analyse_choose_index")],
                             key="component_selector", disabled=True)

        # Ligne 2 : Indice | Bouton | Ticker sélectionné — tous alignés horizontalement
        col_idx, col_btn, col_info = st.columns([2, 1, 2])

        with col_idx:
            selected_comp_label = st.selectbox(
                t("analyse_indice_label"),
                options=all_index_options, index=0, key="sb_composants"
            )
            selected_comp_key = all_index_keys_map[all_index_options.index(selected_comp_label)]

        with col_btn:
            st.markdown("<div style='height:26px'></div>", unsafe_allow_html=True)
            btn_analyser = st.button(t("analyse_launch_btn"), type="primary", use_container_width=True)

        with col_info:
            # Résolution : le dernier widget modifié gagne (tracé via _ticker_source)
            _source = st.session_state.get("_ticker_source", "autocomplete")
            if _source == "composant" and ticker_from_index:
                ticker_input = ticker_from_index
            elif ticker_manual.strip():
                ticker_input = ticker_manual.strip().upper()
            elif ticker_from_index:
                ticker_input = ticker_from_index
            else:
                ticker_input = "AAPL"
            st.markdown("<div style='height:26px'></div>", unsafe_allow_html=True)
            st.markdown(
                f"{t('analyse_ticker_selected_html')} <strong style='color:#4C9BE8;font-size:1rem'>{ticker_input}</strong>",
                unsafe_allow_html=True
            )

    # Période par défaut : 10 Ans (modifiable via le sélecteur à côté du graphique)
    period_label_sel = st.session_state.get("individuel_period_default", list(_get_periods().keys())[6])
    period = _get_periods().get(period_label_sel, "10y")

    _do_analyse = btn_analyser
    if _prefill and not btn_analyser:
        _do_analyse = True
        st.session_state["individuel_prefill"] = ""

    if _do_analyse:
        try:
            hist = get_history(ticker_input, period)
            if hist.empty:
                st.error(t("analyse_data_not_found"))
                st.session_state.individuel_result = None
            else:
                df_reg, std_dev, sigma_pos = compute_regression(hist)
                with st.spinner(t("analyse_loading_fundamentals")):
                    f = compute_fundamentals(ticker_input)
                st.session_state.individuel_result = {
                    "ticker":       ticker_input,
                    "period":       period,
                    "period_label": list(_get_periods().keys())[6],
                    "df_reg":       df_reg,
                    "sigma_pos":    sigma_pos,
                    "f":            f,
                }
        except Exception as e:
            st.error(t("analyse_error", e=e))
            st.session_state.individuel_result = None

    st.markdown("<div style='margin-top:-12px'></div>", unsafe_allow_html=True)
    res = st.session_state.individuel_result
    if res is not None:
        ticker_disp = res['ticker']
        period_disp = res.get('period_label', res['period'])
        company_disp = res["f"]["info"].get("longName", "")

        # ── Bandeau cours live + bouton watchlist ──────────────
        quote = get_live_quote(ticker_disp)
        banner_cols = st.columns([6, 1])
        with banner_cols[0]:
            if quote and quote.get("price"):
                price  = quote["price"]
                chg    = quote.get("change_pct")
                name   = quote.get("name", ticker_disp)
                curr   = quote.get("currency", "")
                if chg is not None:
                    if chg > 0:
                        chg_html = f'<span class="live-up">▲ +{chg:.2f}%</span>'
                    elif chg < 0:
                        chg_html = f'<span class="live-down">▼ {chg:.2f}%</span>'
                    else:
                        chg_html = f'<span class="live-neu">— {chg:.2f}%</span>'
                    live_dot = '<span class="live-dot"></span><span style="color:#28a745;font-size:0.75rem">LIVE</span>'
                else:
                    chg_html = ""
                    live_dot = ""
                st.markdown(
                    f'<div class="live-banner">'
                    f'<div><div class="live-name">{name} &nbsp;<span style="color:#888;font-size:0.8rem">{ticker_disp}</span></div>'
                    f'<div class="live-price">{price:,.2f} <span style="font-size:0.9rem;color:#aaa">{curr}</span></div></div>'
                    f'<div>{chg_html}</div>'
                    f'<div style="margin-left:auto;font-size:0.75rem;color:#666">{live_dot}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        with banner_cols[1]:
            st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
            watchlist_button(ticker_disp, company_disp, key_suffix="analyse")
            yahoo_url = f"https://finance.yahoo.com/quote/{ticker_disp}"
            st.markdown(
                f'<div class="yahoo-link" style="margin-top:6px;font-size:0.75rem;">'
                f'🔗 <a href="{yahoo_url}" target="_blank">{t("analyse_yahoo_link_text", ticker=ticker_disp)}</a></div>',
                unsafe_allow_html=True
            )

        # ── Informations société dans une fenêtre dépliante ───────
        with st.expander(t("analyse_company_expander"), expanded=False):
            render_company_info(ticker_disp, res["f"]["info"])

        st.divider()
        with st.expander(t("analyse_trend_expander"), expanded=True):
            # ── Graphique + sélecteur période + contrôles échelle ─────
            gph_left, gph_right = st.columns([8, 1])
        with gph_right:
            current_period_label = res.get("period_label", period_disp)
            new_period_label = st.selectbox(
                t("chart_period_label"),
                options=list(_get_periods().keys()),
                index=list(_get_periods().keys()).index(current_period_label) if current_period_label in _get_periods() else
                      list(_get_periods().values()).index(res['period']) if res['period'] in _get_periods().values() else 6,
                key="chart_period_selector",
                label_visibility="collapsed"
            )
            new_period = _get_periods()[new_period_label]

            st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

            # Contrôle échelle Y
            yaxis_choice = st.radio(
                t("chart_yaxis_label"),
                options=[t("chart_yaxis_linear"), t("chart_yaxis_log")],
                index=0 if st.session_state.chart_yaxis_type == "linear" else 1,
                key="yaxis_type_radio",
                help=t("chart_yaxis_help"),
            )
            st.session_state.chart_yaxis_type = "linear" if yaxis_choice == t("chart_yaxis_linear") else "log"

            _is_log = (st.session_state.chart_yaxis_type == "log")

            st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

            # Contrôle mode affichage — forcé "Cours" si échelle logarithmique
            display_choice = st.radio(
                t("chart_display_label"),
                options=[t("chart_display_cours"), t("chart_display_pct")],
                index=0,  # forcé à 0 si log, sinon état courant
                key="display_mode_radio",
                help=t("chart_display_help") + (t("chart_display_log_disabled") if _is_log else ""),
                disabled=_is_log,
            )
            if _is_log:
                st.session_state.chart_display_mode = "cours"
            else:
                st.session_state.chart_display_mode = "cours" if display_choice == t("chart_display_cours") else "pct"

        if new_period != res['period']:
            try:
                hist2 = get_history(ticker_disp, new_period)
                if not hist2.empty:
                    df_reg2, _, sigma_pos2 = compute_regression(hist2)
                    st.session_state.individuel_result["period"]       = new_period
                    st.session_state.individuel_result["period_label"] = new_period_label
                    st.session_state.individuel_result["df_reg"]       = df_reg2
                    st.session_state.individuel_result["sigma_pos"]    = sigma_pos2
                    res = st.session_state.individuel_result
            except:
                pass

        live_period_label = res.get("period_label", new_period_label)

        with gph_left:
            fig_reg = build_regression_chart(
                res["df_reg"], ticker_disp, company_disp,
                yaxis_type=st.session_state.chart_yaxis_type,
                display_mode=st.session_state.chart_display_mode,
            )
            # ── Ligne PRU ────────────────────────────────────────
            if st.session_state.chart_display_mode != "pct":
                _pru = get_pru(ticker_disp)
                if _pru is not None:
                    fig_reg.add_hline(
                        y=_pru,
                        line=dict(color="#4C9BE8", width=2),
                        annotation_text=f"PRU {_pru:.2f}",
                        annotation_position="top left",
                        annotation_font=dict(color="#4C9BE8", size=11),
                    )
            st.plotly_chart(fig_reg, use_container_width=True)

        sigma_pos = res["sigma_pos"]
        zone_label = next(
            (lbl for lbl, (mn, mx, _) in _get_sigma_criteria().items() if mn <= sigma_pos < mx),
            f"{sigma_pos:+.2f}σ"
        )
        st.caption(t("analyse_position_caption", sigma=f"{sigma_pos:+.2f}", zone=zone_label.split("(")[0].strip()))

        st.divider()

        # ── Graphique Analyse Technique ───────────────────────────
        with st.expander(t("analyse_tech_expander"), expanded=False):
            tc_col1, tc_col2 = st.columns([6, 1])

            with tc_col2:
                st.markdown(t("tc_period_section"))
                tc_period_label = st.selectbox(
                    "Période AT",
                    options=list(_get_periods().keys()),
                    index=list(_get_periods().keys()).index(
                        res.get("period_label", list(_get_periods().keys())[6])
                    ) if res.get("period_label", list(_get_periods().keys())[6]) in _get_periods() else 6,
                    key="tc_period_selector",
                    label_visibility="collapsed",
                )
                _tc_period_raw = _get_periods()[tc_period_label]

                st.markdown(t("tc_granularity_section"))
                _TC_INTERVALS = {
                    "1 min":   "1m",
                    "2 min":   "2m",
                    "5 min":   "5m",
                    "15 min":  "15m",
                    "30 min":  "30m",
                    "1 heure": "1h",
                    "1 jour":  "1d",
                    "1 sem.":  "1wk",
                    "1 mois":  "1mo",
                }
                tc_interval_label = st.selectbox(
                    "Granularité AT",
                    options=list(_TC_INTERVALS.keys()),
                    index=6,   # défaut : 1 jour
                    key="tc_interval_selector",
                    label_visibility="collapsed",
                )
                _tc_interval = _TC_INTERVALS[tc_interval_label]

                # Contrainte automatique période max selon intervalle
                _tc_max_period = _INTRADAY_MAX_PERIOD.get(_tc_interval, None)
                _TC_PERIOD_DAYS = {
                    "1d": 1, "5d": 5, "7d": 7, "1mo": 30, "3mo": 90,
                    "6mo": 180, "1y": 365, "2y": 730, "5y": 1825,
                    "10y": 3650, "max": 99999,
                }
                if _tc_max_period:
                    _max_days  = _TC_PERIOD_DAYS.get(_tc_max_period, 99999)
                    _req_days  = _TC_PERIOD_DAYS.get(_tc_period_raw, 99999)
                    if _req_days > _max_days:
                        _tc_period = _tc_max_period
                        st.caption(f"⚠️ Période limitée à **{_tc_max_period}** pour la granularité {tc_interval_label}.")
                    else:
                        _tc_period = _tc_period_raw
                else:
                    _tc_period = _tc_period_raw

                st.markdown(t("tc_indicators_section"))
                tc_show_ma      = st.checkbox(t("tc_ma_label"), value=True,  key="tc_show_ma")
                tc_show_bb      = st.checkbox(t("tc_bb_label"), value=False, key="tc_show_bb")
                tc_show_rsi     = st.checkbox(t("tc_rsi_label"), value=False, key="tc_show_rsi")
                tc_show_volume  = st.checkbox(t("tc_volume_label"), value=False, key="tc_show_volume")

                if tc_show_ma:
                    st.markdown(t("tc_ma_periods_section"))
                    tc_ma1 = st.number_input("MM 1", value=9,   min_value=1, max_value=500, key="tc_ma1", label_visibility="collapsed")
                    tc_ma2 = st.number_input("MM 2", value=20,  min_value=1, max_value=500, key="tc_ma2", label_visibility="collapsed")
                    tc_ma3 = st.number_input("MM 3", value=200, min_value=1, max_value=500, key="tc_ma3", label_visibility="collapsed")
                    st.caption(t("analyse_ma_caption", ma1=int(tc_ma1), ma2=int(tc_ma2), ma3=int(tc_ma3)))

                if tc_show_bb:
                    tc_bb_period = st.number_input(t("tc_bb_period_label"), value=20, min_value=5, max_value=100, key="tc_bb_period")
                    tc_bb_std    = st.number_input(t("tc_bb_std_label"), value=2.0, min_value=0.5, max_value=4.0, step=0.5, key="tc_bb_std")

                st.markdown(t("tc_options_section"))
                tc_hide_gaps = st.checkbox(
                    t("tc_hide_gaps_label"),
                    value=True,
                    key="tc_hide_gaps",
                    help=t("tc_hide_gaps_help")
                )

            with tc_col1:
                try:
                    _tc_hist = get_history_intraday(ticker_disp, _tc_period, _tc_interval)
                    if _tc_hist.empty:
                        st.warning(t("tc_no_data"))
                    else:
                        # Nettoyage colonnes (MultiIndex possible avec yfinance)
                        if isinstance(_tc_hist.columns, pd.MultiIndex):
                            _tc_hist.columns = _tc_hist.columns.get_level_values(0)
                        _tc_hist = _tc_hist.reset_index()
                        _date_col = _tc_hist.columns[0]

                        # Nombre de sous-graphiques
                        _n_rows = 1
                        _row_heights = [0.6]
                        if tc_show_rsi:
                            _n_rows += 1
                            _row_heights.append(0.2)
                        if tc_show_volume:
                            _n_rows += 1
                            _row_heights.append(0.2)
                        # Normaliser les hauteurs
                        _total = sum(_row_heights)
                        _row_heights = [h / _total for h in _row_heights]

                        _subplot_titles = [f"{ticker_disp} — Cours"]
                        if tc_show_rsi:    _subplot_titles.append("RSI")
                        if tc_show_volume: _subplot_titles.append("Volume")

                        fig_tc = make_subplots(
                            rows=_n_rows, cols=1,
                            shared_xaxes=True,
                            row_heights=_row_heights,
                            subplot_titles=_subplot_titles,
                            vertical_spacing=0.04,
                        )

                        # ── Cours (chandelier japonais) ──────────────────
                        if all(c in _tc_hist.columns for c in ["Open", "High", "Low", "Close"]):
                            fig_tc.add_trace(go.Candlestick(
                                x=_tc_hist[_date_col],
                                open=_tc_hist["Open"], high=_tc_hist["High"],
                                low=_tc_hist["Low"],   close=_tc_hist["Close"],
                                name="Cours",
                                increasing_line_color="#26a69a",
                                decreasing_line_color="#ef5350",
                                showlegend=False,
                            ), row=1, col=1)
                        else:
                            fig_tc.add_trace(go.Scatter(
                                x=_tc_hist[_date_col], y=_tc_hist["Close"],
                                mode="lines", name="Cours",
                                line=dict(color="#4C9BE8", width=1.5),
                            ), row=1, col=1)

                        # ── Moyennes mobiles ─────────────────────────────
                        if tc_show_ma:
                            _ma_colors = ["#FFD700", "#FF6B35", "#A855F7"]
                            for _ma_p, _ma_col in zip(
                                [int(tc_ma1), int(tc_ma2), int(tc_ma3)], _ma_colors
                            ):
                                if len(_tc_hist) >= _ma_p:
                                    _ma_vals = _tc_hist["Close"].rolling(_ma_p).mean()
                                    fig_tc.add_trace(go.Scatter(
                                        x=_tc_hist[_date_col], y=_ma_vals,
                                        mode="lines", name=f"MM{_ma_p}",
                                        line=dict(color=_ma_col, width=1.2, dash="solid"),
                                    ), row=1, col=1)

                        # ── Bandes de Bollinger ──────────────────────────
                        if tc_show_bb:
                            _bb_p   = int(tc_bb_period)
                            _bb_s   = float(tc_bb_std)
                            _bb_mid = _tc_hist["Close"].rolling(_bb_p).mean()
                            _bb_std = _tc_hist["Close"].rolling(_bb_p).std()
                            _bb_up  = _bb_mid + _bb_s * _bb_std
                            _bb_dn  = _bb_mid - _bb_s * _bb_std
                            fig_tc.add_trace(go.Scatter(
                                x=_tc_hist[_date_col], y=_bb_up,
                                mode="lines", name=f"BB+ {_bb_s}σ",
                                line=dict(color="rgba(76,155,232,0.6)", width=1, dash="dot"),
                            ), row=1, col=1)
                            fig_tc.add_trace(go.Scatter(
                                x=_tc_hist[_date_col], y=_bb_dn,
                                mode="lines", name=f"BB- {_bb_s}σ",
                                line=dict(color="rgba(76,155,232,0.6)", width=1, dash="dot"),
                                fill="tonexty",
                                fillcolor="rgba(76,155,232,0.05)",
                            ), row=1, col=1)
                            fig_tc.add_trace(go.Scatter(
                                x=_tc_hist[_date_col], y=_bb_mid,
                                mode="lines", name=f"BB mid",
                                line=dict(color="rgba(76,155,232,0.4)", width=1),
                            ), row=1, col=1)

                        # ── RSI ──────────────────────────────────────────
                        _rsi_row = None
                        if tc_show_rsi:
                            _rsi_row = 2
                            _delta = _tc_hist["Close"].diff()
                            _gain  = _delta.clip(lower=0).rolling(14).mean()
                            _loss  = (-_delta.clip(upper=0)).rolling(14).mean()
                            _rs    = _gain / _loss.replace(0, float("nan"))
                            _rsi   = 100 - (100 / (1 + _rs))
                            fig_tc.add_trace(go.Scatter(
                                x=_tc_hist[_date_col], y=_rsi,
                                mode="lines", name="RSI(14)",
                                line=dict(color="#FF6B35", width=1.5),
                            ), row=_rsi_row, col=1)
                            # Zones 30 / 70
                            for _lvl, _clr in [(70, "rgba(220,53,69,0.3)"), (30, "rgba(40,167,69,0.3)")]:
                                fig_tc.add_hline(
                                    y=_lvl, line_dash="dot",
                                    line_color=_clr, row=_rsi_row, col=1
                                )
                            fig_tc.update_yaxes(range=[0, 100], row=_rsi_row, col=1)

                        # ── Volume ───────────────────────────────────────
                        if tc_show_volume and "Volume" in _tc_hist.columns:
                            _vol_row = _rsi_row + 1 if tc_show_rsi else 2
                            _vol_colors = [
                                "#26a69a" if c >= o else "#ef5350"
                                for c, o in zip(_tc_hist["Close"], _tc_hist.get("Open", _tc_hist["Close"]))
                            ]
                            fig_tc.add_trace(go.Bar(
                                x=_tc_hist[_date_col], y=_tc_hist["Volume"],
                                name="Volume",
                                marker_color=_vol_colors,
                                showlegend=False,
                            ), row=_vol_row, col=1)

                        # ── Mise en page ─────────────────────────────────
                        _tc_pru = get_pru(ticker_disp)
                        if _tc_pru is not None:
                            fig_tc.add_hline(
                                y=_tc_pru, row=1, col=1,
                                line=dict(color="#4C9BE8", width=2),
                                annotation_text=f"PRU {_tc_pru:.2f}",
                                annotation_position="top left",
                                annotation_font=dict(color="#4C9BE8", size=11),
                            )
                        _tc_height = 420 + (_n_rows - 1) * 160
                        fig_tc.update_layout(
                            template="plotly_dark",
                            height=_tc_height,
                            margin=dict(l=10, r=10, t=30, b=10),
                            legend=dict(
                                orientation="h", yanchor="bottom", y=1.01,
                                xanchor="left", x=0, font=dict(size=11),
                                bgcolor="rgba(0,0,0,0)",
                            ),
                            xaxis_rangeslider_visible=False,
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(13,27,42,0.8)",
                        )
                        # ── Sauts temporels (weekends, nuits) ────────────
                        _rangebreaks = []
                        if tc_hide_gaps:
                            if _tc_interval in ("1m", "2m", "5m", "15m", "30m", "60m", "1h"):
                                # Intraday : masquer nuits (17h→9h) + weekends
                                _rangebreaks = [
                                    dict(bounds=["sat", "mon"]),          # weekends
                                    dict(bounds=[17, 9], pattern="hour"), # nuits
                                ]
                            else:
                                # Daily/weekly : masquer seulement weekends
                                _rangebreaks = [
                                    dict(bounds=["sat", "mon"]),
                                ]

                        fig_tc.update_xaxes(
                            showgrid=True, gridcolor="#1e2e3e", zeroline=False,
                            rangebreaks=_rangebreaks if _rangebreaks else None,
                        )
                        fig_tc.update_yaxes(
                            showgrid=True, gridcolor="#1e2e3e", zeroline=False,
                        )
                        st.plotly_chart(fig_tc, use_container_width=True)

                except Exception as _e:
                    st.error(t("tc_chart_err", e=_e))

        st.divider()
        # ── Prix Juste Historique ──────────────────────────────────
        with st.expander(t("analyse_fv_expander"), expanded=False):
            fv_methods = ["DCF", "Multiples (P/E)", "Gordon-Shapiro (DDM)", "ANR (Book Value)"]
            fv_gran_opts_fr = {"Mensuelle": "1mo", "Hebdomadaire": "1wk", "Annuelle": "1y"}
            fv_gran_opts = {t("fv_gran_monthly"): "1mo", t("fv_gran_weekly"): "1wk", t("fv_gran_yearly"): "1y"}

            # ── Calcul de la méthode suggérée (avant les widgets) ──
            _info_fv = res["f"]["info"]
            _beta    = float(_info_fv.get("beta") or 1.0)
            _debt    = res["f"].get("debt_fcf") or 0
            _sector  = (_info_fv.get("sector") or "").lower()
            _div_yield  = float(_info_fv.get("dividendYield") or 0)
            _div_yield  = _div_yield if _div_yield < 1 else _div_yield / 100
            _payout     = float(_info_fv.get("payoutRatio") or 0)
            _fcf        = _info_fv.get("freeCashflow") or 0

            # Logique de sélection automatique
            if _div_yield > 0.02 and _payout > 0.40:
                _suggested_method = "Gordon-Shapiro (DDM)"
                _suggested_reason = (
                    f"**{_info_fv.get('shortName', ticker_disp)}** verse un dividende significatif "
                    f"({_div_yield*100:.1f}% de rendement, taux de distribution {_payout*100:.0f}%). "
                    "La méthode Gordon-Shapiro est la plus adaptée pour valoriser une société "
                    "mature qui distribue régulièrement ses bénéfices."
                )
                _suggested_icon = "💰"
            elif any(s in _sector for s in ["real estate", "financial"]) and \
                 float(_info_fv.get("priceToBook") or 99) < 2.5:
                _suggested_method = "ANR (Book Value)"
                _suggested_reason = (
                    f"**{_info_fv.get('shortName', ticker_disp)}** opère dans le secteur "
                    f"**{_info_fv.get('sector', '?')}** avec un P/B faible "
                    f"({_info_fv.get('priceToBook', '?'):.1f}x). "
                    "La valeur patrimoniale (ANR) est la référence pour les sociétés foncières, "
                    "holdings et établissements financiers dont les actifs au bilan sont déterminants."
                )
                _suggested_icon = "🏢"
            elif _fcf and _fcf > 0 and any(s in _sector for s in [
                "technology", "software", "communication", "healthcare",
                "consumer discretionary", "industrials"
            ]):
                _suggested_method = "DCF"
                _suggested_reason = (
                    f"**{_info_fv.get('shortName', ticker_disp)}** génère un Free Cash Flow positif "
                    f"dans le secteur **{_info_fv.get('sector', '?')}**. "
                    "La méthode DCF est la plus adaptée : elle capture la valeur intrinsèque "
                    "basée sur les flux futurs actualisés, idéale pour les entreprises "
                    "à croissance prévisible."
                )
                _suggested_icon = "📈"
            else:
                _suggested_method = "Multiples (P/E)"
                _suggested_reason = (
                    f"**{_info_fv.get('shortName', ticker_disp)}** opère dans le secteur "
                    f"**{_info_fv.get('sector', '?')}**, un secteur homogène "
                    "où la comparaison par multiples (P/E sectoriel) est la méthode "
                    "la plus fiable et la plus rapide pour estimer un juste prix relatif."
                )
                _suggested_icon = "⚖️"

            fv_c1, fv_c2, fv_c3, fv_c4 = st.columns([2, 2, 1, 1])
            with fv_c1:
                # Si la case "méthode auto" est cochée, on force la méthode suggérée
                _use_auto = st.session_state.get("fv_use_auto", False)
                _default_idx = fv_methods.index(_suggested_method) if _use_auto else \
                               st.session_state.get("fv_method_idx", 0)
                fv_method = st.selectbox(
                    t("fv_method_label"), fv_methods,
                    index=fv_methods.index(_suggested_method) if _use_auto else 0,
                    key="fv_method",
                    disabled=_use_auto,
                )
            with fv_c2:
                fv_gran_label = st.selectbox(t("fv_granularity_label"), list(fv_gran_opts.keys()),
                                             index=0, key="fv_gran")
                fv_gran = fv_gran_opts[fv_gran_label]
            with fv_c3:
                _fv_period_default = res.get("period_label", list(_get_periods().keys())[6])
                fv_period_label = st.selectbox(
                    "Période",
                    options=list(_get_periods().keys()),
                    index=list(_get_periods().keys()).index(_fv_period_default)
                          if _fv_period_default in _get_periods() else 6,
                    key="fv_period_selector",
                )
                fv_period = _get_periods()[fv_period_label]
            with fv_c4:
                fv_overlay = st.checkbox(t("fv_overlay_label"), value=True,
                                         key="fv_overlay")
                fv_use_auto = st.checkbox(
                    t("fv_auto_label"),
                    value=False,
                    key="fv_use_auto",
                    help=t("fv_auto_help", method=_suggested_method)
                )

            # Si méthode auto cochée, forcer la méthode suggérée
            if fv_use_auto:
                fv_method = _suggested_method

            # Bulle d'information sur la méthode suggérée
            st.markdown(
                f'<div style="background:rgba(76,155,232,0.08);border-left:3px solid #4C9BE8;'
                f'border-radius:6px;padding:8px 12px;margin:4px 0 8px 0;font-size:0.83rem;color:#ccc;">'
                f'{_suggested_icon} <strong>{t("fv_suggested")} {_suggested_method}</strong><br>'
                f'{_suggested_reason}'
                f'</div>',
                unsafe_allow_html=True
            )

            # WACC auto : taux sans risque 4% + prime de risque 5% × beta + spread dette
            _risk_free  = 0.04
            _mkt_prem   = 0.05
            _debt_spread = 0.01 if _debt < 2 else 0.02 if _debt < 4 else 0.03
            _wacc_auto  = round((_risk_free + _mkt_prem * _beta + _debt_spread) * 100, 1)
            _wacc_auto  = max(6.0, min(_wacc_auto, 18.0))

            # g perpétuel auto : secteur tech/croissance → 3%, mature → 2%, utilities → 1.5%
            if any(s in _sector for s in ["technology", "software", "communication"]):
                _g_auto = 3.0
            elif any(s in _sector for s in ["utilities", "real estate", "consumer staples"]):
                _g_auto = 1.5
            else:
                _g_auto = 2.5

            # Rendement exigé Gordon auto : légèrement > WACC, min 5%
            _k_gs_auto = round(max(_wacc_auto + 0.5, 6.0), 1)

            # Applicabilité selon la méthode sélectionnée
            _is_dcf     = (fv_method == "DCF")
            _is_gordon  = (fv_method == "Gordon-Shapiro (DDM)")
            _dcf_only   = not _is_dcf     # grisé si pas DCF
            _gordon_only= not _is_gordon  # grisé si pas Gordon

            # Paramètres ajustables selon la méthode
            with st.expander("⚙️ Paramètres avancés", expanded=False):
                st.caption(t("fv_adv_params_caption"))
                p_c1, p_c2, p_c3, p_c4 = st.columns(4)
                with p_c1:
                    fv_wacc = st.number_input(
                        t("fv_wacc_label"), min_value=1.0, max_value=30.0,
                        value=_wacc_auto, step=0.5, key="fv_wacc",
                        disabled=_dcf_only,
                    ) / 100
                    st.markdown(
                        f"<small style='color:#888'>📌 Estimé : {_wacc_auto}%<br>"
                        f"β={_beta:.2f} · Dette/FCF={_debt:.1f}<br>"
                        f"Secteur : {_info_fv.get('sector','?')}<br>"
                        f"{t('fv_dcf_only')}</small>",
                        unsafe_allow_html=True
                    )
                with p_c2:
                    fv_g_perp = st.number_input(
                        t("fv_g_label"), min_value=0.0, max_value=5.0,
                        value=_g_auto, step=0.25, key="fv_g_perp",
                        disabled=_dcf_only,
                    ) / 100
                    st.markdown(
                        f"<small style='color:#888'>📌 Estimé : {_g_auto}%<br>"
                        f"Secteur : {_info_fv.get('sector','?')}<br>"
                        "Doit rester &lt; WACC<br>"
                        f"{t('fv_dcf_only')}</small>",
                        unsafe_allow_html=True
                    )
                with p_c3:
                    fv_k_gs = st.number_input(
                        t("fv_k_gs_label"), min_value=1.0, max_value=20.0,
                        value=_k_gs_auto, step=0.5, key="fv_k_gs",
                        disabled=_gordon_only,
                    ) / 100
                    st.markdown(
                        f"<small style='color:#888'>📌 Estimé : {_k_gs_auto}%<br>"
                        "Légèrement &gt; WACC<br>"
                        "Doit être &gt; g dividende<br>"
                        f"{t('fv_gordon_only')}</small>",
                        unsafe_allow_html=True
                    )
                with p_c4:
                    fv_horizon = st.number_input(
                        t("fv_horizon_label"), min_value=3, max_value=20,
                        value=10, step=1, key="fv_horizon",
                        disabled=_dcf_only,
                    )
                    st.markdown(
                        "<small style='color:#888'>📌 Défaut : 10 ans<br>"
                        "5 ans → cycliques<br>"
                        "10 ans → standard<br>"
                        "15–20 ans → forte croissance<br>"
                        f"{t('fv_dcf_only')}</small>",
                        unsafe_allow_html=True
                    )

            # Calcul
            with st.spinner(t("fv_calc_spinner")):
                df_fv = compute_fair_value_history(
                    ticker_disp, fv_period, fv_method, fv_gran,
                    fv_wacc, fv_g_perp, fv_k_gs, int(fv_horizon)
                )

            if df_fv is not None and not df_fv.empty:
                fv_valid = df_fv.dropna(subset=["Prix_Juste"])
                if fv_valid.empty:
                    st.info("ℹ️ Données insuffisantes pour calculer le prix juste avec cette méthode "
                            "sur la période sélectionnée. Essayez une autre méthode ou une période plus longue.")
                else:
                    fig_fv = go.Figure()

                    if fv_overlay:
                        fig_fv.add_trace(go.Scatter(
                            x=df_fv["Date"], y=df_fv["Prix_Reel"],
                            mode="lines", name="Cours réel",
                            line=dict(color="#FFFFFF", width=2),
                            hovertemplate="Cours réel : %{y:.2f}<extra></extra>"
                        ))

                    fig_fv.add_trace(go.Scatter(
                        x=fv_valid["Date"], y=fv_valid["Prix_Juste"],
                        mode="lines", name=f"Prix juste ({fv_method})",
                        line=dict(color="#FFD700", width=2.5, dash="dash"),
                        hovertemplate=f"Prix juste : %{{y:.2f}}<extra></extra>"
                    ))

                    # Zone colorée : sous/sur-évalué
                    if fv_overlay and not fv_valid.empty:
                        fig_fv.add_trace(go.Scatter(
                            x=pd.concat([fv_valid["Date"], fv_valid["Date"][::-1]]).tolist(),
                            y=pd.concat([fv_valid["Prix_Juste"],
                                         df_fv.loc[fv_valid.index, "Prix_Reel"][::-1]]).tolist(),
                            fill="toself", fillcolor="rgba(76,155,232,0.08)",
                            line=dict(color="rgba(255,255,255,0)"),
                            showlegend=False, hoverinfo="skip",
                        ))

                    # Calcul du ratio actuel
                    last_valid = fv_valid.iloc[-1]
                    last_real  = df_fv.iloc[-1]["Prix_Reel"]
                    if last_valid["Prix_Juste"] and last_real:
                        ecart_pct = (last_real - last_valid["Prix_Juste"]) / last_valid["Prix_Juste"] * 100
                        if ecart_pct > 5:
                            ecart_txt = f"⚠️ Sur-évalué de {ecart_pct:+.1f}% par rapport au prix juste"
                            ecart_col = "#FF4C4C"
                        elif ecart_pct < -5:
                            ecart_txt = f"🟢 Sous-évalué de {ecart_pct:+.1f}% par rapport au prix juste"
                            ecart_col = "#4CE87A"
                        else:
                            ecart_txt = f"➡️ À proximité du prix juste ({ecart_pct:+.1f}%)"
                            ecart_col = "#FFD700"

                        fig_fv.add_annotation(
                            x=0.01, y=0.97, xref="paper", yref="paper",
                            text=ecart_txt, showarrow=False,
                            font=dict(color=ecart_col, size=12),
                            bgcolor="rgba(0,0,0,0.5)", bordercolor=ecart_col,
                            borderwidth=1, borderpad=6, align="left",
                        )

                    fig_fv.update_layout(
                        title=dict(
                            text=f"{ticker_disp} — Prix juste ({fv_method}) | {fv_period_label}",
                            font_size=14
                        ),
                        template="plotly_dark", height=360,
                        margin=dict(l=55, r=20, t=45, b=30),
                        hovermode="x unified",
                        legend=dict(orientation="h", y=-0.15, font_size=10),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=True, gridcolor="#333", title="Prix"),
                    )
                    st.plotly_chart(fig_fv, use_container_width=True)

                    # Légende méthode
                    method_tips = {
                        "DCF": "💡 DCF : valeur intrinsèque basée sur les flux de trésorerie futurs actualisés. "
                               "Plus fiable pour les entreprises générant du FCF régulier.",
                        "Multiples (P/E)": "💡 Multiples P/E : prix juste = P/E historique moyen × BPA. "
                                           "Méthode relative — dépend du niveau de valorisation du marché.",
                        "Gordon-Shapiro (DDM)": "💡 Gordon-Shapiro : valeur = D₁ / (k − g). "
                                                "Pertinent uniquement pour les sociétés à dividende croissant.",
                        "ANR (Book Value)": "💡 ANR : valeur patrimoniale = capitaux propres / nombre d'actions. "
                                            "Adapté aux holdings, foncières, sociétés à fort actif tangible.",
                    }
                    st.caption(method_tips.get(fv_method, ""))
            else:
                st.warning("⚠️ Impossible de récupérer les données pour calculer le prix juste.")

        st.divider()
        with st.expander("📊 Scorecard Fondamental & Score Global", expanded=True):
            render_scorecard(res["f"])

        st.divider()
        with st.expander("📊 Historique des Indicateurs Fondamentaux", expanded=True):
            render_historical_charts(res["f"], ticker_disp)
    else:
        if not btn_analyser:
            st.info(t("analyse_configure_first"))


# ============================================================
# PAGE WATCHLIST
# ============================================================
elif current_page == t("page_watchlists"):
    st.title(t("wl_title"))

    # ── Bandeau d'information token anonyme ───────────────────
    uid_display = get_user_id()[:8] + "..."
    st.info(t("wl_info_full", uid=uid_display))

    # ── Gestion des watchlists ─────────────────────────────────
    wl_names = load_wl_index()
    if st.session_state.active_watchlist not in wl_names:
        st.session_state.active_watchlist = wl_names[0]

    with st.expander(t("wl_manage_expander"), expanded=True):
        wl_hdr_col1, wl_hdr_col2, wl_hdr_col3, wl_hdr_col4 = st.columns([3, 1, 1, 1])
        with wl_hdr_col1:
            selected_wl = st.selectbox(
                "📋 Watchlist active",
                options=wl_names,
                index=wl_names.index(st.session_state.active_watchlist),
                key="wl_selector",
                label_visibility="collapsed"
            )
            if selected_wl != st.session_state.active_watchlist:
                st.session_state.active_watchlist = selected_wl
                st.rerun()
        with wl_hdr_col2:
            if st.button(t("wl_new_btn"), use_container_width=True, key="wl_new_list_btn"):
                st.session_state["wl_show_create"] = True
        with wl_hdr_col3:
            if st.button(t("wl_delete_btn"), use_container_width=True, key="wl_del_list_btn",
                         help=f"Supprimer la watchlist '{st.session_state.active_watchlist}'"):
                st.session_state["wl_show_delete"] = True
        with wl_hdr_col4:
            if st.button(t("wl_import_btn"), use_container_width=True, key="wl_import_btn",
                         help="Importer un compte titre depuis Portfolio Performance (.xml)"):
                st.session_state.import_step   = 1
                st.session_state.import_parsed = None
                dialog_import_portfolio()

        # ── Dialogs création / suppression ─────────────────────────
        if st.session_state.get("wl_show_create"):
            with st.form("form_create_wl"):
                new_wl_name = st.text_input("Nom de la nouvelle watchlist", placeholder="ex : Tech USA, Dividendes…")
                c1, c2 = st.columns(2)
                with c1:
                    submitted = st.form_submit_button("✅ Créer", use_container_width=True)
                with c2:
                    cancelled = st.form_submit_button("❌ Annuler", use_container_width=True)
                if submitted and new_wl_name.strip():
                    create_watchlist(new_wl_name.strip())
                    st.session_state.active_watchlist = new_wl_name.strip()
                    st.session_state["wl_show_create"] = False
                    st.rerun()
                if cancelled:
                    st.session_state["wl_show_create"] = False
                    st.rerun()

        if st.session_state.get("wl_show_delete"):
            wl_to_del = st.session_state.active_watchlist
            st.warning(t("wl_delete_confirm_warn", name=wl_to_del))
            d1, d2 = st.columns(2)
            with d1:
                if st.button("🗑️ Confirmer la suppression", use_container_width=True, key="wl_del_confirm"):
                    remaining = delete_watchlist(wl_to_del)
                    if not remaining:
                        create_watchlist("Ma Watchlist")
                        remaining = ["Ma Watchlist"]
                    st.session_state.active_watchlist = remaining[0]
                    st.session_state["wl_show_delete"] = False
                    st.rerun()
            with d2:
                if st.button("❌ Annuler", use_container_width=True, key="wl_del_cancel"):
                    st.session_state["wl_show_delete"] = False
                    st.rerun()

        st.caption(t("wl_autosave_caption", name=st.session_state.active_watchlist))

    df_wl = load_watchlist(st.session_state.active_watchlist)

    # ── Ajout manuel ─────────────────────────────────────────
    with st.expander(t("wl_add_expander"), expanded=len(df_wl) == 0):
        wl_add_col1, wl_add_col2, wl_add_col3 = st.columns([2, 3, 1])
        with wl_add_col1:
            new_ticker = st.text_input("Ticker", placeholder="ex : AAPL, MC.PA…", key="wl_new_ticker").strip().upper()
        with wl_add_col2:
            new_company = st.text_input("Nom (optionnel)", placeholder="ex : Apple Inc.", key="wl_new_company").strip()
        with wl_add_col3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button(t("wl_add_btn"), type="primary", use_container_width=True, key="wl_add_btn"):
                if new_ticker:
                    # Tentative de récupération auto du nom si vide
                    company_to_add = new_company
                    if not company_to_add:
                        try:
                            q = get_live_quote(new_ticker)
                            company_to_add = q.get("name", "") if q else ""
                        except:
                            company_to_add = ""
                    ok = add_to_watchlist(new_ticker, company_to_add, name=st.session_state.active_watchlist)
                    if ok:
                        st.success(t("wl_add_ok", ticker=new_ticker))
                        st.rerun()
                    else:
                        st.warning(t("wl_add_duplicate", ticker=new_ticker))
                else:
                    st.warning(t("wl_add_empty"))

    st.divider()

    # ── Tableau principal ─────────────────────────────────────
    df_wl = load_watchlist(st.session_state.active_watchlist)
    if df_wl.empty:
        st.info(t("wl_empty_info"))
    else:
        # Boutons de contrôle — même ligne horizontale
        ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 4])
        with ctrl1:
            st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
            refresh_wl = st.button(t("wl_refresh_btn"), use_container_width=True,
                                   help="Vide le cache et recharge :\n"
                                        "• Cours live et variation journalière\n"
                                        "• Score fondamental, ROIC, Marge FCF\n"
                                        "• Dette/FCF, P/E, PEG\n"
                                        "• Position σ")
        with ctrl2:
            wl_period_label = st.selectbox(
                t("wl_period_sigma_label"),
                options=list(_get_periods().keys()),
                index=3,  # 1 year (index stable across languages)
                key="wl_period",
            )
            wl_period = _get_periods()[wl_period_label]

        st.markdown(t("wl_stocks_followed", n=len(df_wl), uid=f"*Période σ : {wl_period_label}*"))

        # ── Chargement des données pour chaque ligne ──────────
        rows_data = []
        load_errors = []

        progress = st.progress(0, text="Chargement des données…")
        total_wl = len(df_wl)

        for idx, row in df_wl.iterrows():
            tkr = row["ticker"]
            progress.progress((idx + 1) / total_wl if total_wl > 0 else 1,
                              text=f"Chargement {tkr}…")
            entry = {
                "ticker":     tkr,
                "company":    row["company"],
                "ajout_date": row["ajout_date"],
                "note":       row["note"],
                "prix_achat": row["prix_achat"],
                # données live
                "price":      None, "change_pct": None, "currency": "",
                "price_eur":  None,   # cours converti en EUR
                "name":       row["company"] or tkr,
                # fondamentaux
                "score_10":   None, "grade": "—",
                "rev_growth": None, "roic": None, "fcf_margin": None,
                "debt_fcf":   None, "pe_ratio": None, "peg": None,
                "sigma_pos":  None,
                # écart PRU / cours actuel (en EUR)
                "pru_pct":    None,
            }
            try:
                q = get_live_quote(tkr)
                if q:
                    entry["price"]      = q.get("price")
                    entry["change_pct"] = q.get("change_pct")
                    entry["currency"]   = q.get("currency", "")
                    if not entry["company"]:
                        entry["name"]    = q.get("name", tkr)
                        entry["company"] = q.get("name", tkr)

                    # Convertir le cours en EUR
                    curr = (entry["currency"] or "EUR").upper()
                    if entry["price"]:
                        if curr == "EUR":
                            entry["price_eur"] = entry["price"]
                        else:
                            try:
                                rate = get_eur_to_currency_rate(curr)
                                # rate = EUR→curr, donc price_eur = price / rate
                                if rate and rate > 0:
                                    entry["price_eur"] = round(entry["price"] / rate, 4)
                            except:
                                pass

                    # Calcul +/- PRU : price_eur vs prix_achat (en EUR)
                    prix_achat_raw = row["prix_achat"]
                    if prix_achat_raw and entry["price_eur"]:
                        try:
                            # Parser : accepte "152.30" ou "152,30"
                            pru_eur = float(str(prix_achat_raw).strip().split()[0].replace(",", "."))
                            if pru_eur > 0:
                                entry["pru_pct"] = round(
                                    (entry["price_eur"] - pru_eur) / pru_eur * 100, 2
                                )
                        except:
                            pass
            except:
                load_errors.append(tkr)

            try:
                f_data = compute_fundamentals(tkr)
                entry["score_10"]   = f_data["score_10"]
                entry["grade"]      = f_data["grade"]
                entry["rev_growth"] = f_data["rev_growth"]
                entry["roic"]       = f_data["roic"]
                entry["fcf_margin"] = f_data["fcf_margin"]
                entry["debt_fcf"]   = f_data["debt_fcf"]
                entry["pe_ratio"]   = f_data["pe_ratio"]
                entry["peg"]        = f_data["peg"]
                if not entry["company"] or entry["company"] == tkr:
                    entry["company"] = f_data["info"].get("longName", "") or entry["company"]
                    entry["name"]    = entry["company"]
            except:
                pass

            try:
                h = get_history(tkr, wl_period)
                if not h.empty and len(h) >= 20:
                    _, _, sp = compute_regression(h)
                    entry["sigma_pos"] = sp
            except:
                pass

            rows_data.append(entry)

        progress.empty()
        if load_errors:
            st.caption(t("wl_data_errors_caption", tickers=", ".join(load_errors)))

        # ── Helpers formatage valeurs ─────────────────────────
        def fmt_num(v, suffix="", dec=1):
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return None
            return round(float(v), dec)

        # ── Construction du DataFrame interactif ──────────────
        df_display_rows = []
        for e in rows_data:
            price_str     = f"{e['price']:,.2f} {e['currency']}" if e['price'] else "—"
            price_eur_val = round(e['price_eur'], 2) if e['price_eur'] else None
            pru_val       = None
            if e['prix_achat']:
                try:
                    pru_val = float(str(e['prix_achat']).strip().split()[0].replace(",", "."))
                except:
                    pass
            df_display_rows.append({
                "📈":            False,
                "Ticker":        e['ticker'],
                "Société":       e.get('name') or e.get('company') or "—",
                "Prix":          price_str,
                "Prix EUR (€)":  price_eur_val,
                "Var. J. (%)":   round(e['change_pct'], 2) if e['change_pct'] is not None else None,
                "Score":         f"{e['score_10']:.1f}/10 ({e['grade']})" if e['score_10'] is not None else "—",
                "Croiss. CA (%)": fmt_num(e['rev_growth']),
                "ROIC (%)":      fmt_num(e['roic']),
                "Marge FCF (%)": fmt_num(e['fcf_margin']),
                "Dette/FCF":     fmt_num(e['debt_fcf'], dec=2),
                "P/E":           fmt_num(e['pe_ratio'], dec=1),
                "PEG":           fmt_num(e['peg'], dec=2),
                "σ Position":    round(e['sigma_pos'], 2) if e['sigma_pos'] is not None else None,
                "PRU (€)":       pru_val,
                "+/- PRU (%)":   round(e['pru_pct'], 1) if e['pru_pct'] is not None else None,
                "Ajouté le":     e['ajout_date'] or "",
                "Note":          e['note'] or "",
            })

        df_display = pd.DataFrame(df_display_rows)

        # ── Fonctions de coloration Pandas Styler ─────────────
        def _color_signed(val, pos_color="#28a745", neg_color="#dc3545", neu_color=""):
            """Texte vert si positif, rouge si négatif."""
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return ""
            if val > 0:   return f"color: {pos_color}; font-weight: 700"
            if val < 0:   return f"color: {neg_color}; font-weight: 700"
            return f"color: {neu_color}" if neu_color else ""

        def _color_var_j(val):
            return _color_signed(val)

        def _color_pru_pct(val):
            return _color_signed(val)

        def _color_sigma(val):
            if val is None or (isinstance(val, float) and np.isnan(val)):
                return ""
            if val > 1.25:   return "color: #FF4C4C; font-weight: 700"
            if val > 0.25:   return "color: #FFA07A; font-weight: 700"
            if val > -0.25:  return "color: #aaaaaa; font-weight: 700"
            if val > -1.25:  return "color: #90EE90; font-weight: 700"
            return "color: #3CB371; font-weight: 700"

        def _color_roic(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return ""
            if val > 15: return "color: #28a745"
            if val > 8:  return "color: #ffc107"
            return "color: #dc3545"

        def _color_pct_pos(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return ""
            if val > 10: return "color: #28a745"
            if val > 0:  return "color: #ffc107"
            return "color: #dc3545"

        def _color_marge(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return ""
            if val > 10: return "color: #28a745"
            if val > 5:  return "color: #ffc107"
            return "color: #dc3545"

        def _color_dette(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return ""
            if val < 3:  return "color: #28a745"
            if val < 5:  return "color: #ffc107"
            return "color: #dc3545"

        def _color_peg(val):
            if val is None or (isinstance(val, float) and np.isnan(val)): return ""
            if 0 < val < 2:  return "color: #28a745"
            if val <= 3:     return "color: #ffc107"
            return "color: #dc3545"

        # ── Styler Pandas sur les colonnes numériques ─────────
        df_styled = df_display.drop(columns=["📈"])

        styled = (
            df_styled.style
            .map(_color_var_j,    subset=["Var. J. (%)"])
            .map(_color_pru_pct,  subset=["+/- PRU (%)"])
            .map(_color_sigma,    subset=["σ Position"])
            .map(_color_roic,     subset=["ROIC (%)"])
            .map(_color_pct_pos,  subset=["Croiss. CA (%)"])
            .map(_color_marge,    subset=["Marge FCF (%)"])
            .map(_color_dette,    subset=["Dette/FCF"])
            .map(_color_peg,      subset=["PEG"])
        )

        # ── Affichage : checkbox dans colonne étroite + tableau coloré ──
        # Hauteur exacte : 35px par ligne + 38px header + 2px marge
        ROW_H      = 35
        HEADER_H   = 38
        TABLE_HEIGHT = len(df_display) * ROW_H + HEADER_H + 2

        st.caption(t("wl_analyse_tip"))

        col_chk, col_tbl = st.columns([1, 30])

        with col_chk:
            df_chk = df_display[["📈"]].copy()
            edited_main = st.data_editor(
                df_chk,
                use_container_width=True,
                hide_index=True,
                key="wl_main_editor",
                height=TABLE_HEIGHT,
                column_config={
                    "📈": st.column_config.CheckboxColumn("📈", help="Lancer l'analyse individuelle", width="small"),
                },
            )

        # Traitement de la colonne Analyser
        for idx_m, row_m in edited_main.iterrows():
            if bool(row_m["📈"]):
                st.session_state.page = t("page_analyse")
                st.session_state["individuel_prefill"] = df_display.loc[idx_m, "Ticker"]
                st.rerun()

        with col_tbl:
            st.dataframe(
                styled,
                use_container_width=True,
                hide_index=True,
                height=TABLE_HEIGHT,
                column_config={
                    "Ticker": st.column_config.TextColumn("Ticker", width="small", help="Symbole Yahoo Finance"),
                    "Société": st.column_config.TextColumn("Société", width="medium"),
                    "Prix": st.column_config.TextColumn("Prix", width="small", help="Cours actuel dans la devise native"),
                    "Prix EUR (€)": st.column_config.NumberColumn("Prix EUR (€)", format="%.2f €", width="small", help="Cours converti en EUR"),
                    "Var. J. (%)": st.column_config.NumberColumn("Var. J. (%)", format="%.2f%%", width="small", help="Variation journalière"),
                    "Score": st.column_config.TextColumn("Score", width="small", help="Score fondamental /10 (grade A→F)"),
                    "Croiss. CA (%)": st.column_config.NumberColumn("Croiss. CA (%)", format="%.1f%%", width="small"),
                    "ROIC (%)": st.column_config.NumberColumn("ROIC (%)", format="%.1f%%", width="small"),
                    "Marge FCF (%)": st.column_config.NumberColumn("Marge FCF (%)", format="%.1f%%", width="small"),
                    "Dette/FCF": st.column_config.NumberColumn("Dette/FCF", format="%.2f", width="small"),
                    "P/E": st.column_config.NumberColumn("P/E", format="%.1fx", width="small"),
                    "PEG": st.column_config.NumberColumn("PEG", format="%.2f", width="small"),
                    "σ Position": st.column_config.NumberColumn("σ Position", format="%.2fσ", width="small", help="Position sigma par rapport à la régression logarithmique"),
                    "PRU (€)": st.column_config.NumberColumn("PRU (€)", format="%.4f €", width="small", help="Prix de revient unitaire en EUR"),
                    "+/- PRU (%)": st.column_config.NumberColumn("+/- PRU (%)", format="%.1f%%", width="small", help="Écart entre le cours actuel (en €) et le PRU"),
                    "Ajouté le": st.column_config.TextColumn("Ajouté le", width="small"),
                    "Note": st.column_config.TextColumn("Note", width="medium"),
                },
            )

        st.markdown("")

        # ── Mini-graphique sigma (aperçu) ─────────────────────
        with st.expander("📈 Aperçu graphique de la watchlist (positions sigma)", expanded=False):
            sigma_vals = [(e["ticker"], e["sigma_pos"]) for e in rows_data if e["sigma_pos"] is not None]
            if sigma_vals:
                sigma_vals.sort(key=lambda x: x[1], reverse=True)
                tickers_s = [s[0] for s in sigma_vals]
                sigmas_s  = [s[1] for s in sigma_vals]
                colors_bar = []
                for s in sigmas_s:
                    if s > 1.25:   colors_bar.append("#FF4C4C")
                    elif s > 0.25: colors_bar.append("#FFA07A")
                    elif s > -0.25: colors_bar.append("#aaaaaa")
                    elif s > -1.25: colors_bar.append("#90EE90")
                    else:           colors_bar.append("#3CB371")

                fig_wl = go.Figure(go.Bar(
                    x=tickers_s, y=sigmas_s,
                    marker_color=colors_bar,
                    text=[f"{s:+.2f}σ" for s in sigmas_s],
                    textposition="outside",
                ))
                fig_wl.add_hline(y=0, line_color="#888", line_dash="dash")
                fig_wl.add_hrect(y0=1.75, y1=4, fillcolor="#FF4C4C", opacity=0.07, line_width=0)
                fig_wl.add_hrect(y0=-4, y1=-1.75, fillcolor="#3CB371", opacity=0.07, line_width=0)
                fig_wl.update_layout(
                    title=f"Position sigma — {wl_period_label}",
                    template="plotly_dark", height=340,
                    margin=dict(l=40,r=20,t=45,b=40),
                    yaxis=dict(title="σ", zeroline=True),
                    xaxis=dict(title=""),
                )
                st.plotly_chart(fig_wl, use_container_width=True)
            else:
                st.caption(t("wl_sigma_no_data"))

        # ── Mini-graphique +/- PRU (écart cours / prix d'achat) ──
        with st.expander("💰 Aperçu graphique de la watchlist (écart cours / PRU)", expanded=False):
            pru_vals = [
                (e["ticker"], e["pru_pct"])
                for e in rows_data
                if e["pru_pct"] is not None and e["prix_achat"]
            ]
            if pru_vals:
                pru_vals.sort(key=lambda x: x[1], reverse=True)
                tickers_p = [p[0] for p in pru_vals]
                pcts_p    = [p[1] for p in pru_vals]
                colors_p  = []
                for p in pcts_p:
                    if p >= 20:    colors_p.append("#28a745")
                    elif p >= 5:   colors_p.append("#4CE87A")
                    elif p >= 0:   colors_p.append("#90EE90")
                    elif p >= -10: colors_p.append("#FFA07A")
                    else:          colors_p.append("#dc3545")

                fig_pru = go.Figure(go.Bar(
                    x=tickers_p, y=pcts_p,
                    marker_color=colors_p,
                    text=[f"{p:+.1f}%" for p in pcts_p],
                    textposition="outside",
                ))
                fig_pru.add_hline(y=0, line_color="#888", line_dash="dash")
                fig_pru.add_hrect(y0=0, y1=200, fillcolor="#28a745", opacity=0.04, line_width=0)
                fig_pru.add_hrect(y0=-200, y1=0, fillcolor="#dc3545", opacity=0.04, line_width=0)
                fig_pru.update_layout(
                    title="Écart cours actuel (en €) / Prix d'achat PRU (en €)",
                    template="plotly_dark", height=340,
                    margin=dict(l=40, r=20, t=45, b=40),
                    yaxis=dict(title="%", zeroline=True, ticksuffix="%"),
                    xaxis=dict(title=""),
                )
                st.plotly_chart(fig_pru, use_container_width=True)
            else:
                st.caption(t("wl_pru_no_data"))

        st.divider()

        # ── Gestion avancée via data_editor avec checkboxes ───
        with st.expander("⚙️ Gestion de la watchlist", expanded=False):

            # Construire la liste pour le sélecteur de note/prix : "TICKER — Nom de l'action"
            wl_tickers_list = df_wl["ticker"].tolist()
            wl_display_list = []
            for e in rows_data:
                nom = e.get("name") or e.get("company") or e["ticker"]
                wl_display_list.append(f"{e['ticker']} — {nom}")

            wl_editor_data = []
            for e in rows_data:
                wl_editor_data.append({
                    "🗑️ Retirer":  False,
                    "Ticker":      e["ticker"],
                    "Société":     e.get("name") or e.get("company") or e["ticker"],
                })

            df_wl_editor = pd.DataFrame(wl_editor_data)
            st.caption(t("wl_remove_tip"))
            edited_wl = st.data_editor(
                df_wl_editor,
                use_container_width=True,
                hide_index=True,
                key="wl_mgmt_editor",
                column_order=["🗑️ Retirer", "Ticker", "Société"],
                column_config={
                    "🗑️ Retirer":  st.column_config.CheckboxColumn("🗑️ Retirer",  help="Cochez pour retirer de la watchlist", width="small"),
                    "Ticker":      st.column_config.TextColumn("Ticker",      disabled=True, width="small"),
                    "Société":     st.column_config.TextColumn("Société",     disabled=True),
                },
            )
            # Traitement des cases cochées
            for idx_e, row_e in edited_wl.iterrows():
                tkr_e = row_e["Ticker"]
                if bool(row_e["🗑️ Retirer"]):
                    remove_from_watchlist(tkr_e, name=st.session_state.active_watchlist)
                    st.toast(f"❌ {tkr_e} retiré de la watchlist", icon="🗑️")
                    st.rerun()

            st.markdown("")
            # ── Note / prix d'achat ────────────────────────────────
            with st.expander("✏️ Ajouter une note / prix d'achat", expanded=False):
                note_col1, note_col2 = st.columns([2, 3])
                with note_col1:
                    ticker_to_edit = st.selectbox(
                        "Action",
                        options=[t("wl_choose_dash")] + wl_display_list,
                        key="wl_edit_sel"
                    )
                with note_col2:
                    edit_note  = st.text_input("Note personnelle", key="wl_edit_note",  placeholder="ex : Position longue")
                    edit_price = st.text_input("Prix d'achat",     key="wl_edit_price", placeholder="ex : 178.50")
                    if st.button("💾 Enregistrer", key="wl_edit_btn", use_container_width=True):
                        if ticker_to_edit != t("wl_choose_dash"):
                            ticker_raw = ticker_to_edit.split(" — ")[0].strip()
                            df_edit = load_watchlist(st.session_state.active_watchlist)
                            mask = df_edit["ticker"].str.upper() == ticker_raw.upper()
                            if edit_note:
                                df_edit.loc[mask, "note"] = edit_note
                            if edit_price:
                                df_edit.loc[mask, "prix_achat"] = edit_price
                            save_watchlist(df_edit, name=st.session_state.active_watchlist)
                            st.success(t("wl_edit_ok"))
                            st.rerun()


# ============================================================
# PAGE 2 — COMPARAISON DE VALEURS
# ============================================================
elif current_page == t("page_comparaison"):
    st.title(t("comp_title"))

    select_all_options  = []
    all_tickers_options = []
    tickers_by_index    = {}

    for key, df_idx in all_data_extended.items():
        label            = get_label_extended(key)
        select_all_token = f"── Sélectionner tout : {label} ──"
        select_all_options.append(select_all_token)
        tickers_by_index[select_all_token] = []
        for _, row in df_idx.iterrows():
            opt = f"{row['Ticker']} — {row['Company']} [{label}]"
            all_tickers_options.append(opt)
            tickers_by_index[select_all_token].append(opt)

    all_tickers_options = sorted(set(all_tickers_options))
    full_options        = select_all_options + all_tickers_options

    with st.expander(t("comp_params_expander"), expanded=True):
        row1_col1, row1_col2 = st.columns([3, 1])
        with row1_col1:
            st.markdown(t("comp_select_label"))
            manual_tickers_raw = st.text_input("Tickers manuels (séparés par des virgules)", value="^GSPC, ^NDX",
                                               label_visibility="collapsed",
                                               placeholder=t("comp_ticker_ph"))
            manual_tickers = [t.strip().upper() for t in manual_tickers_raw.split(",") if t.strip()]
        with row1_col2:
            st.markdown(t("comp_period_label_bold"))
            period_label = st.selectbox(
                "Horizon temporel",
                options=list(_get_periods().keys()),
                index=2,
                key="comp_period",
                label_visibility="collapsed",
            )
            compare_period = _get_periods()[period_label]

        row2_col1, row2_col2 = st.columns([3, 1])
        with row2_col1:
            if full_options:
                selected_from_index = st.multiselect(
                    t("comp_add_from_index"), options=full_options, default=[],
                    help=t("comp_add_from_index_help")
                )
                expanded = []
                for sel in selected_from_index:
                    expanded.extend(tickers_by_index[sel]) if sel in tickers_by_index else expanded.append(sel)
                extra_tickers = [s.split(" — ")[0].strip() for s in list(dict.fromkeys(expanded))]
            else:
                extra_tickers = []

            compare_tickers = list(dict.fromkeys(manual_tickers + extra_tickers))
            if compare_tickers:
                st.caption(t("comp_selected_cap", tickers=", ".join(compare_tickers)))

        with row2_col2:
            st.markdown("<div style='height:26px'></div>", unsafe_allow_html=True)
            btn_compare = st.button(t("comp_btn"), use_container_width=True)

    st.divider()

    if btn_compare and compare_tickers:
        with st.spinner(t("comp_load_spinner")):
            price_data, errors = {}, []
            for tkr in compare_tickers:
                try:
                    h = get_history(tkr, compare_period)
                    if not h.empty: price_data[tkr] = h['Close']
                    else: errors.append(tkr)
                except: errors.append(tkr)

            if errors: st.warning(t("comp_errors", tickers=", ".join(errors)))

            if price_data:
                for tkr in list(price_data.keys()):
                    idx = price_data[tkr].index
                    if hasattr(idx,"tz") and idx.tz is not None:
                        price_data[tkr].index = idx.tz_localize(None)
                df_prices   = pd.DataFrame(price_data).dropna(how="all").dropna(axis=1, how="all")
                first_valid = df_prices.apply(lambda col: col.dropna().iloc[0] if len(col.dropna()) > 0 else 1)
                df_norm     = df_prices.div(first_valid)

                perf_rows = []
                for tkr in df_norm.columns:
                    series = df_norm[tkr].dropna()
                    if len(series) < 2: continue
                    perf_pct   = (series.iloc[-1] - 1) * 100
                    max_dd     = ((series / series.cummax()) - 1).min() * 100
                    volatility = series.pct_change().std() * np.sqrt(252) * 100
                    perf_rows.append({"Ticker": tkr, "Performance": f"{perf_pct:+.1f}%",
                                      "Volatilité ann.": f"{volatility:.1f}%", "Max Drawdown": f"{max_dd:.1f}%",
                                      "_perf_raw": perf_pct})
                df_perf = pd.DataFrame(perf_rows).sort_values("_perf_raw", ascending=False)

                st.session_state.comparaison_result = {
                    "df_norm":      df_norm,
                    "df_perf":      df_perf,
                    "period_label": period_label,
                    "tickers":      list(df_norm.columns),
                }
            else:
                st.error(t("comp_no_data"))
                st.session_state.comparaison_result = None

    res_c = st.session_state.comparaison_result
    if res_c is not None:
        df_norm      = res_c["df_norm"]
        df_perf      = res_c["df_perf"]
        period_label = res_c["period_label"]

        fig = go.Figure()
        colors = ["#4C9BE8","#E8834C","#4CE87A","#E84C4C","#A44CE8","#E8D14C","#4CE8D1","#E84CA4","#8BE84C","#4C4CE8"]
        for i, tkr in enumerate(df_norm.columns):
            fig.add_trace(go.Scatter(
                x=df_norm.index, y=df_norm[tkr], mode="lines", name=tkr,
                line=dict(color=colors[i%len(colors)], width=2),
                hovertemplate=f"<b>{tkr}</b><br>Date: %{{x|%d %b %Y}}<br>Perf: %{{y:.2%}}<extra></extra>"
            ))
        fig.add_hline(y=1.0, line_dash="dot", line_color="gray", opacity=0.5)
        fig.update_layout(
            title=dict(text=t("comp_chart_title", period=period_label), font_size=16),
            xaxis_title=t("comp_date_axis"), yaxis_title=t("comp_yaxis"), yaxis_tickformat=".0%",
            hovermode="x unified", legend=dict(orientation="v", x=1.02, y=1),
            height=480, margin=dict(l=50,r=120,t=50,b=50), template="plotly_dark",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader(t("comp_table_subtitle"))
        k1, k2, _ = st.columns([1,1,4])
        with k1: st.metric(t("comp_best"),   df_perf.iloc[0]["Ticker"], delta=f"{df_perf.iloc[0]['_perf_raw']:+.1f}%")
        with k2: st.metric(t("comp_worst"), df_perf.iloc[-1]["Ticker"], delta=f"{df_perf.iloc[-1]['_perf_raw']:+.1f}%")

        # ── Tableau interactif avec colonnes WL + Analyser ─────────
        _active_wl_comp = st.session_state.get("active_watchlist", "Ma Watchlist")
        wl_tickers_set_comp = set(load_watchlist(_active_wl_comp)["ticker"].str.upper().tolist())
        # Récupérer les noms des tickers depuis les infos yfinance (cache)
        comp_editor_data = []
        for _, perf_row in df_perf.iterrows():
            tkr_c = perf_row["Ticker"]
            try:
                _info_c = get_info(tkr_c)
                nom_c = _info_c.get("longName") or _info_c.get("shortName") or ""
            except:
                nom_c = ""
            comp_editor_data.append({
                "Ticker":          tkr_c,
                "Nom":             nom_c,
                "Performance":     perf_row["Performance"],
                "Volatilité ann.": perf_row["Volatilité ann."],
                "Max Drawdown":    perf_row["Max Drawdown"],
                "📈 Analyser":     False,
                "⭐ Watchlist":    tkr_c.upper() in wl_tickers_set_comp,
            })
        df_comp_editor = pd.DataFrame(comp_editor_data)
        st.caption(t("comp_tip_caption"))
        edited_comp = st.data_editor(
            df_comp_editor,
            use_container_width=True,
            hide_index=True,
            key="comp_editor",
            column_config={
                "Ticker":          st.column_config.TextColumn("Ticker", disabled=True, width="small"),
                "Nom":             st.column_config.TextColumn("Nom", disabled=True, width="medium"),
                "Performance":     st.column_config.TextColumn("Performance", disabled=True, width="small"),
                "Volatilité ann.": st.column_config.TextColumn("Volatilité ann.", disabled=True, width="small"),
                "Max Drawdown":    st.column_config.TextColumn("Max Drawdown", disabled=True, width="small"),
                "📈 Analyser":     st.column_config.CheckboxColumn("📈 Analyser", help="Lancer l'analyse individuelle", width="small"),
                "⭐ Watchlist":    st.column_config.CheckboxColumn("⭐ WL", help="Ajouter/retirer de la watchlist", width="small"),
            },
        )
        for _, row_c in edited_comp.iterrows():
            tkr_c = row_c["Ticker"]
            if bool(row_c["📈 Analyser"]):
                st.session_state.page = t("page_analyse")
                st.session_state["individuel_prefill"] = tkr_c
                st.rerun()

        _check_wl_toggle(edited_comp, "prev_wl_comp", "Ticker", "Nom", "⭐ Watchlist")
    else:
        if not btn_compare:
            st.info(t("comp_configure_msg"))


# ============================================================
# PAGE 3 — Screener Sigma
# ============================================================
elif current_page == t("page_screener_sigma"):
    st.title(t("screener_sigma_title"))

    with st.expander(t("screener_sigma_params"), expanded=True):
        left_col, right_col = st.columns([1, 1], gap="large")

        with left_col:
            sigma_period_label = st.selectbox(t("screener_sigma_period_label"), options=list(_get_periods().keys()), index=4, key="sigma_period")
            sigma_period = _get_periods()[sigma_period_label]

            sigma_index_options = [k for k in all_data_extended if not all_data_extended[k].empty]
            if sigma_index_options:
                sigma_index_labels = [get_label_extended(k) for k in sigma_index_options]
                sigma_index_label = st.selectbox(
                    t("screener_sigma_index_label"),
                    options=sigma_index_labels,
                    key="sigma_index",
                    help=t("screener_sigma_index_help")
                )
                sigma_index_key = sigma_index_options[sigma_index_labels.index(sigma_index_label)]
            else:
                st.warning(t("screener_no_index_warn"))
                sigma_index_key = None

        with right_col:
            st.markdown(t("screener_sigma_zones_title"))
            selected_criteria = []
            for idx_z, zone_key in enumerate(_get_sigma_criteria()):
                z_min, z_max, z_psycho = _get_sigma_criteria()[zone_key]
                if st.checkbox(zone_key, value=False, key=f"chk_{idx_z}", help=f"💬 {z_psycho}"):
                    selected_criteria.append(zone_key)

        with left_col:
            if selected_criteria:
                psycho_lines = [f"**{zk.split('(')[0].strip()}** : {SIGMA_CRITERIA[zk][2]}" for zk in selected_criteria]
                st.info("💬 " + "  \n".join(psycho_lines))
            btn_scan = st.button(t("screener_sigma_launch"), type="primary",
                                 disabled=(not selected_criteria or sigma_index_key is None))

    st.divider()

    if btn_scan and sigma_index_key and selected_criteria:
        df_index    = all_data_extended[sigma_index_key]
        total       = len(df_index)
        zones_label = ", ".join(zk.split("(")[0].strip() for zk in selected_criteria)
        st.info(t("screener_sigma_info", n=total, index=get_label_extended(sigma_index_key), period=sigma_period_label, zones=zones_label))

        progress_bar  = st.progress(0, text=t("screener_sigma_init"))
        results_found = []
        errors_scan   = []

        for i, row in df_index.iterrows():
            tkr, company = row["Ticker"], row["Company"]
            progress_bar.progress((list(df_index.index).index(i)+1)/total, text=t("screener_sigma_analyse_progress", ticker=tkr))
            try:
                hist = get_history(tkr, sigma_period)
                if hist.empty or len(hist) < 20: continue
                _, _, sigma_pos = compute_regression(hist)
                for zk in selected_criteria:
                    z_min, z_max, z_psycho = _get_sigma_criteria()[zk]
                    if z_min <= sigma_pos < z_max:
                        roe_sigma = None
                        try:
                            f_sigma = compute_fundamentals(tkr)
                            roe_sigma = f_sigma.get("roe")
                        except:
                            pass
                        results_found.append({"ticker": tkr, "company": company, "sigma_pos": sigma_pos,
                                              "hist": hist, "matched_zone": zk, "psycho": z_psycho,
                                              "roe": roe_sigma})
                        break
            except: errors_scan.append(tkr)

        progress_bar.empty()
        if errors_scan:
            st.caption(t("screener_sigma_errors_cap", tickers=", ".join(errors_scan[:10]) + ("…" if len(errors_scan)>10 else "")))

        if not results_found:
            st.warning(t("screener_sigma_no_result"))
            st.session_state.sigma_result = None
        else:
            results_found.sort(key=lambda r: r["sigma_pos"], reverse=True)
            st.session_state.sigma_result = {
                "results_found": results_found,
                "period_label":  sigma_period_label,
                "index_label":   get_label_extended(sigma_index_key),
            }

    res_s = st.session_state.sigma_result
    if res_s is not None:
        results_found = res_s["results_found"]
        period_lbl    = res_s["period_label"]

        st.success(t("screener_sigma_found", n=len(results_found), index=res_s["index_label"], period=period_lbl))

        def fmtv_s(v, u="", dec=1):
            return f"{v:.{dec}f}{u}" if v is not None and not (isinstance(v, float) and np.isnan(v)) else "N/A"

        # ── Tableau interactif avec colonnes d'action ──────────────
        _active_wl_sig = st.session_state.get("active_watchlist", "Ma Watchlist")
        wl_tickers_set_sig = set(load_watchlist(_active_wl_sig)["ticker"].str.upper().tolist())
        sig_editor_data = []
        for r in results_found:
            sig_editor_data.append({
                "Ticker":       r["ticker"],
                "Société":      r["company"],
                "Position σ":   f"{r['sigma_pos']:+.2f}σ",
                "ROE":          fmtv_s(r.get("roe"), "%"),
                "Zone":         r["matched_zone"].split("(")[0].strip(),
                "Psychologie":  r["psycho"],
                "📈 Analyser":  False,
                "⭐ WL":        r["ticker"].upper() in wl_tickers_set_sig,
            })
        df_sig_editor = pd.DataFrame(sig_editor_data)
        st.caption(t("screener_sigma_tip"))
        edited_sig = st.data_editor(
            df_sig_editor,
            use_container_width=True,
            hide_index=True,
            key="sig_editor",
            column_config={
                "Ticker":      st.column_config.TextColumn("Ticker", disabled=True, width="small"),
                t("screener_sigma_col_company"): st.column_config.TextColumn(t("screener_sigma_col_company"), disabled=True),
                t("screener_sigma_col_sigma"): st.column_config.TextColumn(t("screener_sigma_col_sigma"), disabled=True, width="small"),
                t("screener_sigma_col_roe"): st.column_config.TextColumn(t("screener_sigma_col_roe"), disabled=True, width="small"),
                t("screener_sigma_col_zone"): st.column_config.TextColumn(t("screener_sigma_col_zone"), disabled=True),
                t("screener_sigma_col_psycho"): st.column_config.TextColumn(t("screener_sigma_col_psycho"), disabled=True),
                "📈 Analyser": st.column_config.CheckboxColumn(t("screener_sigma_col_analyse"), help=t("screener_sigma_analyse_help"), width="small"),
                "⭐ WL":       st.column_config.CheckboxColumn(t("screener_sigma_col_wl"), help=t("screener_sigma_wl_help"), width="small"),
            },
        )
        # Traitement des cases cochées
        for idx_e, row_e in edited_sig.iterrows():
            tkr_e = row_e["Ticker"]
            if bool(row_e["📈 Analyser"]):
                st.session_state.page = t("page_analyse")
                st.session_state["individuel_prefill"] = tkr_e
                st.rerun()

        _check_wl_toggle(edited_sig, "prev_wl_sig", "Ticker", "Société", "⭐ WL")

        st.divider()
        st.subheader(t("screener_sigma_charts", n=len(results_found)))

        # ── Contrôles d'échelle pour les graphiques du screener sigma ──
        sig_ctrl1, sig_ctrl2, sig_ctrl3 = st.columns([2, 2, 6])
        with sig_ctrl1:
            sig_yaxis = st.radio(
                t("chart_yaxis_label"), [t("chart_yaxis_linear"), t("chart_yaxis_log")],
                index=0 if st.session_state.chart_yaxis_type == "linear" else 1,
                key="sig_yaxis_radio",
                help=t("chart_yaxis_help_short"),
            )
            st.session_state.chart_yaxis_type = "linear" if sig_yaxis == t("chart_yaxis_linear") else "log"
        with sig_ctrl2:
            _log_mode = (st.session_state.chart_yaxis_type == "log")
            sig_disp = st.radio(
                t("chart_display_label"), [t("chart_display_cours"), t("chart_display_pct")],
                index=0,  # forcé à Cours si log
                key="sig_disp_radio",
                help=t("chart_display_help_short"),
                disabled=_log_mode,
            )
            if _log_mode:
                st.session_state.chart_display_mode = "cours"
                st.caption(t("screener_sigma_log_cap"))
            else:
                st.session_state.chart_display_mode = "cours" if sig_disp == t("chart_display_cours") else "pct"

        for r in results_found:
            df_chart, _, sigma_pos_r = compute_regression(r["hist"])
            zone_short = r["matched_zone"].split("(")[0].strip()
            # Récupère le nom de la société si absent
            _sig_company = r["company"].strip() if r["company"] else ""
            if not _sig_company:
                try:
                    _sig_company = get_info(r["ticker"]).get("longName") or get_info(r["ticker"]).get("shortName") or ""
                except:
                    _sig_company = ""
            st.markdown(
                f'<div class="sigma-header"><h4>{r["ticker"]}'
                + (f' &nbsp;—&nbsp; {_sig_company}' if _sig_company else '')
                + f'</h4>'
                f'<span>Position : <b>{sigma_pos_r:+.2f}σ</b> &nbsp;|&nbsp; Zone : {zone_short} &nbsp;|&nbsp; Période : {period_lbl}</span><br>'
                f'<span style="color:#aad4f5;font-style:italic">💬 {r["psycho"]}</span></div>',
                unsafe_allow_html=True
            )
            _fig_sig = build_regression_chart(
                df_chart, r["ticker"], _sig_company,
                yaxis_type=st.session_state.chart_yaxis_type,
                display_mode=st.session_state.chart_display_mode,
            )
            # ── Ligne PRU ────────────────────────────────────────
            if st.session_state.chart_display_mode != "pct":
                _pru_sig = get_pru(r["ticker"])
                if _pru_sig is not None:
                    _fig_sig.add_hline(
                        y=_pru_sig,
                        line=dict(color="#4C9BE8", width=2),
                        annotation_text=f"PRU {_pru_sig:.2f}",
                        annotation_position="top left",
                        annotation_font=dict(color="#4C9BE8", size=11),
                    )
            st.plotly_chart(_fig_sig, use_container_width=True)
    else:
        if not btn_scan:
            st.info(t("screener_sigma_select_zone") if not selected_criteria else t("screener_sigma_click_to_start"))


# ============================================================
# PAGE 4 — SCREENER MULTI-CRITÈRES
# ============================================================
elif current_page == t("page_screener_multi"):
    st.title(t("screener_multi_title"))
    st.markdown(t("screener_multi_desc"))

    # ── CSS encarts avec bordure arrondie bleue ────────────────
    st.markdown("""
    <style>
    .scr-card {
        border: 2px solid #4C9BE8; border-radius: 14px;
        padding: 16px 18px; margin-bottom: 8px;
        background: linear-gradient(135deg,#0a1520 0%,#0d1b2a 100%);
    }
    .scr-card-title {
        font-size: 0.92rem; font-weight: 700; color: #7ab8e8;
        margin-bottom: 12px;
    }
    </style>
    """, unsafe_allow_html=True)

    with st.expander(t("screener_multi_params"), expanded=True):
        sc_col1, sc_col2, sc_col3 = st.columns([1, 1.2, 1.2], gap="medium")

        # ── Colonne 1 : Paramètres de scan ────────────────────────
        with sc_col1:
            st.markdown('<div class="scr-card"><div class="scr-card-title">🗂️ Paramètres de scan</div>', unsafe_allow_html=True)
            scr_period_label = st.selectbox(t("screener_multi_period_label"), options=list(_get_periods().keys()), index=4, key="scr_period")
            scr_period = _get_periods()[scr_period_label]

            scr_index_opts = [k for k in all_data_extended if not all_data_extended[k].empty]
            if scr_index_opts:
                scr_index_labels = [get_label_extended(k) for k in scr_index_opts]
                scr_index_label  = st.selectbox(
                    t("screener_multi_index_label"),
                    options=scr_index_labels,
                    key="scr_index",
                    help=t("screener_multi_index_help")
                )
                scr_index_key = scr_index_opts[scr_index_labels.index(scr_index_label)]
            else:
                st.warning(t("screener_multi_no_index"))
                scr_index_key = None
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
            btn_screener = st.button("🚀 Lancer le screener", type="primary",
                                     use_container_width=True,
                                     disabled=(scr_index_key is None))
            if st.button("↺ Réinitialiser les filtres", key="scr_reset",
                         use_container_width=True,
                         help=t("screener_multi_reset_help")):
                for k, v in [("f_rev", 5), ("f_roic", 10), ("f_fcfm", 5),
                              ("f_debt", 5), ("f_score", 5.0)]:
                    st.session_state[k] = v
                st.rerun()

        # ── Colonne 2 : Filtres fondamentaux ──────────────────────
        with sc_col2:
            st.markdown('<div class="scr-card"><div class="scr-card-title">📊 Filtres fondamentaux</div>', unsafe_allow_html=True)
            f_rev_min   = st.slider(t("screener_multi_rev_min"),  -20, 50,   5, key="f_rev")
            f_roic_min  = st.slider(t("screener_multi_roic_min"),             0, 40,  10, key="f_roic")
            f_fcfm_min  = st.slider(t("screener_multi_fcfm_min"),        0, 40,   5, key="f_fcfm")
            f_debt_max  = st.slider(t("screener_multi_debt_max"),             0, 20,   5, key="f_debt")
            f_score_min = st.slider(t("screener_multi_score_min"),  0.0, 10.0, 5.0, step=0.5, key="f_score")
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Colonne 3 : Filtres sigma ──────────────────────────────
        with sc_col3:
            st.markdown('<div class="scr-card"><div class="scr-card-title">🔭 Filtres sigma <em style="font-weight:400;font-size:0.8rem">(cochez les zones acceptées)</em></div>', unsafe_allow_html=True)
            scr_sigma_selected = []
            for idx_z, zone_key in enumerate(_get_sigma_criteria()):
                z_min, z_max, z_psycho = _get_sigma_criteria()[zone_key]
                short_label = zone_key.split("(")[0].strip() + (" (" + zone_key.split("(")[1] if "(" in zone_key else "")
                if st.checkbox(short_label, value=False, key=f"scr_chk_{idx_z}", help=f"💬 {z_psycho}"):
                    scr_sigma_selected.append(zone_key)
            if not scr_sigma_selected:
                st.caption(t("screener_multi_no_sigma"))
            st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    if btn_screener and scr_index_key:
        df_index = all_data_extended[scr_index_key]
        total    = len(df_index)
        st.info(t("screener_multi_info", n=total, index=get_label_extended(scr_index_key)))

        progress_bar = st.progress(0, text=t("screener_multi_init"))
        all_raw      = []   # toutes les données brutes, sans filtrage
        scr_errors   = []

        for i, row in df_index.iterrows():
            tkr, company = row["Ticker"], row["Company"]
            progress_bar.progress((list(df_index.index).index(i)+1)/total, text=t("screener_multi_progress", ticker=tkr))
            try:
                hist = get_history(tkr, scr_period)
                if hist.empty or len(hist) < 20: continue
                _, _, sigma_pos = compute_regression(hist)
                try:
                    f = compute_fundamentals(tkr)
                except: continue
                zone_matched = next(
                    (lbl for lbl, (mn, mx, _) in _get_sigma_criteria().items() if mn <= sigma_pos < mx), "N/A"
                )
                all_raw.append({
                    "ticker":     tkr, "company": company,
                    "sigma_pos":  sigma_pos, "zone": zone_matched,
                    "zone_short": zone_matched.split("(")[0].strip(),
                    "score":      f["score_10"], "grade": f["grade"],
                    "rev_growth": f["rev_growth"], "roic": f["roic"], "roe": f.get("roe"),
                    "fcf_margin": f["fcf_margin"], "debt_fcf": f["debt_fcf"],
                    "pe":         f["pe_ratio"], "peg": f["peg"],
                    "hist":       hist,
                })
            except: scr_errors.append(tkr)

        progress_bar.empty()
        if scr_errors:
            st.caption(t("screener_multi_errors_cap", tickers=", ".join(scr_errors[:10]) + ("…" if len(scr_errors)>10 else "")))

        st.session_state.screener_result = {
            "all_raw":      all_raw,
            "period_label": scr_period_label,
            "index_label":  get_label_extended(scr_index_key),
        }

    res_scr = st.session_state.screener_result
    if res_scr is not None:
        all_raw    = res_scr["all_raw"]
        period_lbl = res_scr["period_label"]

        # ── Filtrage en temps réel selon les curseurs et checkboxes ──
        scr_results = []
        for r in all_raw:
            sigma_pos = r["sigma_pos"]
            # Filtre sigma
            if scr_sigma_selected:
                sigma_ok = any(mn <= sigma_pos < mx for zk in scr_sigma_selected
                               for mn, mx, _ in [SIGMA_CRITERIA[zk]])
                if not sigma_ok: continue
            # Filtres fondamentaux
            if f_rev_min  > -20 and (r["rev_growth"] or -999) < f_rev_min:  continue
            if f_roic_min  > 0  and (r["roic"]       or -999) < f_roic_min: continue
            if f_fcfm_min  > 0  and r["fcf_margin"]            < f_fcfm_min: continue
            _debt = r["debt_fcf"]
            if f_debt_max < 20 and _debt is not None and _debt > 0 and _debt > f_debt_max: continue
            if f_score_min > 0  and r["score"]                 < f_score_min: continue
            scr_results.append(r)

        scr_results.sort(key=lambda r: (r["score"], r["sigma_pos"]), reverse=True)

        st.success(t("screener_multi_found", n=len(scr_results), index=res_scr["index_label"], period=period_lbl, total=len(all_raw)))

        def fmtv(v, u="", dec=1):
            return f"{v:.{dec}f}{u}" if v is not None and not (isinstance(v, float) and np.isnan(v)) else "N/A"

        # ── Tableau interactif avec colonnes d'action ──────────────
        _active_wl_scr = st.session_state.get("active_watchlist", "Ma Watchlist")
        wl_tickers_set_scr = set(load_watchlist(_active_wl_scr)["ticker"].str.upper().tolist())
        scr_editor_data = []
        for r in scr_results:
            scr_editor_data.append({
                "Ticker":       r["ticker"],
                "Société":      r["company"],
                "Score":        f"{r['score']:.1f}/10 ({r['grade']})",
                "Position σ":   f"{r['sigma_pos']:+.2f}σ",
                "Zone":         r["zone_short"],
                "Croiss. CA":   fmtv(r["rev_growth"], "%"),
                "ROIC":         fmtv(r["roic"],       "%"),
                "ROE":          fmtv(r.get("roe"),    "%"),
                "Marge FCF":    fmtv(r["fcf_margin"], "%"),
                "Dette/FCF":    fmtv(r["debt_fcf"]),
                "P/E":          fmtv(r["pe"],         "x"),
                "PEG":          fmtv(r["peg"]),
                "📈 Analyser":  False,
                "⭐ WL":        r["ticker"].upper() in wl_tickers_set_scr,
            })
        df_scr_editor = pd.DataFrame(scr_editor_data)
        st.caption(t("screener_sigma_tip"))
        edited_scr = st.data_editor(
            df_scr_editor,
            use_container_width=True,
            hide_index=True,
            key="scr_editor",
            column_config={
                "Ticker":      st.column_config.TextColumn("Ticker", disabled=True, width="small"),
                "Société":     st.column_config.TextColumn("Société", disabled=True),
                "Score":       st.column_config.TextColumn("Score", disabled=True, width="small"),
                "Position σ":  st.column_config.TextColumn("Position σ", disabled=True, width="small"),
                "Zone":        st.column_config.TextColumn("Zone", disabled=True),
                "Croiss. CA":  st.column_config.TextColumn("Croiss. CA", disabled=True, width="small"),
                "ROIC":        st.column_config.TextColumn("ROIC", disabled=True, width="small"),
                "ROE":         st.column_config.TextColumn("ROE", disabled=True, width="small"),
                "Marge FCF":   st.column_config.TextColumn("Marge FCF", disabled=True, width="small"),
                "Dette/FCF":   st.column_config.TextColumn("Dette/FCF", disabled=True, width="small"),
                "P/E":         st.column_config.TextColumn("P/E", disabled=True, width="small"),
                "PEG":         st.column_config.TextColumn("PEG", disabled=True, width="small"),
                "📈 Analyser": st.column_config.CheckboxColumn("📈 Analyser", help="Cochez pour lancer l'analyse de ce ticker", width="small"),
                "⭐ WL":       st.column_config.CheckboxColumn("⭐ WL", help="Ajouter/retirer de la watchlist", width="small"),
            },
        )
        # Traitement des cases cochées
        for idx_e, row_e in edited_scr.iterrows():
            tkr_e = row_e["Ticker"]
            if bool(row_e["📈 Analyser"]):
                st.session_state.page = t("page_analyse")
                st.session_state["individuel_prefill"] = tkr_e
                st.rerun()

        _check_wl_toggle(edited_scr, "prev_wl_scr", "Ticker", "Société", "⭐ WL")

        st.divider()
        st.subheader(t("screener_multi_charts_title", n=len(scr_results)))

        # ── Contrôles d'échelle pour les graphiques du screener multi ──
        scr_ctrl1, scr_ctrl2, scr_ctrl3 = st.columns([2, 2, 6])
        with scr_ctrl1:
            scr_yaxis = st.radio(
                t("chart_yaxis_label"), [t("chart_yaxis_linear"), t("chart_yaxis_log")],
                index=0 if st.session_state.chart_yaxis_type == "linear" else 1,
                key="scr_yaxis_radio",
                help=t("chart_yaxis_help_short"),
            )
            st.session_state.chart_yaxis_type = "linear" if scr_yaxis == t("chart_yaxis_linear") else "log"
        with scr_ctrl2:
            scr_disp = st.radio(
                t("chart_display_label"), [t("chart_display_cours"), t("chart_display_pct")],
                index=0 if st.session_state.chart_display_mode == "cours" else 1,
                key="scr_disp_radio",
                help=t("chart_display_help_scr"),
            )
            st.session_state.chart_display_mode = "cours" if scr_disp == t("chart_display_cours") else "pct"

        for r in scr_results:
            df_chart, _, sigma_pos_r = compute_regression(r["hist"])
            st.markdown(
                f'<div class="sigma-header"><h4>{r["ticker"]} &nbsp;—&nbsp; {r["company"]}'
                f' &nbsp; <span style="color:#FFD700">Score : {r["score"]:.1f}/10 ({r["grade"]})</span></h4>'
                f'<span>Position : <b>{sigma_pos_r:+.2f}σ</b> &nbsp;|&nbsp; Zone : {r["zone"]}'
                f' &nbsp;|&nbsp; ROIC: {fmtv(r["roic"],"%")} &nbsp;|&nbsp; Marge FCF: {fmtv(r["fcf_margin"],"%")}'
                f' &nbsp;|&nbsp; Période : {period_lbl}</span></div>',
                unsafe_allow_html=True
            )
            st.plotly_chart(
                build_regression_chart(
                    df_chart, r["ticker"], r["company"],
                    yaxis_type=st.session_state.chart_yaxis_type,
                    display_mode=st.session_state.chart_display_mode,
                ),
                use_container_width=True
            )
    else:
        if not btn_screener:
            st.info(t("screener_multi_configure_first"))


# ============================================================
# PAGE 5 — GUIDE DES INDICATEURS
# ============================================================
elif current_page == t("page_explications"):
    st.title(t("explain_title"))
    st.markdown(t("explain_subtitle"))

    st.markdown("""
    <style>
    .guide-card {
        background: linear-gradient(135deg, #0d1b2a 0%, #1a2a3a 100%);
        border-radius: 12px; padding: 16px 20px; margin-bottom: 14px;
        border-left: 4px solid #4C9BE8;
    }
    .guide-card.green  { border-left-color: #28a745; }
    .guide-card.yellow { border-left-color: #ffc107; }
    .guide-card.red    { border-left-color: #dc3545; }
    .guide-card.purple { border-left-color: #a855f7; }
    .guide-title   { font-size: 1.05rem; font-weight: 700; color: #e0e0e0; margin-bottom: 4px; }
    .guide-formula { font-size: 0.78rem; color: #aad4f5; font-family: monospace; margin-bottom: 6px; }
    .guide-body    { font-size: 0.88rem; color: #ccc; line-height: 1.5; }
    .guide-target  { display:inline-block; margin-top:6px; padding:2px 10px;
                     border-radius:20px; font-size:0.78rem; font-weight:600; }
    .tgt-green  { background:#1a4a2a; color:#4ade80; }
    .tgt-yellow { background:#3a2a00; color:#fbbf24; }
    .tgt-red    { background:#3a0a0a; color:#f87171; }
    </style>
    """, unsafe_allow_html=True)

    GUIDE_SECTIONS = [
        {
            "title": t("guide_growth_title"),
            "color": "green",
            "items": [
                {
                    "name": t("guide_rev_name"),
                    "formula": "CAGR = (CA_dernier / CA_premier) ^ (1/n) - 1",
                    "body": t("guide_rev_body"),
                    "target": t("guide_rev_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_rev_interp"),
                },
                {
                    "name": t("guide_fcf_name"),
                    "formula": "CAGR FCF = (FCF_dernier / FCF_premier) ^ (1/n) - 1",
                    "body": t("guide_fcf_body"),
                    "target": t("guide_fcf_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_fcf_interp"),
                },
            ],
        },
        {
            "title": t("guide_profit_title"),
            "color": "blue",
            "items": [
                {
                    "name": t("guide_roic_name"),
                    "formula": "ROIC = EBIT / (Total Assets - Current Liabilities) x 100",
                    "body": t("guide_roic_body"),
                    "target": t("guide_roic_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_roic_interp"),
                },
                {
                    "name": t("guide_roe_name"),
                    "formula": "ROE = Net Income / Equity x 100",
                    "body": t("guide_roe_body"),
                    "target": t("guide_roe_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_roe_interp"),
                },
                {
                    "name": t("guide_fcfm_name"),
                    "formula": "FCF Margin = Free Cash Flow / Revenue x 100",
                    "body": t("guide_fcfm_body"),
                    "target": t("guide_fcfm_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_fcfm_interp"),
                },
            ],
        },
        {
            "title": t("guide_valuation_title"),
            "color": "yellow",
            "items": [
                {
                    "name": t("guide_pe_name"),
                    "formula": "P/E = Price / EPS",
                    "body": t("guide_pe_body"),
                    "target": t("guide_pe_target"),
                    "tgt_cls": "tgt-yellow",
                    "interpretation": t("guide_pe_interp"),
                },
                {
                    "name": t("guide_peg_name"),
                    "formula": "PEG = P/E / Earnings Growth Rate (%)",
                    "body": t("guide_peg_body"),
                    "target": t("guide_peg_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_peg_interp"),
                },
                {
                    "name": t("guide_pb_name"),
                    "formula": "P/B = Market Cap / Net Book Value",
                    "body": t("guide_pb_body"),
                    "target": t("guide_pb_target"),
                    "tgt_cls": "tgt-yellow",
                    "interpretation": t("guide_pb_interp"),
                },
                {
                    "name": t("guide_pfcf_name"),
                    "formula": "P/FCF = Market Cap / Free Cash Flow",
                    "body": t("guide_pfcf_body"),
                    "target": t("guide_pfcf_target"),
                    "tgt_cls": "tgt-yellow",
                    "interpretation": t("guide_pfcf_interp"),
                },
            ],
        },
        {
            "title": t("guide_solidity_title"),
            "color": "red",
            "items": [
                {
                    "name": t("guide_debt_name"),
                    "formula": "Net Debt / FCF = (Total Debt - Cash) / Free Cash Flow",
                    "body": t("guide_debt_body"),
                    "target": t("guide_debt_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_debt_interp"),
                },
            ],
        },
        {
            "title": t("guide_sigma_title"),
            "color": "purple",
            "items": [
                {
                    "name": t("guide_sigma_name"),
                    "formula": "sigma_pos = (log(Price) - log(Regression)) / Std Dev",
                    "body": t("guide_sigma_body"),
                    "target": t("guide_sigma_target"),
                    "tgt_cls": "tgt-yellow",
                    "interpretation": t("guide_sigma_interp"),
                },
                {
                    "name": t("guide_score_name"),
                    "formula": "Score = Sum(validated criteria x weight) / Sum(total weights) x 10",
                    "body": t("guide_score_body"),
                    "target": t("guide_score_target"),
                    "tgt_cls": "tgt-green",
                    "interpretation": t("guide_score_interp"),
                },
            ],
        },
    ]

    st.markdown(t("explain_scoring_title"))


    for section in GUIDE_SECTIONS:
        with st.expander(section['title'], expanded=False):
            for item in section["items"]:
                color_cls = section["color"]
                st.markdown(
                    f'<div class="guide-card {color_cls}">'
                    f'<div class="guide-title">{item["name"]}</div>'
                    f'<div class="guide-formula">📐 {item["formula"]}</div>'
                    f'<div class="guide-body">{item["body"]}</div>'
                    f'<div class="guide-body" style="margin-top:6px;color:#aaa;">'
                    f'📊 <em>{item["interpretation"]}</em></div>'
                    f'<span class="guide-target {item["tgt_cls"]}">{t("guide_target_lbl")} {item["target"]}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
            st.markdown("")

    st.divider()
    st.markdown(t("guide_strategy_title") + "\n\n" + t("guide_strategy_body"))
    st.markdown(t("guide_strategy_screener"))

    st.divider()
    st.markdown(t("explain_valuation_title"))
    st.markdown(t("explain_valuation_intro"))
    st.markdown("")

    # ── Méthode 1 — DCF ─────────────────────────────────────────
    with st.expander(t("dcf_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card blue">'
            f'<div class="guide-title">{t("dcf_html_title")}</div>'
            f'<div class="guide-body">{t("dcf_html_concept")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="guide-card blue" style="margin-top:-14px;border-top:none;padding-top:8px;">'
            f'<div class="guide-formula">{t("dcf_html_formula_lbl")}</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            r"$$V_0 = \sum_{t=1}^{n} \frac{FCF_t}{(1+k)^t} + \frac{TV}{(1+k)^n}$$"
        )
        st.markdown(
            '<div class="guide-card blue" style="margin-top:-14px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("dcf_html_vars")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:8px;color:#aaa;">{t("dcf_html_usecase")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("dcf_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-green">{t("dcf_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    # ── Méthode 2 — Multiples ────────────────────────────────────
    with st.expander(t("multiples_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card yellow">'
            f'<div class="guide-title">{t("mult_html_title")}</div>'
            f'<div class="guide-body">{t("mult_html_concept")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="guide-card yellow" style="margin-top:-14px;border-top:none;padding-top:8px;">'
            f'<div class="guide-formula">{t("mult_html_formula_lbl")}</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            r"$$Prix_{estim\acute{e}} = Ratio_{moyen\ secteur} \times Indicateur_{entreprise}$$"
        )
        st.markdown(
            '<div class="guide-card yellow" style="margin-top:-14px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("mult_html_details")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:8px;color:#aaa;">{t("mult_html_usecase")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("mult_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-yellow">{t("mult_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    # ── Méthode 3 — Gordon-Shapiro ───────────────────────────────
    with st.expander(t("gordon_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card green">'
            f'<div class="guide-title">{t("gordon_html_title")}</div>'
            f'<div class="guide-body">{t("gordon_html_concept")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="guide-card green" style="margin-top:-14px;border-top:none;padding-top:8px;">'
            f'<div class="guide-formula">{t("gordon_html_formula_lbl")}</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            r"$$P = \frac{D_1}{k - g}$$"
        )
        st.markdown(
            '<div class="guide-card green" style="margin-top:-14px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("gordon_html_vars")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:8px;color:#aaa;">{t("gordon_html_usecase")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("gordon_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-green">{t("gordon_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    # ── Méthode 4 — ANR ─────────────────────────────────────────
    with st.expander(t("anr_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card red">'
            f'<div class="guide-title">{t("anr_html_title")}</div>'
            f'<div class="guide-body">{t("anr_html_concept")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            f'<div class="guide-card red" style="margin-top:-14px;border-top:none;padding-top:8px;">'
            f'<div class="guide-formula">{t("anr_html_formula_lbl")}</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            r"$$ANR = Actifs\ R\acute{e}els - Dettes\ Totales$$"
            "\n\n"
            r"$$Prix\ Juste \approx \frac{ANR}{Nombre\ d'actions}$$"
        )
        st.markdown(
            '<div class="guide-card red" style="margin-top:-14px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("anr_html_variants")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:8px;color:#aaa;">{t("anr_html_usecase")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("anr_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-red">{t("anr_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    # ── Tableau récapitulatif ────────────────────────────────────
    st.markdown("")
    st.markdown(
        f'<div class="guide-card" style="border-left-color:#4C9BE8;">'
        f'<div class="guide-title">{t("explain_model_table_title")}</div>'
        '</div>',
        unsafe_allow_html=True
    )
    st.markdown(t("explain_model_table"))
    st.markdown(
        f'<div class="guide-card" style="border-left-color:#4C9BE8;margin-top:0;">'
        f'<div class="guide-body" style="color:#aaa;">{t("explain_convergence_note")}'
        '</div>'
        '</div>',
        unsafe_allow_html=True
    )

    st.divider()
    st.markdown(t("explain_params_title"))
    st.markdown(t("explain_params_intro"))

    with st.expander(t("wacc_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card blue">'
            f'<div class="guide-title">{t("wacc_html_title")}</div>'
            f'<div class="guide-body">{t("wacc_html_body")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(r"$$WACC = \frac{E}{E+D} \cdot k_e + \frac{D}{E+D} \cdot k_d \cdot (1 - t)$$")
        st.markdown(
            '<div class="guide-card blue" style="margin-top:-12px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("wacc_html_vars")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:8px;">{t("wacc_html_ranges")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("wacc_html_auto")}</em>'
            '</div>'
            f'<span class="guide-target tgt-yellow">{t("wacc_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    with st.expander(t("g_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card green">'
            f'<div class="guide-title">{t("g_html_title")}</div>'
            f'<div class="guide-body">{t("g_html_body")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(r"$$TV = \frac{FCF_n \cdot (1+g)}{WACC - g}$$")
        st.markdown(
            '<div class="guide-card green" style="margin-top:-12px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("g_html_rules")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("g_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-green">{t("g_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    with st.expander(t("k_gordon_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card yellow">'
            f'<div class="guide-title">{t("k_html_title")}</div>'
            f'<div class="guide-body">{t("k_html_body")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(r"$$k = r_f + \beta \cdot (r_m - r_f)$$")
        st.markdown(
            '<div class="guide-card yellow" style="margin-top:-12px;border-top:none;border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("k_html_calibration")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("k_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-yellow">{t("k_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    with st.expander(t("horizon_expander"), expanded=False):
        st.markdown(
            f'<div class="guide-card purple">'
            f'<div class="guide-title">{t("horizon_html_title")}</div>'
            f'<div class="guide-body">{t("horizon_html_body")}'
            '</div>'
            '</div>',
            unsafe_allow_html=True
        )
        st.markdown(
            '<div class="guide-card purple" style="border-radius:0 0 12px 12px;padding-top:10px;">'
            f'<div class="guide-body">{t("horizon_html_reco")}'
            '</div>'
            f'<div class="guide-body" style="margin-top:6px;color:#aaa;"><em>{t("horizon_html_warning")}</em>'
            '</div>'
            f'<span class="guide-target tgt-green">{t("horizon_html_target")}</span>'
            '</div>',
            unsafe_allow_html=True
        )

    st.markdown(
        f'<div class="guide-card" style="border-left-color:#a855f7;margin-top:4px;">'
        f'<div class="guide-title">{t("sensitivity_title")}</div>'
        '</div>',
        unsafe_allow_html=True
    )
    st.markdown(t("sensitivity_table_md"))
    st.caption(t("sensitivity_caption"))


# ============================================================
# PAGE 6 — CONFIGURATION
# ============================================================
elif current_page == t("page_configuration"):
    st.title(t("config_title"))

    # ── Section publique : état des indices chargés ───────────
    st.subheader(t("config_indices_title"))

    if all_data:
        rows_main = []
        try:
            res_main = supabase.table("index_components").select("index_key").execute()
            if res_main.data:
                df_master = pd.DataFrame(res_main.data)
                summary_main = df_master.groupby("index_key").size().reset_index(name="Nb valeurs")
                for _, row in summary_main.iterrows():
                    rows_main.append({
                        "index_key":  row["index_key"],
                        "Indice":     get_label(row["index_key"]),
                        "Nb valeurs": int(row["Nb valeurs"]),
                        "Source":     "Wikipedia",
                        "badge":      "badge-wiki",
                        "badge_txt":  "📖 Wikipedia",
                    })
        except Exception:
            pass

        rows_custo = []
        try:
            res_custo = supabase.table("index_components_custom").select("index_key").execute()
            if res_custo.data:
                df_custo = pd.DataFrame(res_custo.data)
                summary_custo = df_custo.groupby("index_key").size().reset_index(name="Nb valeurs")
                for _, row in summary_custo.iterrows():
                    rows_custo.append({
                        "index_key":  row["index_key"],
                        "Indice":     get_label(row["index_key"]),
                        "Nb valeurs": int(row["Nb valeurs"]),
                        "Source":     "Custom",
                        "badge":      "badge-custom",
                        "badge_txt":  "🔧 Custom",
                    })
        except Exception:
            pass

        all_rows = rows_main + rows_custo
        if all_rows:
            rows_html = ""
            for r in all_rows:
                rows_html += (
                    f'<tr>'
                    f'<td><strong>{r["Indice"]}</strong></td>'
                    f'<td style="font-family:monospace;color:#aad4f5">{r["index_key"]}</td>'
                    f'<td style="text-align:center"><strong>{r["Nb valeurs"]}</strong></td>'
                    f'<td><span class="{r["badge"]}">{r["badge_txt"]}</span></td>'
                    f'</tr>'
                )
            st.markdown(
                f'<table class="config-table">'
                f'<thead><tr>'
                f'<th>Indice</th><th>Clé</th>'
                f'<th style="text-align:center">Nb valeurs</th><th>Source</th>'
                f'</tr></thead>'
                f'<tbody>{rows_html}</tbody>'
                f'</table>',
                unsafe_allow_html=True
            )
        st.markdown("")

        st.markdown("---")
        st.subheader(t("config_preview_title"))
        all_preview_options = ["-- Choisir un indice --"] + [get_label(k) for k in all_data]
        all_preview_keys    = [None] + list(all_data.keys())
        preview_label = st.selectbox(t("config_preview_select"), options=all_preview_options)
        preview_key   = all_preview_keys[all_preview_options.index(preview_label)]
        if preview_key:
            df_preview = all_data[preview_key].reset_index(drop=True)
            st.dataframe(df_preview, use_container_width=True, hide_index=True, height=400)
    else:
        st.info(t("config_no_index"))

    st.markdown("---")

    # ── Section admin protégée par mot de passe ───────────────
    st.subheader(t("config_admin_title"))

    # Vérification mot de passe admin (stocké dans st.secrets)
    _admin_ok = st.session_state.get("_admin_authenticated", False)

    if not _admin_ok:
        st.caption(t("config_admin_caption"))
        with st.form("form_admin_login", clear_on_submit=True):
            _pwd = st.text_input(t("config_admin_pwd_label"),
                                 type="password",
                                 placeholder="••••••••",
                                 label_visibility="collapsed")
            _submitted = st.form_submit_button("🔑 Accéder", type="primary")
            if _submitted:
                _expected = st.secrets.get("ADMIN_PASSWORD", "")
                if _pwd and _pwd == _expected:
                    st.session_state["_admin_authenticated"] = True
                    st.rerun()
                else:
                    st.error(t("config_admin_err"))
    else:
        # Bouton déconnexion admin
        col_admin_title, col_admin_logout = st.columns([4, 1])
        with col_admin_title:
            st.success(t("config_admin_ok"))
        with col_admin_logout:
            if st.button(t("config_admin_logout"), key="btn_admin_logout"):
                st.session_state["_admin_authenticated"] = False
                st.rerun()

        # ── Expander 1 : Mise à jour des indices Wikipedia ────
        with st.expander(t("config_wiki_expander"), expanded=False):
            st.markdown(t("config_wiki_desc"))
            col_upd1, col_upd2, col_upd3 = st.columns([2, 1, 3])
            with col_upd1:
                index_options   = ["-- Aucun --"] + [cfg["label"] for cfg in INDICES_CONFIG.values()]
                index_keys_list = [None]           + list(INDICES_CONFIG.keys())
                selected_label  = st.selectbox("Indice à mettre à jour",
                                               options=index_options, index=0, key="sb_indice")
                selected_index_key = index_keys_list[index_options.index(selected_label)]
            with col_upd2:
                st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
                btn_update = st.button(t("config_wiki_btn"), type="primary",
                                       help=t("config_wiki_btn_help"))
            with col_upd3:
                st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
                if selected_index_key:
                    url = INDICES_CONFIG[selected_index_key]["url"]
                    st.caption(f"🌐 Source : [{url}]({url})")

            if btn_update and selected_index_key is not None:
                with st.spinner(t("config_wiki_loading", label=INDICES_CONFIG[selected_index_key]["label"])):
                    try:
                        df_scraped = scrape_index(selected_index_key)
                        save_index_to_master_csv(selected_index_key, df_scraped)
                        st.success(
                            f"✅ {len(df_scraped)} composants sauvegardés "
                            f"pour {INDICES_CONFIG[selected_index_key]['label']}."
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(t("config_wiki_err", e=e))
            elif btn_update and selected_index_key is None:
                st.warning(t("config_wiki_select_first"))

        # ── Expander 2 : Informations système ─────────────────
        with st.expander(t("config_sys_expander"), expanded=False):
            wl_count = sum(len(load_watchlist(n)) for n in load_wl_index())
            wl_status = f"✅ {len(load_wl_index())} liste(s), {wl_count} entrée(s) au total"
            try:
                nb_comp  = supabase.table("index_components")                    .select("id", count="exact").execute().count or 0
                nb_cache = supabase.table("market_cache")                    .select("id", count="exact").execute().count or 0
                nb_sess  = supabase.table("usage_sessions")                    .select("id", count="exact").execute().count or 0
                nb_rat   = supabase.table("user_ratings")                    .select("id", count="exact").execute().count or 0
                nb_fb    = supabase.table("user_feedback")                    .select("id", count="exact").execute().count or 0
            except Exception:
                nb_comp = nb_cache = nb_sess = nb_rat = nb_fb = "?"
            st.code(
                f"Backend             : Supabase (PostgreSQL)\n"
                f"  Composants indices: {nb_comp} lignes\n"
                f"  Cache market      : {nb_cache} entrées\n"
                f"  Sessions          : {nb_sess} enregistrées\n"
                f"  Avis/notations    : {nb_rat}\n"
                f"  Suggestions       : {nb_fb}\n"
                f"  Watchlists        : {wl_status}\n"
                f"  User ID (vous)    : {get_user_id()}",
                language=""
            )
            if st.button(t("config_cache_btn"),
                         help=t("config_cache_help")):
                purge_old_cache(max_age_hours=2)
                st.success(t("config_cache_ok"))

        # ── Expander 3 : Messages privés ──────────────────────
        with st.expander(t("config_private_expander"), expanded=False):
            private_msgs = get_feedback_messages(limit=50, include_private=True)
            private_only = [m for m in private_msgs if m.get("is_private")]
            if private_only:
                st.caption(t("config_private_count", n=len(private_only)))
                for m in private_only:
                    try:
                        dt = datetime.fromisoformat(m["created_at"].replace("Z", "+00:00"))
                        date_str = dt.strftime("%d/%m/%Y %H:%M")
                    except Exception:
                        date_str = ""
                    st.markdown(
                        f'<div style="background:#1a0d2a;border-radius:8px;padding:10px 14px;'
                        f'margin-bottom:6px;border-left:3px solid #a855f7;">'
                        f'<div style="display:flex;justify-content:space-between;">'
                        f'<span style="color:#c084fc;font-size:0.72rem;">🔒 Privé · 📅 {date_str}</span>'
                        f'</div>'
                        f'<span style="color:#ddd;font-size:0.88rem;">{m["message"]}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
            else:
                st.caption(t("config_private_none"))

        # ── Expander 4 : Nettoyage watchlists inactives ───────
        with st.expander(t("config_purge_expander"), expanded=False):
            st.caption(
                "Supprime les watchlists (et leurs contenus) des utilisateurs "
                "qui ne se sont pas connectés depuis plus de 30 jours. "
                "⚠️ Opération irréversible."
            )
            col_purge1, col_purge2 = st.columns([2, 3])
            with col_purge1:
                if st.button(t("config_purge_btn"),
                             type="primary",
                             help=t("config_purge_help")):
                    nb = purge_inactive_watchlists(days=30)
                    if nb > 0:
                        st.success(t("config_purge_ok", n=nb))
                        st.rerun()
                    else:
                        st.info(t("config_purge_none"))
