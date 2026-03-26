# ============================================================
# SUPABASE — CONNEXION
# ============================================================
# Remplace toute la gestion CSV (watchlists, indices, cache yfinance)
# Les clés sont lues depuis st.secrets (Streamlit Cloud)
# ou depuis les variables d'environnement en local.
# ============================================================

import json
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
from supabase import create_client, Client

# ── Connexion (lue depuis st.secrets sur Streamlit Cloud) ──────
@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase()


# ============================================================
# IDENTIFIANT UTILISATEUR ANONYME
# ============================================================
# Sans authentification Google (phase 1), chaque visiteur reçoit
# un token unique stocké dans st.query_params.
# Ses watchlists sont isolées par ce token.
# ============================================================

def get_user_id() -> str:
    """
    Retourne l'identifiant unique de l'utilisateur courant.
    - Si un token existe dans l'URL  (?uid=xxxx) → on le réutilise
    - Sinon on en génère un nouveau et on le place dans l'URL
    L'utilisateur garde ses données tant qu'il conserve son URL/marque-page.
    """
    import uuid
    uid = st.query_params.get("uid", None)
    if not uid:
        uid = str(uuid.uuid4())
        st.query_params["uid"] = uid
    return uid


# ============================================================
# MULTI-WATCHLIST — FONCTIONS SUPABASE
# (remplacent toutes les fonctions CSV ci-dessous)
# ============================================================

WATCHLIST_COLS = ["ticker", "company", "ajout_date", "note", "prix_achat"]


def _get_wl_id(name: str, user_id: str | None = None) -> int | None:
    """Retourne l'id Supabase de la watchlist (user_id, name), ou None."""
    if user_id is None:
        user_id = get_user_id()
    res = (
        supabase.table("watchlists")
        .select("id")
        .eq("user_id", user_id)
        .eq("name", name)
        .execute()
    )
    if res.data:
        return res.data[0]["id"]
    return None


def load_wl_index(user_id: str | None = None) -> list[str]:
    """Retourne la liste des noms de watchlists de l'utilisateur."""
    if user_id is None:
        user_id = get_user_id()
    res = (
        supabase.table("watchlists")
        .select("name")
        .eq("user_id", user_id)
        .order("created_at")
        .execute()
    )
    names = [r["name"] for r in res.data] if res.data else []
    if not names:
        # Créer la watchlist par défaut au premier accès
        create_watchlist("Ma Watchlist", user_id=user_id)
        return ["Ma Watchlist"]
    return names


def create_watchlist(name: str, user_id: str | None = None) -> list[str]:
    """Crée une nouvelle watchlist. Retourne la liste mise à jour."""
    if user_id is None:
        user_id = get_user_id()
    existing = load_wl_index(user_id=user_id)
    if name not in existing:
        supabase.table("watchlists").insert(
            {"user_id": user_id, "name": name}
        ).execute()
        existing.append(name)
    return existing


def delete_watchlist(name: str, user_id: str | None = None) -> list[str]:
    """Supprime une watchlist et tous ses items (CASCADE en base)."""
    if user_id is None:
        user_id = get_user_id()
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id:
        supabase.table("watchlists").delete().eq("id", wl_id).execute()
    remaining = load_wl_index(user_id=user_id)
    return [n for n in remaining if n != name]


def load_watchlist(name: str | None = None, user_id: str | None = None) -> pd.DataFrame:
    """Charge le contenu d'une watchlist → DataFrame."""
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        return pd.DataFrame(columns=WATCHLIST_COLS)
    res = (
        supabase.table("watchlist_items")
        .select("ticker, company, ajout_date, note, prix_achat")
        .eq("watchlist_id", wl_id)
        .execute()
    )
    if not res.data:
        return pd.DataFrame(columns=WATCHLIST_COLS)
    df = pd.DataFrame(res.data)
    for col in WATCHLIST_COLS:
        if col not in df.columns:
            df[col] = ""
    return df[WATCHLIST_COLS].fillna("")


def save_watchlist(df: pd.DataFrame, name: str | None = None, user_id: str | None = None):
    """
    Remplace intégralement le contenu d'une watchlist.
    Utilisé lors de l'import Portfolio Performance ou d'une édition en masse.
    """
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    # Créer la watchlist si elle n'existe pas encore
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        create_watchlist(name, user_id=user_id)
        wl_id = _get_wl_id(name, user_id=user_id)
    # Supprimer tous les items existants
    supabase.table("watchlist_items").delete().eq("watchlist_id", wl_id).execute()
    # Réinsérer
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


def add_to_watchlist(ticker: str, company: str = "", name: str | None = None,
                     user_id: str | None = None) -> bool:
    """Ajoute un ticker. Retourne False si déjà présent."""
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    # Créer la watchlist si besoin
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id is None:
        create_watchlist(name, user_id=user_id)
        wl_id = _get_wl_id(name, user_id=user_id)
    # Vérifier doublon
    res = (
        supabase.table("watchlist_items")
        .select("id")
        .eq("watchlist_id", wl_id)
        .eq("ticker", ticker)
        .execute()
    )
    if res.data:
        return False  # déjà présent
    supabase.table("watchlist_items").insert({
        "watchlist_id": wl_id,
        "ticker":       ticker,
        "company":      company,
        "ajout_date":   datetime.now().strftime("%Y-%m-%d"),
        "note":         "",
        "prix_achat":   "",
    }).execute()
    return True


def remove_from_watchlist(ticker: str, name: str | None = None,
                          user_id: str | None = None):
    """Retire un ticker d'une watchlist."""
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    wl_id = _get_wl_id(name, user_id=user_id)
    if wl_id:
        supabase.table("watchlist_items").delete()\
            .eq("watchlist_id", wl_id).eq("ticker", ticker).execute()


def is_in_watchlist(ticker: str, name: str | None = None,
                    user_id: str | None = None) -> bool:
    """Retourne True si le ticker est dans la watchlist."""
    if user_id is None:
        user_id = get_user_id()
    if name is None:
        name = st.session_state.get("active_watchlist", load_wl_index(user_id=user_id)[0])
    ticker = ticker.strip().upper()
    wl_id = _get_wl_id(name, user_id=user_id)
    if not wl_id:
        return False
    res = (
        supabase.table("watchlist_items")
        .select("id")
        .eq("watchlist_id", wl_id)
        .eq("ticker", ticker)
        .execute()
    )
    return bool(res.data)


def get_pru(ticker: str, user_id: str | None = None) -> float | None:
    """Cherche le PRU dans toutes les watchlists. Retourne float ou None."""
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


def save_wl_index(names: list[str], user_id: str | None = None):
    """
    Compatibilité : synchronise la liste des watchlists.
    En pratique rarement appelé directement — préférer create/delete.
    """
    if user_id is None:
        user_id = get_user_id()
    existing = load_wl_index(user_id=user_id)
    for name in names:
        if name not in existing:
            create_watchlist(name, user_id=user_id)


# ============================================================
# INDICES BOURSIERS — FONCTIONS SUPABASE
# (remplacent load_all_indices, load_indices_list,
#  save_index_to_master_csv et les chemins CSV)
# ============================================================

def load_all_indices() -> dict[str, pd.DataFrame]:
    """
    Charge tous les composants d'indices depuis Supabase.
    Fusionne Wikipedia + custom (même logique qu'avec les CSV).
    """
    result = {}

    # ── Wikipedia ──────────────────────────────────────────────
    res = supabase.table("index_components")\
        .select("index_key, ticker, company").execute()
    if res.data:
        df = pd.DataFrame(res.data)
        df.columns = ["index_key", "Ticker", "Company"]
        for key in df["index_key"].unique():
            result[key] = df[df["index_key"] == key][["Ticker", "Company"]]\
                .reset_index(drop=True)

    # ── Custom ─────────────────────────────────────────────────
    res_c = supabase.table("index_components_custom")\
        .select("index_key, ticker, company").execute()
    if res_c.data:
        df_custo = pd.DataFrame(res_c.data)
        df_custo.columns = ["index_key", "Ticker", "Company"]
        for key in df_custo["index_key"].unique():
            df_key = df_custo[df_custo["index_key"] == key][["Ticker", "Company"]]\
                .reset_index(drop=True)
            if key in result:
                existing_tickers = set(result[key]["Ticker"].str.upper())
                df_new = df_key[~df_key["Ticker"].str.upper().isin(existing_tickers)]
                result[key] = pd.concat([result[key], df_new], ignore_index=True)
            else:
                result[key] = df_key

    return result


def load_indices_list() -> pd.DataFrame | None:
    """Charge la liste des grands indices libres (^GSPC, ^NDX…)."""
    res = supabase.table("index_list_custom")\
        .select("ticker, company").execute()
    if not res.data:
        return None
    df = pd.DataFrame(res.data)
    df.columns = ["Ticker", "Company"]
    return df.dropna().reset_index(drop=True)


def save_index_to_master_csv(index_key: str, df_new: pd.DataFrame):
    """
    Remplace save_index_to_master_csv.
    Upsert les composants d'un indice dans Supabase (Wikipedia source).
    """
    # Supprimer les anciens composants de cet indice
    supabase.table("index_components")\
        .delete().eq("index_key", index_key).execute()
    # Insérer les nouveaux
    rows = [
        {
            "index_key": index_key,
            "ticker":    str(row["Ticker"]).strip(),
            "company":   str(row["Company"]).strip(),
            "source":    "wikipedia",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for _, row in df_new.iterrows()
    ]
    if rows:
        # Insérer par lots de 500 pour éviter les limites Supabase
        for i in range(0, len(rows), 500):
            supabase.table("index_components").insert(rows[i:i+500]).execute()


# ============================================================
# CACHE YFINANCE PARTAGÉ — SUPABASE
# (remplace les @st.cache_data individuels par session)
# ============================================================
# TTL par type de donnée :
#   Historique cours    : 60 min
#   Info fondamentaux   : 60 min
#   Quote live          : 2 min
#   Taux de change      : 60 min
# ============================================================

_CACHE_TTL = {
    "history":    60,   # minutes
    "info":       60,
    "financials": 60,
    "live":        2,
    "fx":         60,
}


def _cache_get(cache_key: str, ttl_minutes: int):
    """
    Cherche une entrée dans market_cache.
    Retourne les données parsées si fraîches, None sinon.
    """
    try:
        res = supabase.table("market_cache")\
            .select("data_json, updated_at")\
            .eq("ticker", cache_key)\
            .eq("period", "meta")\
            .execute()
        # Note : on réutilise la colonne "period" comme discriminant de type
    except Exception:
        return None

    if not res.data:
        return None

    row = res.data[0]
    updated = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - updated).total_seconds() / 60
    if age > ttl_minutes:
        return None
    try:
        return json.loads(row["data_json"])
    except Exception:
        return None


def _cache_set(cache_key: str, data):
    """Stocke ou met à jour une entrée dans market_cache."""
    try:
        supabase.table("market_cache").upsert({
            "ticker":     cache_key,
            "period":     "meta",
            "data_json":  json.dumps(data, default=str),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass  # Le cache est best-effort, on ne bloque jamais l'app


def _history_cache_key(ticker: str, period: str) -> str:
    return f"history|{ticker}|{period}"


def _history_cache_get(ticker: str, period: str, ttl_minutes: int = 60):
    """Cherche un historique de cours dans le cache Supabase."""
    key = _history_cache_key(ticker, period)
    try:
        res = supabase.table("market_cache")\
            .select("data_json, updated_at")\
            .eq("ticker", ticker)\
            .eq("period", period)\
            .execute()
    except Exception:
        return None
    if not res.data:
        return None
    row = res.data[0]
    # Vérifier la fraîcheur
    updated = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - updated).total_seconds() / 60
    if age > ttl_minutes:
        return None
    try:
        data = json.loads(row["data_json"])
        df = pd.DataFrame(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        return df
    except Exception:
        return None


def _history_cache_set(ticker: str, period: str, df: pd.DataFrame):
    """Stocke un historique de cours dans le cache Supabase."""
    try:
        df_reset = df.reset_index()
        data_json = df_reset.to_json(date_format="iso")
        supabase.table("market_cache").upsert({
            "ticker":     ticker,
            "period":     period,
            "data_json":  data_json,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass


# ── Fonctions publiques (mêmes signatures qu'avant) ────────────

def get_history(ticker: str, period: str) -> pd.DataFrame:
    """Historique cours — cache partagé 60 min."""
    cached = _history_cache_get(ticker, period, ttl_minutes=60)
    if cached is not None:
        return cached
    df = yf.Ticker(ticker).history(period=period)
    if not df.empty:
        _history_cache_set(ticker, period, df)
    return df


# Contraintes période max par intervalle intraday (inchangé)
_INTRADAY_MAX_PERIOD = {
    "1m":  "7d",  "2m":  "60d", "5m":  "60d",
    "15m": "60d", "30m": "60d", "60m": "730d", "1h": "730d",
}

@st.cache_data(ttl=300, show_spinner=False)
def get_history_intraday(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """Intraday — cache session Streamlit 5 min (volume faible, pas en base)."""
    return yf.Ticker(ticker).history(period=period, interval=interval)


def get_info(ticker: str) -> dict:
    """Infos fondamentales — cache partagé 60 min."""
    cache_key = f"info|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["info"])
    if cached is not None:
        return cached
    info = yf.Ticker(ticker).info
    _cache_set(cache_key, info)
    return info


def get_financials(ticker: str):
    """Financials — cache session Streamlit 60 min (DataFrames complexes)."""
    return _get_financials_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_financials_cached(ticker: str):
    t = yf.Ticker(ticker)
    return t.financials, t.balance_sheet, t.cashflow


def get_recommendations(ticker: str):
    """Recommandations analystes — cache session 60 min."""
    return _get_recommendations_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_recommendations_cached(ticker: str):
    try:
        t = yf.Ticker(ticker)
        rec = t.recommendations
        if rec is not None and not rec.empty:
            return rec
    except Exception:
        pass
    return None


def get_calendar(ticker: str):
    """Calendrier dividende/earnings — cache session 60 min."""
    return _get_calendar_cached(ticker)

@st.cache_data(ttl=3600, show_spinner=False)
def _get_calendar_cached(ticker: str):
    try:
        return yf.Ticker(ticker).calendar
    except Exception:
        return None


def get_live_quote(ticker: str) -> dict | None:
    """Quote live — cache partagé 2 min."""
    cache_key = f"live|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["live"])
    if cached is not None:
        return cached
    try:
        t = yf.Ticker(ticker)
        info = t.info
        price = info.get("currentPrice") or info.get("regularMarketPrice")
        prev  = info.get("previousClose") or info.get("regularMarketPreviousClose")
        name  = info.get("longName") or info.get("shortName") or ticker
        curr  = info.get("currency", "")
        if price and prev and prev != 0:
            change_pct = (price - prev) / prev * 100
        else:
            change_pct = None
        result = {"name": name, "price": price, "change_pct": change_pct, "currency": curr}
        _cache_set(cache_key, result)
        return result
    except Exception:
        return None


def get_ticker_currency(ticker: str) -> str:
    """Devise du ticker — cache partagé 60 min."""
    cache_key = f"currency|{ticker}"
    cached = _cache_get(cache_key, _CACHE_TTL["fx"])
    if cached is not None:
        return cached if isinstance(cached, str) else "EUR"
    try:
        info = yf.Ticker(ticker).info
        curr = info.get("currency", "EUR") or "EUR"
        _cache_set(cache_key, curr)
        return curr
    except Exception:
        return "EUR"


def get_eur_to_currency_rate(target_currency: str) -> float:
    """Taux de change EUR→devise — cache partagé 60 min."""
    if not target_currency or target_currency.upper() == "EUR":
        return 1.0
    cache_key = f"fx|EUR{target_currency.upper()}"
    cached = _cache_get(cache_key, _CACHE_TTL["fx"])
    if cached is not None:
        return float(cached)
    try:
        symbol = f"EUR{target_currency.upper()}=X"
        t = yf.Ticker(symbol)
        info = t.info
        rate = info.get("regularMarketPrice") or info.get("currentPrice")
        if not rate or rate <= 0:
            hist = t.history(period="5d")
            if not hist.empty:
                rate = float(hist["Close"].iloc[-1])
        if rate and rate > 0:
            _cache_set(cache_key, rate)
            return float(rate)
    except Exception:
        pass
    return 1.0


# ============================================================
# NETTOYAGE CACHE PÉRIMÉ (optionnel — appeler depuis ⚙️ Config)
# ============================================================

def purge_old_cache(max_age_hours: int = 2):
    """
    Supprime les entrées de market_cache plus vieilles que max_age_hours.
    À appeler occasionnellement depuis la page Configuration.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
        supabase.table("market_cache").delete().lt("updated_at", cutoff).execute()
    except Exception:
        pass


# ============================================================
# SUPPRESSION DES ANCIENS CHEMINS CSV
# (ces lignes remplacent le bloc CONFIGURATION de l'original)
# ============================================================
# Les variables suivantes ne sont plus nécessaires mais sont
# conservées comme stubs pour éviter des NameError si elles
# sont référencées ailleurs dans le script.
# ============================================================

ASSETS_DIR      = ""   # supprimé — Supabase
WATCHLIST_DIR   = ""   # supprimé — Supabase
EXPORTS_DIR     = ""   # supprimé — Supabase
COMPONENTS_CSV  = ""   # supprimé — Supabase
INDICES_LIST_CSV= ""   # supprimé — Supabase
CUSTO_CSV       = ""   # supprimé — Supabase
WATCHLIST_CSV   = ""   # supprimé — Supabase
WATCHLIST_INDEX = ""   # supprimé — Supabase
