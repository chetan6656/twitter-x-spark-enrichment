"""Pure Spark-side candidate extraction and scoring helpers.

This module intentionally has no Spark, boto3, or network imports.  The rules
mirror ``src/enrich_segment5_twitter.py`` so they can be unit-tested and used
from a Spark UDF or a local validation job.
"""

import re
import unicodedata


RESERVED_HANDLES = {
    "home", "explore", "search", "i", "intent", "share", "hashtag",
    "settings", "notifications", "messages", "compose", "login", "signup",
    "about", "tos", "privacy", "help", "status", "download", "signin",
    "logout", "account", "widgets", "welcome", "oauth", "jobs", "press",
    "developer", "en", "es",
}
PROFILE_RE = re.compile(
    r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})(?:[/?#]|$)",
    re.I,
)
CONFIDENCE_RANK = {"very_high": 4, "high": 3, "medium": 2, "low": 1}


def clean(value):
    if value is None:
        return ""
    value = str(value).strip()
    return "" if value.lower() in {"nan", "none", "null", "n/a", "na", "-"} else value


def norm(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def compact(value):
    return norm(value).replace(" ", "")


def score_candidate(row, handle, title, snippet):
    """Return the baseline confidence tier without changing its rules."""
    first, last = norm(row.get("ic_fname")), norm(row.get("ic_lname"))
    company, location = norm(row.get("ic_company")), norm(row.get("Location"))
    text = norm(f"{title} {snippet}")
    handle_lower = handle.lower()
    handle_tokens = norm(re.sub(r"(?<=[a-z])(?=[A-Z])", " ", handle))
    last_compact, first_compact = last.replace(" ", ""), first.replace(" ", "")

    name_in_text = bool(first and last) and first in text and last in text
    name_in_handle = bool(last) and (
        last_compact in handle_lower
        or last in handle_tokens
        or (first and (first_compact[:1] + last_compact) in handle_lower)
        or (first and first_compact in handle_lower and last_compact in handle_lower)
    )
    company_hit = bool(company) and any(t in text for t in company.split() if len(t) > 3)
    location_hit = bool(location) and any(t in text for t in location.split() if len(t) > 3)
    company_compact = compact(company)
    is_brand_account = (
        len(company_compact) > 3 and company_compact in handle_lower and not name_in_handle
    )
    if is_brand_account:
        return "low"
    if name_in_text and name_in_handle and (company_hit or location_hit):
        return "very_high"
    if name_in_text and (name_in_handle or company_hit):
        return "high"
    if name_in_handle and (company_hit or location_hit):
        return "high"
    if name_in_text or name_in_handle:
        return "medium"
    return "low"


def candidates_from_payload(row, payload, matched_query):
    """Extract every valid profile candidate, strongest first."""
    if not isinstance(payload, dict):
        return []
    candidates = []
    for item in payload.get("organic", []):
        link = clean(item.get("link"))
        if "/status/" in link.lower() or "/i/" in link.lower():
            continue
        match = PROFILE_RE.search(link)
        if not match or match.group(1).lower() in RESERVED_HANDLES:
            continue
        handle = match.group(1)
        title, snippet = clean(item.get("title")), clean(item.get("snippet"))
        candidates.append({
            "username": handle,
            "handle": "@" + handle,
            "profile_url": "https://x.com/" + handle,
            "confidence": score_candidate(row, handle, title, snippet),
            "matched_query": matched_query,
            "evidence": f"{title} | {snippet}"[:300],
        })
    return sorted(candidates, key=lambda c: CONFIDENCE_RANK[c["confidence"]], reverse=True)


def best_candidate(row, payloads):
    """Select the best candidate across ``{query: payload}`` responses."""
    best = None
    for query, payload in payloads.items():
        for candidate in candidates_from_payload(row, payload, query):
            if best is None or CONFIDENCE_RANK[candidate["confidence"]] > CONFIDENCE_RANK[best["confidence"]]:
                best = candidate
    return best


def final_fields(candidate, verified="unverified"):
    """Produce the required final and QA fields; untrusted candidates are blank."""
    candidate = candidate or {}
    trusted = (
        candidate.get("confidence") in {"very_high", "high"}
        and verified != "rejected"
    )
    username = clean(candidate.get("username")) if trusted else ""
    return {
        "twitter_username_final": username,
        "twitter_handle_final": "@" + username if username else "",
        "twitter_profile_url_final": "https://x.com/" + username if username else "",
        "twitter_confidence": candidate.get("confidence", "not_found") if candidate else "not_found",
        "twitter_verified": verified if candidate else "",
        "twitter_match_evidence": candidate.get("evidence", "") if candidate else "",
        "twitter_matched_query": candidate.get("matched_query", "") if candidate else "",
        "twitter_display_name": candidate.get("display_name", "") if candidate else "",
        "twitter_followers": candidate.get("followers", "") if candidate else "",
    }
