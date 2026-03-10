#!/usr/bin/env python3
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import email.utils
import hashlib
import html
import json
import logging
import re
import sqlite3
import sys
import textwrap
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Iterable

import requests
import yaml


LOG = logging.getLogger("sponsor_watch")
USER_AGENT = "sponsor-watch/1.0 (+https://github.com/)"
DEFAULT_TIMEOUT = 20
MORTGAGE_KEYWORDS = (
    "mortgage",
    "broker",
    "brokerage",
    "loan",
    "lender",
    "lending",
    "wholesale",
    "origination",
    "home loan",
    "loan officer",
    "housing",
)
HTML_LINK_RE = re.compile(r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>", re.I | re.S)
HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class SponsorWatchError(RuntimeError):
    pass


@dataclasses.dataclass
class Source:
    url: str
    kind: str = "rss"


@dataclasses.dataclass
class Company:
    name: str
    list_name: str
    aliases: list[str]
    official_domains: list[str]
    official_sources: list[Source]
    strict_google_match: bool = False

    @property
    def search_terms(self) -> list[str]:
        return [self.name, *self.aliases]


@dataclasses.dataclass
class Article:
    company: Company
    list_name: str
    title: str
    url: str
    summary: str
    published: str | None
    source_label: str
    source_url: str
    is_official: bool
    dedupe_key: str


class DedupeStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_items (
                dedupe_key TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                list_name TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                seen_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def already_seen(self, dedupe_key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM seen_items WHERE dedupe_key = ? LIMIT 1",
            (dedupe_key,),
        ).fetchone()
        return row is not None

    def mark_seen(self, article: Article) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO seen_items
            (dedupe_key, company_name, list_name, title, url, seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                article.dedupe_key,
                article.company.name,
                article.list_name,
                article.title,
                article.url,
                dt.datetime.now(dt.timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class RoamClient:
    def __init__(self, base_url: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            }
        )

    def post_message(self, chat_id: str, text: str) -> dict[str, Any]:
        payload = {"chat_id": chat_id, "text": text}
        response = self.session.post(
            f"{self.base_url}/chat.post",
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def list_chats(self) -> list[dict[str, Any]]:
        response = self.session.get(f"{self.base_url}/chat.list", timeout=self.timeout)
        response.raise_for_status()
        data = response.json() if response.content else {}
        chats = data.get("chats")
        return chats if isinstance(chats, list) else []

    def token_info(self) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/token.info", timeout=self.timeout)
        response.raise_for_status()
        return response.json() if response.content else {}

    def resolve_chat_id(self, configured_value: str) -> str:
        if configured_value.startswith(("C-", "G-", "D-")) or UUID_RE.match(configured_value):
            return configured_value
        target = configured_value.strip().lower()
        for chat in self.list_chats():
            candidates = {
                str(chat.get("id", "")).lower(),
                str(chat.get("name", "")).lower(),
                str(chat.get("address", "")).lower(),
                str(chat.get("display_name", "")).lower(),
            }
            if target in candidates:
                return str(chat["id"])
        raise SponsorWatchError(f"Unable to resolve Roam chat: {configured_value}")


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise SponsorWatchError(f"Expected a mapping in {path}")
    return data


def normalize_company_name(value: str) -> str:
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return WHITESPACE_RE.sub(" ", value).strip()


def load_companies(path: Path) -> dict[str, list[Company]]:
    data = load_yaml(path)
    watchlists = data.get("watchlists")
    if not isinstance(watchlists, dict):
        raise SponsorWatchError("companies.yaml must define watchlists")
    companies_by_list: dict[str, list[Company]] = {}
    for list_name, entries in watchlists.items():
        if not isinstance(entries, list):
            raise SponsorWatchError(f"watchlists.{list_name} must be a list")
        companies: list[Company] = []
        for entry in entries:
            if isinstance(entry, str):
                name = entry
                aliases: list[str] = []
                official_domains: list[str] = []
                strict_google_match = False
                official_sources: list[Source] = []
            elif isinstance(entry, dict):
                name = str(entry["name"])
                aliases = [str(item) for item in entry.get("aliases", [])]
                official_domains = [str(item).lower() for item in entry.get("official_domains", [])]
                strict_google_match = bool(entry.get("strict_google_match", False))
                official_sources = [
                    Source(url=str(item["url"]), kind=str(item.get("kind", "rss")))
                    for item in entry.get("official_sources", [])
                ]
            else:
                raise SponsorWatchError(f"Unsupported company entry under {list_name}: {entry!r}")
            companies.append(
                Company(
                    name=name,
                    list_name=list_name,
                    aliases=aliases,
                    official_domains=official_domains,
                    official_sources=official_sources,
                    strict_google_match=strict_google_match,
                )
            )
        companies_by_list[list_name] = companies
    return companies_by_list


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor sponsor watchlists and post Roam alerts.")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--companies",
        default="companies.yaml",
        help="Path to watchlist YAML (default: companies.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run", help="Scan both watchlists and post any new alerts to Roam.")
    subparsers.add_parser("test-vendors", help="Scan vendors and print alerts without posting.")
    subparsers.add_parser("test-lenders", help="Scan lenders and print alerts without posting.")
    return parser.parse_args(argv)


def make_session(config: dict[str, Any]) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    timeout = int(config.get("request_timeout_seconds", DEFAULT_TIMEOUT))
    retries = int(config.get("request_retries", 2))
    session.request_timeout = timeout  # type: ignore[attr-defined]
    session.request_retries = retries  # type: ignore[attr-defined]
    return session


def request_text(session: requests.Session, url: str) -> str:
    last_error: Exception | None = None
    retries = getattr(session, "request_retries", 2)
    timeout = getattr(session, "request_timeout", DEFAULT_TIMEOUT)
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(1 + attempt)
    assert last_error is not None
    raise last_error


def normalize_url(url: str, source_url: str | None = None) -> str:
    resolved = urllib.parse.urljoin(source_url or "", url)
    parsed = urllib.parse.urlparse(resolved)
    query = urllib.parse.parse_qs(parsed.query)
    if "url" in query and query["url"]:
        candidate = query["url"][0]
        if candidate.startswith("http"):
            resolved = candidate
            parsed = urllib.parse.urlparse(resolved)
    clean_query = urllib.parse.urlencode(
        [(key, value) for key, values in urllib.parse.parse_qs(parsed.query).items() for value in values if not key.startswith("utm_")]
    )
    parsed = parsed._replace(query=clean_query, fragment="")
    return parsed.geturl()


def parse_isoish_date(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    for parser in (
        dt.datetime.fromisoformat,
        lambda item: email.utils.parsedate_to_datetime(item),
    ):
        try:
            parsed = parser(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc).date().isoformat()
        except (TypeError, ValueError, IndexError):
            continue
    return raw[:10]


def strip_html(value: str) -> str:
    value = html.unescape(value or "")
    value = HTML_TAG_RE.sub(" ", value)
    return WHITESPACE_RE.sub(" ", value).strip()


def short_summary(value: str, length: int = 220) -> str:
    cleaned = strip_html(value)
    if len(cleaned) <= length:
        return cleaned
    return cleaned[: length - 1].rstrip() + "…"


def dedupe_key(company: Company, title: str, url: str) -> str:
    slug = f"{normalize_company_name(company.name)}|{normalize_url(url)}|{normalize_company_name(title)}"
    return hashlib.sha256(slug.encode("utf-8")).hexdigest()


def official_domain_match(company: Company, url: str) -> bool:
    if not company.official_domains:
        return True
    host = urllib.parse.urlparse(url).netloc.lower()
    return any(host == domain or host.endswith(f".{domain}") for domain in company.official_domains)


def strict_google_match(company: Company, title: str, summary: str) -> bool:
    haystack = normalize_company_name(f"{title} {summary}")
    exact_terms = [normalize_company_name(term) for term in company.search_terms]
    has_exact_term = any(term and term in haystack for term in exact_terms)
    has_mortgage_context = any(keyword in haystack for keyword in MORTGAGE_KEYWORDS)
    if company.strict_google_match:
        return has_exact_term and has_mortgage_context
    return has_exact_term


def parse_feed(text: str, source: Source, company: Company) -> list[Article]:
    root = ET.fromstring(text)
    items: list[Article] = []
    if root.tag.endswith("feed"):
        entries = root.findall(".//{*}entry")
        for entry in entries:
            title = (entry.findtext("{*}title") or "").strip()
            link = ""
            for link_node in entry.findall("{*}link"):
                href = link_node.attrib.get("href")
                if href:
                    link = href
                    break
            summary = entry.findtext("{*}summary") or entry.findtext("{*}content") or ""
            published = (
                entry.findtext("{*}published")
                or entry.findtext("{*}updated")
                or entry.findtext("{*}created")
            )
            if not title or not link:
                continue
            items.append(
                Article(
                    company=company,
                    list_name=company.list_name,
                    title=strip_html(title),
                    url=normalize_url(link, source.url),
                    summary=short_summary(summary),
                    published=parse_isoish_date(published),
                    source_label="official company feed",
                    source_url=source.url,
                    is_official=True,
                    dedupe_key=dedupe_key(company, title, link),
                )
            )
    else:
        entries = root.findall(".//item")
        for entry in entries:
            title = (entry.findtext("title") or "").strip()
            link = (entry.findtext("link") or "").strip()
            description = entry.findtext("description") or ""
            published = entry.findtext("pubDate") or entry.findtext("{*}published")
            source_name = entry.findtext("source") or "official company feed"
            if not title or not link:
                continue
            items.append(
                Article(
                    company=company,
                    list_name=company.list_name,
                    title=strip_html(title),
                    url=normalize_url(link, source.url),
                    summary=short_summary(description),
                    published=parse_isoish_date(published),
                    source_label=source_name,
                    source_url=source.url,
                    is_official=True,
                    dedupe_key=dedupe_key(company, title, link),
                )
            )
    return items


def parse_html_listing(text: str, source: Source, company: Company) -> list[Article]:
    items: list[Article] = []
    for match in HTML_LINK_RE.finditer(text):
        href = html.unescape(match.group("href"))
        label = strip_html(match.group("label"))
        if not href or not label or len(label) < 8:
            continue
        url = normalize_url(href, source.url)
        if not official_domain_match(company, url):
            continue
        items.append(
            Article(
                company=company,
                list_name=company.list_name,
                title=label,
                url=url,
                summary="",
                published=None,
                source_label="official company page",
                source_url=source.url,
                is_official=True,
                dedupe_key=dedupe_key(company, label, url),
            )
        )
        if len(items) >= 20:
            break
    return items


def fetch_official_articles(session: requests.Session, company: Company) -> list[Article]:
    articles: list[Article] = []
    for source in company.official_sources:
        try:
            text = request_text(session, source.url)
            if source.kind in {"rss", "atom"}:
                articles.extend(parse_feed(text, source, company))
            elif source.kind == "html":
                articles.extend(parse_html_listing(text, source, company))
            else:
                LOG.warning("Skipping unsupported source kind %s for %s", source.kind, company.name)
        except Exception as exc:
            LOG.warning("Official source failed for %s (%s): %s", company.name, source.url, exc)
    return articles


def google_news_rss_url(company: Company, config: dict[str, Any]) -> str:
    google_config = config.get("google_news", {})
    language = google_config.get("hl", "en-US")
    geo = google_config.get("gl", "US")
    ceid = google_config.get("ceid", "US:en")

    terms: list[str] = [f"\"{company.name}\""]
    for alias in company.aliases:
        terms.append(f"\"{alias}\"")
    context = " OR ".join(terms)
    context_terms = " OR ".join(f"\"{keyword}\"" for keyword in ("mortgage", "loan", "lender", "broker", "wholesale"))
    query = f"({context}) ({context_terms})"
    encoded = urllib.parse.quote(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl={language}&gl={geo}&ceid={ceid}"


def fetch_google_news_articles(session: requests.Session, company: Company, config: dict[str, Any]) -> list[Article]:
    source = Source(url=google_news_rss_url(company, config), kind="rss")
    try:
        text = request_text(session, source.url)
    except Exception as exc:
        LOG.warning("Google News fallback failed for %s: %s", company.name, exc)
        return []

    root = ET.fromstring(text)
    articles: list[Article] = []
    for item in root.findall(".//item"):
        title = strip_html(item.findtext("title") or "")
        link = normalize_url(item.findtext("link") or "", source.url)
        summary = short_summary(item.findtext("description") or "")
        published = parse_isoish_date(item.findtext("pubDate"))
        if not title or not link:
            continue
        if not strict_google_match(company, title, summary):
            continue
        articles.append(
            Article(
                company=company,
                list_name=company.list_name,
                title=title,
                url=link,
                summary=summary,
                published=published,
                source_label="Google News RSS",
                source_url=source.url,
                is_official=False,
                dedupe_key=dedupe_key(company, title, link),
            )
        )
    return articles


def select_recent_articles(articles: Iterable[Article], max_items: int) -> list[Article]:
    unique: dict[str, Article] = {}
    for article in articles:
        existing = unique.get(article.dedupe_key)
        if existing is None or (article.is_official and not existing.is_official):
            unique[article.dedupe_key] = article
    ordered = sorted(
        unique.values(),
        key=lambda item: (item.published or "", item.is_official, item.title.lower()),
        reverse=True,
    )
    return ordered[:max_items]


def collect_articles_for_company(session: requests.Session, company: Company, config: dict[str, Any]) -> list[Article]:
    official = fetch_official_articles(session, company)
    if official:
        return select_recent_articles(official, int(config.get("max_items_per_company", 3)))
    fallback = fetch_google_news_articles(session, company, config)
    return select_recent_articles(fallback, int(config.get("max_items_per_company", 3)))


def format_alert(article: Article) -> str:
    label = "Vendor Alert" if article.list_name == "vendors" else "Lender Alert"
    published = article.published or "unknown"
    source_name = article.source_label
    summary = article.summary or "New mention found. Open the source for details."
    return textwrap.dedent(
        f"""\
        **[{label}] {article.company.name} — {article.title}**
        {summary}

        Source: {source_name}
        Published: {published}
        Link: {article.url}
        """
    ).strip()


def print_alerts(articles: list[Article]) -> None:
    if not articles:
        print("No matching alerts found.")
        return
    for article in articles:
        print(format_alert(article))
        print()


def load_runtime_config(path: Path) -> dict[str, Any]:
    config = load_yaml(path)
    if "state" not in config:
        raise SponsorWatchError("Missing required config section: state")
    return config


def resolve_post_targets(config: dict[str, Any], roam_client: RoamClient) -> dict[str, str]:
    roam_config = config["roam"]
    channels = roam_config.get("channels", {})
    if not isinstance(channels, dict):
        raise SponsorWatchError("config.yaml roam.channels must be a mapping")
    targets: dict[str, str] = {}
    for list_name in ("vendors", "lenders"):
        raw_value = channels.get(list_name)
        if not raw_value:
            raise SponsorWatchError(f"Missing Roam channel mapping for {list_name}")
        targets[list_name] = roam_client.resolve_chat_id(str(raw_value))
    return targets


def build_roam_client(config: dict[str, Any]) -> RoamClient:
    roam_config = config.get("roam")
    if not isinstance(roam_config, dict):
        raise SponsorWatchError("Missing required config section: roam")
    token = roam_config.get("token") or ""
    if not token or str(token).startswith("replace-me"):
        raise SponsorWatchError("Set roam.token in config.yaml")
    return RoamClient(
        base_url=str(roam_config.get("base_url", "https://api.ro.am")),
        token=str(token),
        timeout=int(config.get("request_timeout_seconds", DEFAULT_TIMEOUT)),
    )


def run_watch(command: str, config_path: Path, companies_path: Path) -> int:
    config = load_runtime_config(config_path)
    companies_by_list = load_companies(companies_path)
    session = make_session(config)
    store = DedupeStore(Path(config["state"].get("path", "state/sponsor_watch.sqlite3")))
    should_post = command == "run"

    lists_to_run = ["vendors", "lenders"]
    if command == "test-vendors":
        lists_to_run = ["vendors"]
    elif command == "test-lenders":
        lists_to_run = ["lenders"]

    roam_client: RoamClient | None = None
    post_targets: dict[str, str] = {}
    if should_post:
        roam_client = build_roam_client(config)
        post_targets = resolve_post_targets(config, roam_client)

    try:
        all_new_articles: list[Article] = []
        for list_name in lists_to_run:
            companies = companies_by_list.get(list_name, [])
            for company in companies:
                LOG.info("Scanning %s: %s", list_name, company.name)
                for article in collect_articles_for_company(session, company, config):
                    if store.already_seen(article.dedupe_key):
                        continue
                    all_new_articles.append(article)
                    if should_post and roam_client is not None:
                        roam_client.post_message(post_targets[list_name], format_alert(article))
                        LOG.info("Posted alert for %s: %s", company.name, article.title)
                    store.mark_seen(article)
        print_alerts(all_new_articles)
        return 0
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        return run_watch(
            command=args.command,
            config_path=Path(args.config),
            companies_path=Path(args.companies),
        )
    except SponsorWatchError as exc:
        LOG.error("%s", exc)
        return 2
    except requests.RequestException as exc:
        LOG.error("Network error: %s", exc)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
