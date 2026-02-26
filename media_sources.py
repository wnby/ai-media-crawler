import re
import asyncio
import argparse
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Optional
import html as ihtml
import requests
import xml.etree.ElementTree as ET
import gzip

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


class MediaSourceCrawl4AI:
    def __init__(self, headless: bool = True, debug: bool = False):
        self.headless = headless
        self.debug = debug

    def _log(self, msg: str):
        if self.debug:
            print(msg)

    def _clean(self, s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip()

    def _to_datetime(self, s: str) -> Optional[datetime]:
        s = (s or "").strip()
        if not s:
            return None
        s = s.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-")
        s = re.sub(r"\s+", " ", s)
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:19], fmt)
            except Exception:
                pass
        return None

    def _is_recent(self, dt: Optional[datetime], days: int) -> bool:
        if days <= 0:
            return True
        if dt is None:
            return False
        return dt >= (datetime.now() - timedelta(days=days))

    def _normalize_url(self, base: str, url: str) -> str:
        if not url:
            return ""

        u = url.strip().replace("\\/", "/")

        # 关键修复：若包含多个绝对 URL，取最后一个真实目标
        abs_urls = re.findall(r"https?://[^\"'\s<>]+", u)
        if len(abs_urls) >= 2:
            u = abs_urls[-1]
        elif len(abs_urls) == 1 and not u.startswith(abs_urls[0]):
            # 例如 "xxx https://a.com/1"
            u = abs_urls[0]

        # 相对路径补全
        if not re.match(r"^https?://", u):
            u = urljoin(base, u)
        m = re.match(r"^https?://[^/]+/(https?://.+)$", u)
        if m:
            u = m.group(1)

        return u.split("#")[0].strip()

    def _is_article_url(self, url: str) -> bool:
        u = url.lower().strip()
        pu = urlparse(u)

        if pu.path in ("", "/"): return False
        if pu.query and "author=" in pu.query: return False
        if any(x in u for x in ["/tag/", "/tags/", "/category/", "/author/", "javascript:", "#"]): return False
        if any(x in u for x in ["/meet/", "ai_shortlist", "/short_urls/"]): return False

        if re.search(r"/20\d{2}/\d{1,2}/\d+\.html$", u): return True
        if u.endswith(".html"): return True
        if "/articles/" in u and len(u.split("/articles/")[-1]) > 3: return True

        depth = len([x for x in pu.path.split("/") if x])
        return depth >= 3

    def _extract_date_from_text_or_url(self, text: str, url: str) -> str:
        m = re.search(r"(20\d{2})[-/年](\d{1,2})[-/月](\d{1,2})", text or "")
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m2 = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", url or "")
        if m2:
            return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
        return ""

    def _extract_urls_from_html(self, source: str, entry: str, raw_html: str) -> List[Dict]:
        """当 crawl4ai links 为空时，从源码兜底提取文章 URL"""
        if not raw_html:
            return []
        h = ihtml.unescape(raw_html).replace("\\/", "/")
        urls = set()

        if source == "qbitai":
            for u in re.findall(r'https?://(?:www\.)?qbitai\.com/20\d{2}/\d{2}/\d+\.html', h):
                urls.add(u)
            for p in re.findall(r'(/20\d{2}/\d{2}/\d+\.html)', h):
                urls.add(urljoin(entry, p))

        elif source == "xinzhiyuan":
            # aiera 常见路径：/2026/02/23/...
            for u in re.findall(r'https?://(?:www\.)?aiera\.com\.cn/20\d{2}/\d{2}/\d{2}/[^"\'<>\s]+', h):
                urls.add(u)
            for p in re.findall(r'(/20\d{2}/\d{2}/\d{2}/[^"\'<>\s]+)', h):
                urls.add(urljoin(entry, p))

        elif source == "jiqizhixin":
            # 机器之心文章路径
            for u in re.findall(r'https?://(?:www\.)?jiqizhixin\.com/articles/[^"\'<>\s]+', h):
                urls.add(u)
            for p in re.findall(r'(/articles/[^"\'<>\s]+)', h):
                urls.add(urljoin(entry, p))

        out = [{"title": "待解析标题", "url": u} for u in urls if self._is_article_url(u)]
        return out

    def _get_sitemap_urls_from_robots(self, base: str) -> List[str]:
        urls = []
        try:
            r = requests.get(urljoin(base, "/robots.txt"), timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            if r.ok:
                for line in r.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith("sitemap:"):
                        sm = line.split(":", 1)[1].strip()
                        if sm:
                            urls.append(sm)
        except Exception as e:
            self._log(f"[ROBOTS-ERR] {base} -> {e}")

        # 常见兜底
        if not urls:
            urls = [
                urljoin(base, "/sitemap.xml"),
                urljoin(base, "/sitemap_index.xml"),
                urljoin(base, "/sitemap-index.xml"),
            ]
        return urls

    def _parse_sitemap_recursive(self, sitemap_url: str, max_urls: int = 800) -> List[str]:
        out, queue, seen = [], [sitemap_url], set()
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        while queue and len(out) < max_urls:
            u = queue.pop(0)
            if u in seen:
                continue
            seen.add(u)

            try:
                r = requests.get(
                    u,
                    timeout=12,
                    headers={"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip, deflate"},
                )
                if not r.ok:
                    self._log(f"[SITEMAP-HTTP] {u} -> {r.status_code}")
                    continue

                raw = r.content or b""
                # 关键修复：支持 .gz sitemap
                if u.lower().endswith(".gz") or raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)

                text = raw.decode("utf-8", errors="replace").lstrip("\ufeff").strip()
                if not text.startswith("<"):
                    self._log(f"[SITEMAP-NOT-XML] {u} -> head={text[:80]!r}")
                    continue

                root = ET.fromstring(text)

                smaps = root.findall(".//sm:sitemap/sm:loc", ns) or root.findall(".//sitemap/loc")
                for s in smaps:
                    loc = (s.text or "").strip()
                    if loc:
                        queue.append(loc)

                locs = root.findall(".//sm:url/sm:loc", ns) or root.findall(".//url/loc")
                for loc in locs:
                    x = (loc.text or "").strip()
                    if x:
                        out.append(x)
                        if len(out) >= max_urls:
                            break

            except Exception as e:
                self._log(f"[SITEMAP-ERR] {u} -> {e}")

        return out

    def _fallback_jiqizhixin_sitemap_candidates(self) -> List[Dict]:
        """从机器之心的 sitemap 兜底提取候选文章链接"""
        urls = []
        base = "https://www.jiqizhixin.com"

        for sitemap_url in self._get_sitemap_urls_from_robots(base):
            sitemap_urls = self._parse_sitemap_recursive(sitemap_url, max_urls=500)
            for url in sitemap_urls:
                nu = self._normalize_url(base, url)  # 改这里
                if nu and self._is_article_url(nu):
                    urls.append({"title": "待解析标题", "url": nu})
            if len(urls) > 0:
                break

        return urls

    async def _fetch_site(
        self,
        crawler: AsyncWebCrawler,
        source: str,
        entry_urls: List[str],
        days: int,
        limit: int,
        target_date: Optional[date] = None,   # 新增
    ) -> List[Dict]:
        results = []
        seen_url = set()
        candidates = []

        for entry in entry_urls:
            self._log(f"[OPEN] {entry}")
            try:
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    js_code="""
                        window.scrollTo(0, document.body.scrollHeight);
                        await new Promise(r => setTimeout(r, 1200));
                        window.scrollTo(0, document.body.scrollHeight);
                    """,
                    delay_before_return_html=5.0,   # 3 -> 5
                )
                result = await crawler.arun(url=entry, config=config)
                if not result.success:
                    self._log(f"[ENTRY-ERR] {entry} -> {result.error_message}")
                    continue

                internal_links = result.links.get("internal", []) if result.links else []
                for link_obj in internal_links:
                    href = link_obj.get("href", "")
                    title = self._clean(link_obj.get("text", ""))
                    if not href:
                        continue
                    href = self._normalize_url(entry, href)  # 改这里
                    if not href:
                        continue
                    if len(title) < 2:
                        title = "待解析标题"
                    if not self._is_article_url(href):
                        continue
                    candidates.append({"title": title, "url": href})

                # 关键：links 太少时启用源码兜底
                if len(candidates) < 5:
                    candidates.extend(self._extract_urls_from_html(source, entry, result.html or ""))

            except Exception as e:
                self._log(f"[ENTRY-ERR] {entry} -> {e}")

        # 机器之心：入口抓不到时，走 robots+sitemap
        if source == "jiqizhixin" and len(candidates) == 0:
            candidates.extend(self._fallback_jiqizhixin_sitemap_candidates())

        # 去重候选链接
        dedup = []
        seen = set()
        for c in candidates:
            if c["url"] in seen: continue
            seen.add(c["url"])
            dedup.append(c)

        self._log(f"[{source}] candidates={len(dedup)}")

        # 仅修复：限制候选探测数量，避免 jiqizhixin 候选过多导致抓取过慢
        def _rank_candidate(item: Dict):
            d = self._extract_date_from_text_or_url("", item.get("url", ""))
            dt = self._to_datetime(d) if d else None
            # 优先：有日期 > 无日期；日期越新越靠前
            return (1 if dt else 0, dt or datetime.min)

        dedup = sorted(dedup, key=_rank_candidate, reverse=True)
        max_probe = max(limit, 10) if source == "jiqizhixin" else max(limit, 10)
        dedup = dedup[:max_probe]
        self._log(f"[{source}] probe={len(dedup)}")

        # 2. 抓取文章详情页
        for c in dedup:
            if c["url"] in seen_url: continue
            seen_url.add(c["url"])

            try:
                config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
                res = await crawler.arun(url=c["url"], config=config)
                
                if not res.success:
                    continue

                # crawl4ai 自动提取了干净的 Markdown
                markdown_text = res.markdown or ""
                html_text = res.html or ""
                
                # 提取日期
                pub_date = self._extract_date_from_text_or_url(markdown_text, c["url"])
                if not pub_date:
                    # 兜底：从 HTML meta 中正则提取
                    m = re.search(r'content="([^"]*202\d[-/]\d{1,2}[-/]\d{1,2}[^"]*)"', html_text)
                    if m:
                        pub_date = self._extract_date_from_text_or_url(m.group(1), "")

                dt = self._to_datetime(pub_date)

                # 按指定日期过滤（用于每日推送：昨天 00:00-23:59）
                if target_date is not None:
                    if dt is None or dt.date() != target_date:
                        continue
                else:
                    # 原有逻辑
                    if dt is None:
                        if source != "jiqizhixin":
                            continue
                    elif not self._is_recent(dt, days):
                        continue

                # 修正标题（如果抓取到的标题太短，用详情页的标题）
                final_title = c["title"]
                if res.metadata and res.metadata.get("title"):
                    page_title = self._clean(res.metadata.get("title").split("|")[0].split("-")[0])
                    if len(page_title) > len(final_title) or final_title == "待解析标题":
                        final_title = page_title

                # 机器之心兜底页/栏目页过滤（关键）
                if source == "jiqizhixin":
                    bad_title_kw = ["文章库", "找不到您请求的页面", "404"]
                    if any(k in final_title for k in bad_title_kw):
                        continue
                    # 正文太短也跳过（防止落到空模板页）
                    if len(self._clean(markdown_text)) < 200:
                        continue

                # 如果还是没拿到有效标题，丢弃
                if final_title == "待解析标题" or len(final_title) < 4:
                    continue
                if "找不到您请求的页面" in final_title or "404" in final_title.lower():
                    continue

                results.append({
                    "title": final_title,
                    "url": c["url"],
                    "abstract": self._clean(markdown_text[:800]), # 直接用干净的 Markdown 做摘要
                    "source": source,
                    "source_type": "commentary",
                    "is_secondary": True,
                    "paper_ref_confidence": 0.35,
                    "pub_date": pub_date or "",
                })

                self._log(f"[KEEP] [{source}] {final_title[:28]}... {pub_date or 'N/A'}")
                if len(results) >= limit:
                    break

            except Exception as e:
                self._log(f"[ARTICLE-ERR] {c['url']} -> {e}")

        return results

    async def a_search(
        self,
        days: int = 7,
        limit_per_source: int = 10,
        target_date: Optional[date] = None,   # 新增
    ) -> List[Dict]:
        all_items = []
        browser_config = BrowserConfig(headless=self.headless)
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            # 1. 量子位
            all_items += await self._fetch_site(
                crawler, "qbitai", ["https://www.qbitai.com/"], days, limit_per_source, target_date
            )
            # 2. 新智元
            all_items += await self._fetch_site(
                crawler, "xinzhiyuan", ["https://www.aiera.com.cn/", "https://aiera.com.cn/"], days, limit_per_source, target_date
            )

        # 最终去重
        out, seen = [], set()
        for it in all_items:
            k = (it["title"], it["url"])
            if k in seen:
                continue
            seen.add(k)
            out.append(it)
        return out

    def search(
        self,
        days: int = 7,
        limit_per_source: int = 10,
        target_date: Optional[date] = None,   # 新增
    ) -> List[Dict]:
        return asyncio.run(self.a_search(days, limit_per_source, target_date))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="近几天，0 表示不过滤")
    parser.add_argument("--limit", type=int, default=10, help="每个来源保留条数")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--show", action="store_true", help="显示浏览器窗口（调试）")
    parser.add_argument("--yesterday", action="store_true", help="仅保留昨天 00:00-23:59 的文章")
    args = parser.parse_args()

    agg = MediaSourceCrawl4AI(headless=not args.show, debug=args.debug)

    t_date = None
    if args.yesterday:
        t_date = (datetime.now() - timedelta(days=1)).date()

    data = agg.search(days=args.days, limit_per_source=args.limit, target_date=t_date)

    print(f"\n抓取完成，共 {len(data)} 条")
    for i, item in enumerate(data[:60], 1):
        d = item.get("pub_date") or "N/A"
        print(f"{i:02d}. [{item['source']}] {item['title']} [{d}]")
        print(f"    {item['url']}")