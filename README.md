#  AI Media Crawler (基于 Crawl4AI 的爬虫)

这是一个基于开源库 [Crawl4AI](https://github.com/unclecode/crawl4ai) 构建的异步网页爬虫工具。专门用于抓取中文 AI 公众号（如**量子位**、**新智元**）的文章资讯。

##  核心特性

-  **异步高并发**：基于 `asyncio` 和 `Crawl4AI`，抓取速度极快。
-  **LLM 友好的输出**：自动过滤网页杂质，直接提取高质量的 Markdown 格式文章摘要/正文。
-  **智能日期提取与过滤**：内置强大的正则解析，能从 URL、HTML Meta 或正文中精准提取发布日期。支持按天数过滤，或一键提取“昨日新闻”（非常适合做每日自动推送订阅）。
-  **极强防反爬与兜底机制**：
  - 动态网页支持：内置 JS 脚本自动滚动到底部，等待动态加载。
  - 链接提取兜底：当常规 DOM 解析失败时，自动降级为源码正则提取。
  - Sitemap 深度解析：针对难以抓取入口的站点（如机器之心），支持通过 `robots.txt` 自动追踪并解析 `.xml` 和 `.gz` 格式的 Sitemap 获取最新文章。

##  支持媒体源

当前代码内置支持/兼容以下站点的解析逻辑：
- ✅ **量子位** (QbitAI)
- ✅ **新智元** (XinZhiYuan / Aiera)

##  安装指南
```bash
git clone https://github.com/wnby/ai-media-crawler.git
cd ai-media-crawler
