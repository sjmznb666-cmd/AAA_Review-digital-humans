import asyncio
from playwright.async_api import async_playwright

class LandingPageCrawler:
    def __init__(self, headless=True, executable_path=None):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None

    async def init_browser(self):
        if self.browser and self.browser.is_connected():
            return
        if self.context:
            try:
                await self.context.close()
            except:
                pass
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        if self.playwright:
            try:
                await self.playwright.stop()
            except:
                pass

        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )

    async def close_browser(self):
        try:
            if self.context:
                await self.context.close()
        except:
            pass
        try:
            if self.browser:
                await self.browser.close()
        except:
            pass
        try:
            if self.playwright:
                await self.playwright.stop()
        except:
            pass
        self.browser = None

    async def crawl(self, url, max_retries=2):
        for attempt in range(max_retries + 1):
            try:
                await self.init_browser()
                page = await self.context.new_page()
                result = {"text": "", "images": [], "error": None}
                try:
                    print(f"🌐 访问页面: {url}")
                    await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    result['text'] = await page.inner_text('body')
                    result['images'] = await page.evaluate('''() => {
                        const MIN_SIZE = 300;
                        const getCoreUrlAndSize = (url) => {
                            if (!url) return { coreUrl: null, size: 0 };
                            let cleanedUrl = url.split('?')[0];
                            const match = cleanedUrl.match(/-(\d+)\.(jpg|jpeg|png|webp)$/i);
                            let size = match ? parseInt(match[1]) : 0;
                            let coreUrl = match ? cleanedUrl.replace("-" + match[1], "") : cleanedUrl;
                            return { coreUrl, size };
                        };
                        const uniqueLinksMap = new Map();
                        document.querySelectorAll('img').forEach(img => {
                            if (img.clientWidth < MIN_SIZE || img.clientHeight < MIN_SIZE) return;
                            let src = img.getAttribute('data-url') || img.getAttribute('data-src') || img.src;
                            if (!src || !src.startsWith('http')) return;
                            const { coreUrl, size } = getCoreUrlAndSize(src);
                            const existing = uniqueLinksMap.get(coreUrl);
                            if (!existing || size > existing.size) {
                                uniqueLinksMap.set(coreUrl, { url: src, size: size });
                            }
                        });
                        return Array.from(uniqueLinksMap.values()).map(e => e.url);
                    }''')
                    return result
                except Exception as inner_e:
                    if attempt < max_retries and ('Connection closed' in str(inner_e) or 'Browser' in str(inner_e)):
                        print(f"⚠️ 浏览器连接异常，正在重启并重试...")
                        await page.close()
                        await self.close_browser()
                        continue
                    result['error'] = str(inner_e)[:200]
                    print(f"❌ 爬取失败: {inner_e}")
                    return result
                finally:
                    await page.close()
            except Exception as outer_e:
                if attempt < max_retries:
                    print(f"⚠️ 浏览器初始化失败，重试中...")
                    await self.close_browser()
                    continue
                return {"text": "", "images": [], "error": str(outer_e)[:200]}
        return {"text": "", "images": [], "error": "多次重试后仍然失败"}