import asyncio
import os
import time
import base64
import json
import uuid
import re
from pathlib import Path
from urllib.parse import urlparse
from pyppeteer import launch
import requests

class LandingPageCrawler:
    """
    落地页爬虫工具类 - 负责页面渲染、文本提取、图片抓取及自动加购截图
    """
    def __init__(self, headless=True, executable_path=None):
        self.headless = headless
        self.executable_path = executable_path
        self.browser = None

    async def init_browser(self):
        """初始化浏览器"""
        if not self.browser:
            self.browser = await launch({
                'headless': self.headless,
                'args': ['--no-sandbox', '--disable-setuid-sandbox'],
                'defaultViewport': {'width': 1920, 'height': 1080},
                'executablePath': self.executable_path
            })

    async def close_browser(self):
        """关闭浏览器"""
        if self.browser:
            await self.browser.close()
            self.browser = None

    async def crawl(self, url):
        """
        核心爬取逻辑
        :return: { 'text': str, 'images': list, 'error': str }
        """
        await self.init_browser()
        page = await self.browser.newPage()
        result = {
            'text': '',
            'images': [],
            'error': None
        }

        try:
            # 1. 访问页面
            print(f"🌐 访问页面: {url}")
            # 设置超时并等待网络空闲
            response = await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
            
            if response:
                if response.status == 404:
                    result['error'] = "404 Not Found"
                    return result
                if response.status == 403:
                    result['error'] = "403 Forbidden"
                    return result

            # 2. 提取文本
            result['text'] = await page.evaluate('() => document.body.innerText')
            
            # 3. 提取图片 (移植 p308.js 的去重逻辑)
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

        except Exception as e:
            result['error'] = str(e)
            print(f"❌ 爬取失败: {e}")
        finally:
            await page.close()
            
        return result


# 示例驱动代码 (仅供参考)
if __name__ == "__main__":
    async def test():
        crawler = LandingPageCrawler(headless=False)
        res = await crawler.crawl("https://www.example.com")
        print(f"抓取到文本长度: {len(res['text'])}")
        print(f"抓取到图片数量: {len(res['images'])}")
        await crawler.close_browser()
    
    # asyncio.run(test())
