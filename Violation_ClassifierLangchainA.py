import asyncio
import openpyxl.worksheet.datavalidation
_old_init = openpyxl.worksheet.datavalidation.DataValidation.__init__
def _new_init(self, *args, **kwargs):
    kwargs.pop('id', None)
    _old_init(self, *args, **kwargs)
openpyxl.worksheet.datavalidation.DataValidation.__init__ = _new_init
import json
import requests
from typing import List
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from feishu_api import FeishuAPI
from excel_handler import ExcelHandler
from landing_page_crawler import LandingPageCrawler

# ================= 配置区域 =================

# 飞书应用配置
FEISHU_APP_ID = 'cli_a81fe4ed0730900c'
FEISHU_APP_SECRET = 'wffGOfPCemJA9I3b86e9rlStWWC7OAsf'

# Excel 输入列名定义
COL_URL_SOURCE = "链接来源"
COL_URL = "链接"
COL_URL_TYPE = "链接类型"
COL_URL_THIRDSOURCE = "第三方域名链接"
COL_STORE = "店铺"
COL_SRORE_STAUTS = "店铺状态"
COL_WHETHER_TESTSTORE = "是否为测试店铺"
COL_DEPT = "部门"
COL_OPERATOR = "运营"
COL_VIRTUAL_SPU = "虚拟SPU"
COL_VIRTUAL_SPUSTAUTS = "虚拟SPU审核状态"
COL_REAL_SPU = "真实SPU"
COL_REAL_SPUSTAUTS = "真实SPU是否违规"
COL_STAUTS = "状态"

# 飞书 Bitable 输出列名定义
FIELD_OUT_URL_SOURCE = '链接来源' 
FIELD_OUT_URL = '链接'
FIELD_OUT_URL_TYPE = '链接类型'
FIELD_OUT_URL_THIRDSOURCE = '第三方域名链接'
FIELD_OUT_STORE = '店铺'
FIELD_OUT_STORE_STATUS = '店铺状态'
FIELD_OUT_WHETHER_TESTSTORE = '是否为测试店铺'
FIELD_OUT_DEPT = '部门'
FIELD_OUT_OPERATOR = '运营'
FIELD_OUT_VIRTUAL_SPU = '虚拟SPU'
FIELD_OUT_VIRTUAL_SPU_STATUS = '虚拟SPU审核状态'
FIELD_OUT_REAL_SPU = '真实SPU'
FIELD_OUT_REAL_SPU_STATUS = '真实SPU是否违规'
FIELD_OUT_STATUS = '状态'

FIELD_OUT_IMAGE_CONTENT = '落地页图片内容'
FIELD_OUT_TEXT_CONTENT = '落地页文字内容'

# 侵权审核特有输出字段
FIELD_OUT_CHECK_TRADEMARK = '排查商标'
FIELD_OUT_PROCESS_SCALE = '处理尺度'
FIELD_OUT_IS_VIOLATION = '是否侵权'
FIELD_OUT_PROCESS_METHOD = '处理方式'
FIELD_OUT_VIOLATION_IMAGE = '侵权图片'
FIELD_OUT_AUDIT_STATUS = '审核状态'
FIELD_OUT_VIOLATION_TEXT = '侵权文字'
FIELD_OUT_TEXT_THINK = '文字审核思考过程'
FIELD_OUT_IMAGE_THINK = '图片审核思考过程'

# ================= 固定的内置审核规则 =================

FIXED_TEXT_VIOLATION_RULE = """
重点核查商品文本中是否包含用户指定的商标品牌名、相关的品牌宣传语或其明显的变异混淆词。
"""

FIXED_IMAGE_VIOLATION_RULE = """
重点识别商品图片中是否展现了用户指定品牌的商标 Logo、独特的品牌视觉图案、具有辨识度的品牌外观设计或其高仿标识。
"""

# 大模型 API 配置
API_KEY = "78cbeed3-eea7-4832-ba9c-b3fefeff316f"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

# 模型配置
MODEL_TEXT_AUDIT = "doubao-seed-1-8-251228"
MODEL_IMAGE_AUDIT = "doubao-seed-1-8-251228"

# 初始化模型
llm_text = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_TEXT_AUDIT, temperature=0.0, timeout=60)
llm_vision = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_IMAGE_AUDIT, temperature=0.0, timeout=120)

def detect_browser_path():
    """探测本地浏览器路径"""
    import os
    possible_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
    ]
    for p in possible_paths:
        if os.path.exists(p):
            print(f"✅ Found browser at: {p}")
            return p
    return None

# 初始化工具类
feishu = FeishuAPI(FEISHU_APP_ID, FEISHU_APP_SECRET)
browser_path = detect_browser_path()
crawler = LandingPageCrawler(headless=True, executable_path=browser_path)

# ================= LangChain 构建 =================

def extract_json_from_text(text: str) -> dict:
    """从文本中提取并解析 JSON 对象"""
    if not text or not isinstance(text, str):
        return {}
    
    clean_text = text.replace("```json", "").replace("```", "").strip()
    
    try:
        start = clean_text.find("{")
        end = clean_text.rfind("}")
        if start != -1 and end != -1:
            json_str = clean_text[start:end+1]
            return json.loads(json_str)
    except:
        pass
    
    return {}

def create_text_audit_chain(violation_rule: str):
    """文本侵权审核 Chain"""
    system_prompt = "你将扮演**深度商标侵权检索专家**。你的任务是从海量商品文本中挖掘任何细微的侵权痕迹。"
    user_prompt = f"""
请分析以下落地页文本：
<商品文本>
{{text_content}}
</商品文本>

### 审核目标
核查该文本是否出现了用户指定的特定商标品牌：
{violation_rule}

### 核心判定准则（必须执行）：
1. **完全匹配：** 出现了商标原词。
2. **模糊/变异匹配：** 出现了为了规避审核而修改的词汇（如：Nike 写成 N-ike, Nike_shoes 等）。
3. **输出要求：** 哪怕只出现一次也属于“是(侵权)”。如果没有任何相关商标，判定为“否”。如果发现侵权，必须严格统计并输出侵权商标的**总次数**以及它们的**具体出现位置**。

### 约束规则
- 输出请严格遵循以下 JSON 格式：

---思考过程---
[简要分析在文本中发现了哪个具体的商标、所有的具体出现位置及上下文语境，并计算总次数]

---最终结论---
{{{{
  "{FIELD_OUT_IS_VIOLATION}": "是/否/需人工处理",
  "{FIELD_OUT_VIOLATION_TEXT}": "[若侵权，必须按此格式描述：'第N个侵权点：在特定位置(如标题/正文第X段/详情区等)出现用户输入的侵权商标‘商标名’，共出现X次，需规避删除'。完全未发现填'无']",
  "{FIELD_OUT_TEXT_THINK}": "[简要总结分析逻辑，限300字]"
}}}}
"""
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", user_prompt)
    ])
    return prompt | llm_text | StrOutputParser()

import requests
import json

def check_image_size(url: str) -> int:
    """探测图片真实字节体积，对于不开放HEAD头的CDN降级使用流式短下"""
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        cl = r.headers.get("Content-Length")
        if cl and cl.isdigit() and int(cl) > 0:
            return int(cl)
            
        r = requests.get(url, stream=True, timeout=10)
        size = 0
        for chunk in r.iter_content(1024*1024):
            if chunk:
                size += len(chunk)
                if size > 9.9 * 1024 * 1024:
                    r.close()
                    break
        return size
    except:
        return 0

async def audit_images_async(image_urls: List[str], violation_rule: str, local_paths: List[str] = None):
    """调用视觉模型审核图片 (图片侵权审核)"""
    if not image_urls:
        return {
            "think": "未提供产品图片，无法进行视觉分析",
            "result": f'{{\n  "{FIELD_OUT_CHECK_TRADEMARK}": "无",\n  "{FIELD_OUT_IS_VIOLATION}": "需人工处理",\n  "{FIELD_OUT_PROCESS_METHOD}": "执行人工核查",\n  "{FIELD_OUT_IMAGE_THINK}": "落地页未通过爬虫抓取到有效图片，触发抓取异常规则"\n}}'
        }
    
    prompt_text = f"""你将扮演**深度视觉商标识别专家**。你的任务是从商品图片中寻找用户指定的商标 Logo 或代表性品牌设计：
{violation_rule}

### 核心判定准则：
1. **Logo 识别：** 识别主体产品实体、标签、外包装中是否出现了指定的品牌 Logo，主体产品是否是高仿指定商标产品类型。
2. **外观相似性：** 识别是否有明显模仿某品牌经典外观的行为。
3. **输出要求：** 如果识别到，判定为“是”，否则为“否”。
4. **相关限度：** 如果Logo或者相关Logo旗下商品出现在模特穿搭或者背景等无关要素中，则不视为侵权；相关支付方式，货运方式出现的商标，证书等不视为侵权；

### 约束规则
- 输出请严格遵循以下 JSON 格式：

---思考过程---
[简要描述在第几张图的什么位置发现了什么特征]

---最终结论---
{{
  "{FIELD_OUT_IS_VIOLATION}": "是/否/需人工处理",
  "{FIELD_OUT_VIOLATION_TEXT}": "[若侵权，必须严格按此格式：'第N张侵权图片：在XX位置发现XX特征...'。注意：这里的N不是原始图片序号，而是按找到侵权点的先后进行1, 2, 3...重新编号。例如原图第3张和第5张侵权，你应该输出：'第1张侵权图片：xxx；第2张侵权图片：xxx'。完全未发现填'无']",
  "VIOLATION_IMAGE_INDICES": [违规图片在原始序列中的整型序号数组，例如: [3, 5]],
  "{FIELD_OUT_IMAGE_THINK}": "[简要描述视觉证据，为何判定，限300字]"
}}
"""
    valid_urls_with_i = [(i + 1, url) for i, url in enumerate(image_urls)]
    content = [{"type": "text", "text": prompt_text}]
    for _, url in valid_urls_with_i:
        content.append({
            "type": "image_url",
            "image_url": {"url": url}
        })
    
    oversize_indices = []
    
    try:
        response = await llm_vision.ainvoke([("user", content)])
        full_text = response.content
    except Exception as e:
        err_str = str(e)
        if "Oversize" in err_str or "size" in err_str.lower() or "too large" in err_str.lower() or "Timeout" in err_str:
            print(f"⚠️ 大模型返回图片过大或下载报错，启动本地追溯机制记录异常图片位置...")
            import os
            if local_paths and len(local_paths) == len(image_urls):
                sizes = [os.path.getsize(p) if p and os.path.exists(p) else 0 for p in local_paths]
            else:
                sizes = await asyncio.gather(*[
                    asyncio.to_thread(check_image_size, url) for url in image_urls
                ])
            valid_urls_with_i = []
            for i, (url, size) in enumerate(zip(image_urls, sizes)):
                if size > 9.5 * 1024 * 1024:
                    oversize_indices.append(i + 1)
                else:
                    valid_urls_with_i.append((i + 1, url))
                    
            if not valid_urls_with_i:
                idx_str = "、".join(map(str, oversize_indices)) if oversize_indices else "全部"
                res_dict = {
                    FIELD_OUT_CHECK_TRADEMARK: "无",
                    FIELD_OUT_IS_VIOLATION: "需人工处理",
                    "VIOLATION_IMAGE_INDICES": [],
                    FIELD_OUT_PROCESS_METHOD: f"落地页图片第 {idx_str} 张体积过大，已跳过AI审核需人工补充核查",
                    FIELD_OUT_IMAGE_THINK: "所有图片因过大被剥离，无法继续AI处理"
                }
                return {"think": "所有图片均过大", "result": json.dumps(res_dict, ensure_ascii=False), "oversize_indices": oversize_indices}
            
            print(f"📦 已剔除异常图片序号 {oversize_indices}，正在继续处理后续剩下的图片...")
            
            retry_content = [{"type": "text", "text": prompt_text}]
            for _, url in valid_urls_with_i:
                retry_content.append({"type": "image_url", "image_url": {"url": url}})
                
            try:
                response = await llm_vision.ainvoke([("user", retry_content)])
                full_text = response.content
            except Exception as retry_e:
                err_dict = {
                    FIELD_OUT_CHECK_TRADEMARK: "无",
                    FIELD_OUT_IS_VIOLATION: "需人工处理",
                    "VIOLATION_IMAGE_INDICES": [],
                    FIELD_OUT_PROCESS_METHOD: f"剔除大图后二次请求分析仍然报错: {retry_e}"
                }
                think_msg = f"重新提交图片审核时报错: {retry_e}"
                if "UnsupportedImageFormat" in str(retry_e):
                    think_msg = "落地页图片参数存在问题，需人工审核"
                return {"think": think_msg, "result": json.dumps(err_dict, ensure_ascii=False), "oversize_indices": oversize_indices}
        else:
            think_msg = f"图片审核调用报错: {err_str}"
            if "UnsupportedImageFormat" in err_str:
                think_msg = "落地页图片参数存在问题，需人工审核"
            err_dict = {
                FIELD_OUT_CHECK_TRADEMARK: "无",
                FIELD_OUT_IS_VIOLATION: "需人工处理",
                "VIOLATION_IMAGE_INDICES": [],
                FIELD_OUT_PROCESS_METHOD: f"分析失败(未知报错或被阻断判定): {err_str}"
            }
            return {"think": think_msg, "result": json.dumps(err_dict, ensure_ascii=False), "oversize_indices": oversize_indices}
            
    think = full_text
    result = full_text
    if "---最终结论---" in full_text:
        parts = full_text.split("---最终结论---")
        think = parts[0].replace("---思考过程---", "").strip()
        result = parts[1].strip()
        
    parsed = extract_json_from_text(result)
    if parsed:
        old_indices = parsed.get("VIOLATION_IMAGE_INDICES", [])
        new_indices = []
        if isinstance(old_indices, list):
            for llm_idx in old_indices:
                if isinstance(llm_idx, int) and 1 <= llm_idx <= len(valid_urls_with_i):
                    orig_i = valid_urls_with_i[llm_idx - 1][0]
                    new_indices.append(orig_i)
        
        parsed["VIOLATION_IMAGE_INDICES"] = new_indices
        
        if oversize_indices:
            idx_str = "、".join(map(str, oversize_indices))
            msg = f"落地页图片第 {idx_str} 张体积过大，已跳过AI审核需人工补充核查"
            
            # --- 核心修改：只要有大图，结论固定为“需人工处理” ---
            parsed[FIELD_OUT_IS_VIOLATION] = "需人工处理"
            
            old_method = parsed.get(FIELD_OUT_PROCESS_METHOD, "")
            if old_method and old_method not in ["无", "无需处理", "执行人工核查"]:
                parsed[FIELD_OUT_PROCESS_METHOD] = old_method + "；注意：" + msg
            else:
                parsed[FIELD_OUT_PROCESS_METHOD] = msg
            
        result = json.dumps(parsed, ensure_ascii=False)
    else:
        if oversize_indices:
            idx_str = "、".join(map(str, oversize_indices))
            parsed_fallback = {
                FIELD_OUT_IS_VIOLATION: "需人工处理",
                FIELD_OUT_PROCESS_METHOD: f"落地页图片第 {idx_str} 张体积过大，请执行人工核查",
                "VIOLATION_IMAGE_INDICES": []
            }
            result = json.dumps(parsed_fallback, ensure_ascii=False)
    
    return {"think": think, "result": result, "oversize_indices": oversize_indices}

# ================= 业务逻辑 =================

def safe_get(lst, idx, default=""):
    return lst[idx] if lst and isinstance(lst, list) and len(lst) > idx else default

async def main_async():
    try:
        # 0. 用户输入初始化
        excel_path = input("请输入 Excel 文件路径: ").strip().replace('"', '')
        if not excel_path:
            print("❌ 路径不能为空")
            return
            
        app_token = input("请输入飞书多维表格 APP_TOKEN: ").strip()
        table_id = input("请输入飞书多维表格 TABLE_ID: ").strip()

        if not app_token or not table_id:
            print("❌ 缺少飞书 AppToken 或 TableId，程序退出。")
            return
            
        target_trademarks = input("\n请输入需要特别检索的侵权商标（多个品牌请用逗号隔开，若无则直接回车跳过）: ").strip()
        
        start_row_str = input("\n请输入起始处理的记录序号[例如输入 2 就会从表格第 2 条开始跑]（按回车默认从第 1 条开始）: ").strip()
        start_row = 1
        if start_row_str.isdigit() and int(start_row_str) > 0:
            start_row = int(start_row_str)
            
        # 挂载内置规则
        text_violation_rule = FIXED_TEXT_VIOLATION_RULE
        image_violation_rule = FIXED_IMAGE_VIOLATION_RULE
        
        # 提取 Excel 文件名（去除后缀，如 "待审核商品名单"）
        import os
        excel_filename = os.path.splitext(os.path.basename(excel_path))[0]
        
        # 如果有指定的特定商标，将其注入至模型审核规则中
        if target_trademarks:
            trademark_appendix = f"\n\n#### 特别排查商标清单\n- **重点排查：** 【{target_trademarks}】\n- **任务要求：** 请严密核查商品中是否出现了上述特定商标、Logo图案或其高仿变体标识。如果发现，必须判定为违规。"
            text_violation_rule += trademark_appendix
            image_violation_rule += trademark_appendix
        else:
            print("⚠️ 未输入指定排查商标，程序将执行品牌通用检测。")

        # 1. 读入 Excel 数据
        excel = ExcelHandler(excel_path)
        excel.read_excel()
        
        url_source_data = excel.get_column_data(COL_URL_SOURCE)
        landing_page_urls = excel.get_column_data(COL_URL)
        url_type_data = excel.get_column_data(COL_URL_TYPE)
        url_thirdsource_data = excel.get_column_data(COL_URL_THIRDSOURCE)
        store_data = excel.get_column_data(COL_STORE)
        store_status_data = excel.get_column_data(COL_SRORE_STAUTS)
        whether_teststore_data = excel.get_column_data(COL_WHETHER_TESTSTORE)
        dept_data = excel.get_column_data(COL_DEPT)
        operator_data = excel.get_column_data(COL_OPERATOR)
        virtual_spu_data = excel.get_column_data(COL_VIRTUAL_SPU)
        virtual_spu_status_data = excel.get_column_data(COL_VIRTUAL_SPUSTAUTS)
        real_spu_data = excel.get_column_data(COL_REAL_SPU)
        real_spu_status_data = excel.get_column_data(COL_REAL_SPUSTAUTS)
        status_data = excel.get_column_data(COL_STAUTS)
        
        # 准备结果容器
        feishu_records = [None] * len(landing_page_urls)
        
        # 控制并发数
        CONCURRENCY_LIMIT = 3
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        # 2. 爬取与审核逻辑
        await crawler.init_browser()
        
        async def process_row(idx):
            try:
                return await _process_row_impl(idx)
            except BaseException as e:
                import traceback
                print(f"❌ 行 {idx+1} 抛出了底层异常 (可能是协程被杀): {type(e).__name__} -> {e}")
                record_fields = {
                    FIELD_OUT_URL_SOURCE: safe_get(url_source_data, idx),
                    FIELD_OUT_URL: safe_get(landing_page_urls, idx),
                    FIELD_OUT_URL_TYPE: safe_get(url_type_data, idx),
                    FIELD_OUT_URL_THIRDSOURCE: safe_get(url_thirdsource_data, idx),
                    FIELD_OUT_STORE: safe_get(store_data, idx),
                    FIELD_OUT_STORE_STATUS: safe_get(store_status_data, idx),
                    FIELD_OUT_WHETHER_TESTSTORE: safe_get(whether_teststore_data, idx),
                    FIELD_OUT_DEPT: safe_get(dept_data, idx),
                    FIELD_OUT_OPERATOR: safe_get(operator_data, idx),
                    FIELD_OUT_VIRTUAL_SPU: safe_get(virtual_spu_data, idx),
                    FIELD_OUT_VIRTUAL_SPU_STATUS: safe_get(virtual_spu_status_data, idx),
                    FIELD_OUT_REAL_SPU: safe_get(real_spu_data, idx),
                    FIELD_OUT_REAL_SPU_STATUS: safe_get(real_spu_status_data, idx),
                    FIELD_OUT_STATUS: safe_get(status_data, idx),
                    FIELD_OUT_IS_VIOLATION: "需人工处理",
                    FIELD_OUT_AUDIT_STATUS: "程序崩溃",
                    FIELD_OUT_PROCESS_METHOD: f"未预料的系统异常: {type(e).__name__}",
                    FIELD_OUT_CHECK_TRADEMARK: target_trademarks,
                    FIELD_OUT_PROCESS_SCALE: excel_filename
                }
                feishu_records[idx] = {"fields": record_fields}
                return record_fields, []

        async def _process_row_impl(idx):
            async with sem:
                # 初始化飞书记录结构
                record_fields = {
                    FIELD_OUT_URL_SOURCE: safe_get(url_source_data, idx),
                    FIELD_OUT_URL: safe_get(landing_page_urls, idx),
                    FIELD_OUT_URL_TYPE: safe_get(url_type_data, idx),
                    FIELD_OUT_URL_THIRDSOURCE: safe_get(url_thirdsource_data, idx),
                    FIELD_OUT_STORE: safe_get(store_data, idx),
                    FIELD_OUT_STORE_STATUS: safe_get(store_status_data, idx),
                    FIELD_OUT_WHETHER_TESTSTORE: safe_get(whether_teststore_data, idx),
                    FIELD_OUT_DEPT: safe_get(dept_data, idx),
                    FIELD_OUT_OPERATOR: safe_get(operator_data, idx),
                    FIELD_OUT_VIRTUAL_SPU: safe_get(virtual_spu_data, idx),
                    FIELD_OUT_VIRTUAL_SPU_STATUS: safe_get(virtual_spu_status_data, idx),
                    FIELD_OUT_REAL_SPU: safe_get(real_spu_data, idx),
                    FIELD_OUT_REAL_SPU_STATUS: safe_get(real_spu_status_data, idx),
                    FIELD_OUT_STATUS: safe_get(status_data, idx),
                    FIELD_OUT_TEXT_CONTENT: "",
                    FIELD_OUT_IMAGE_CONTENT: [],
                    FIELD_OUT_CHECK_TRADEMARK: target_trademarks,
                    FIELD_OUT_PROCESS_SCALE: excel_filename,
                    FIELD_OUT_IS_VIOLATION: "否",
                    FIELD_OUT_PROCESS_METHOD: "无需处理",
                    FIELD_OUT_VIOLATION_IMAGE: [],
                    FIELD_OUT_AUDIT_STATUS: "未执行",
                    FIELD_OUT_VIOLATION_TEXT: "",
                    FIELD_OUT_TEXT_THINK: "",
                    FIELD_OUT_IMAGE_THINK: ""
                }

                raw_url = str(landing_page_urls[idx]).strip()
                # --- URL 深度净化逻辑 ---
                clean_url = raw_url.replace("\n", "").replace("\r", "").strip()
                import re
                match = re.search(r'(https?://[^\s\u4e00-\u9fa5]+)', clean_url)
                url = ""
                if match:
                    url = match.group(1)
                else:
                    if clean_url and clean_url.lower() not in ["未知", "nan", "none", ""]:
                        url = clean_url if clean_url.startswith("http") else "https://" + clean_url
                        url = re.split(r'[\s\u4e00-\u9fa5]', url)[0]

                product_display = safe_get(virtual_spu_data, idx, str(idx+1))

                if url and url.lower() not in ["", "未知", "nan", "none"]:
                    print(f"🔍 [并发执行] 正在处理 [{idx+1}/{len(landing_page_urls)}]: {product_display} -> {url}")
                    crawl_res = await crawler.crawl(url)
                    
                    if not crawl_res.get("error"):
                        c_text = crawl_res["text"][:50000]
                        c_text_lower = c_text.lower()
                        
                        if '{"error":"not authorized."}' in c_text_lower or "access denied" in c_text_lower or "403 forbidden" in c_text_lower:
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "落地页返回授权错误，无法分析"
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "403"
                            print(f"⚠️  落地页授权错误: {c_text[:50]}")
                        elif "店铺关闭" in c_text or "shop is currently unavailable" in c_text_lower or "store is currently unavailable" in c_text_lower or "store is closed" in c_text_lower:
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "发现店铺已关闭"
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "店铺关闭"
                            print(f"⚠️  发现店铺关闭: {c_text[:50]}")
                        elif not crawl_res.get("images"):
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "执行人工核查"
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "抓取异常"
                            print(f"⚠️  未抓取到图片，触发全局异常机制: {url}")
                        else:
                            # 1. 记录文字内容
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "已审核"
                            
                            # 2. 同步图片附件 (抓取到多少图片就上传多少张)
                            image_urls = crawl_res["images"]
                            local_paths = []
                            if image_urls:
                                print(f"🖼️  正在同步图片附件至飞书 (共 {len(image_urls)} 张)...")
                                file_tokens = []
                                for img_url in image_urls:
                                    try:
                                        # 改为先下载到本地，暂不删除，以便下一步 AI 审核报错时能追溯文件大小
                                        local_path = feishu.download_image(img_url)
                                        if local_path:
                                            local_paths.append(local_path)
                                            token = feishu.upload_image_to_feishu(local_path, app_token)
                                            if token:
                                                file_tokens.append({"file_token": token})
                                    except: pass
                                record_fields[FIELD_OUT_IMAGE_CONTENT] = file_tokens
                            
                            # 3. 深度 AI 审核
                            print(f"🤖 正在调用大模型对 {product_display} 进行侵权审核...")
                            
                            res_json_text = {}
                            text_raw_think = "未执行文本审核"
                            
                            # A. 文本侵权审核
                            if text_violation_rule:
                                text_audit_chain = create_text_audit_chain(text_violation_rule)
                                try:
                                    text_raw = await text_audit_chain.ainvoke({"text_content": c_text})
                                except Exception as e:
                                    text_raw = f"{{ '{FIELD_OUT_IS_VIOLATION}': '需人工处理', '{FIELD_OUT_PROCESS_METHOD}': '大模型文本审核超时或异常: {e}' }}"
                                
                                res_json_text_parsed = extract_json_from_text(text_raw)
                                if res_json_text_parsed:
                                    res_json_text = res_json_text_parsed
                                    # Try to extract thinking process
                                    parts = text_raw.split("---最终结论---")
                                    text_raw_think = parts[0].replace("---思考过程---", "").strip() if len(parts) > 1 else "无"
                                else:
                                    res_json_text = {
                                        FIELD_OUT_IS_VIOLATION: "需人工处理",
                                        FIELD_OUT_PROCESS_METHOD: f"解析失败，原始输出: {text_raw[:100]}..."
                                    }
                                    text_raw_think = text_raw
                            else:
                                text_raw_think = "未设置文字侵权规则"
                                res_json_text = { FIELD_OUT_IS_VIOLATION: "否" }

                            res_json_img = {}
                            img_raw_think = "未执行图片审核"
                            
                            # B. 产品图片侵权审核
                            if image_violation_rule:
                                # 传入 local_paths 以供大小探测
                                img_audit_res = await audit_images_async(crawl_res["images"], image_violation_rule, local_paths=local_paths)
                                img_raw_think = img_audit_res.get("think", "无")
                                oversize_indices = img_audit_res.get("oversize_indices", [])
                                
                                res_json_img_parsed = extract_json_from_text(img_audit_res["result"])
                                if res_json_img_parsed:
                                    res_json_img = res_json_img_parsed
                                else:
                                    res_json_img = {
                                        FIELD_OUT_IS_VIOLATION: "需人工处理",
                                        FIELD_OUT_PROCESS_METHOD: f"解析失败，原始输出: {img_data['result'][:100]}..."
                                    }
                            else:
                                img_raw_think = "未设置图片侵权规则"
                                res_json_img = { FIELD_OUT_IS_VIOLATION: "否" }
                                
                            # C. 合并策略
                            text_is_violation = str(res_json_text.get(FIELD_OUT_IS_VIOLATION, "否"))
                            img_is_violation = str(res_json_img.get(FIELD_OUT_IS_VIOLATION, "否"))
                            
                            is_text_yes = ("是" in text_is_violation or "侵权" in text_is_violation)
                            is_img_yes = ("是" in img_is_violation or "侵权" in img_is_violation)
                            is_manual_review = ("需人工处理" in text_is_violation or "需人工处理" in img_is_violation)
                            
                            file_tokens_list = record_fields.get(FIELD_OUT_IMAGE_CONTENT, [])
                            
                            # 获取违规的具体图片序号列表
                            violating_file_tokens = []
                            img_indices = res_json_img.get("VIOLATION_IMAGE_INDICES")
                            if img_indices is None or not isinstance(img_indices, list):
                                # 兜底逻辑：如果 AI 返回结构错误，保守将所有图片视为违规图片
                                violating_file_tokens = file_tokens_list
                            else:
                                for i in img_indices:
                                    if isinstance(i, int):
                                        idx = i - 1
                                        if 0 <= idx < len(file_tokens_list):
                                            violating_file_tokens.append(file_tokens_list[idx])
                                # 如果有违规但索引匹配失败，兜底放入全部图片
                                if is_img_yes and not violating_file_tokens:
                                    violating_file_tokens = file_tokens_list
                            
                            # 基础字段合并 (保留排查商标等字段)
                            record_fields.update(res_json_text)
                            # 提前同步侵权图片，只要后端识别到了就先放进去，后续判定为“否”时再清空
                            record_fields[FIELD_OUT_VIOLATION_IMAGE] = violating_file_tokens
                            
                            # 文本思考过程兜底：如果 JSON 里丢失了该项，用外部的 raw_think
                            if not record_fields.get(FIELD_OUT_TEXT_THINK) or record_fields.get(FIELD_OUT_TEXT_THINK) == "无":
                                record_fields[FIELD_OUT_TEXT_THINK] = text_raw_think
                            
                            # 显式同步图片审核的结论字段 (包含思考过程和排查出的特定品牌特征)
                            if res_json_img:
                                # 图片思考过程兜底：如果 JSON 里有就用 JSON 的，没有就用外部抓取到的 raw_think
                                if res_json_img.get(FIELD_OUT_IMAGE_THINK) and res_json_img[FIELD_OUT_IMAGE_THINK] != "无":
                                    record_fields[FIELD_OUT_IMAGE_THINK] = res_json_img[FIELD_OUT_IMAGE_THINK]
                                else:
                                    record_fields[FIELD_OUT_IMAGE_THINK] = img_raw_think
                                
                                # 商标字段将由用户输入强制覆盖，不在此处拼接
                                
                                # 合并侵权文字
                                img_vt = str(res_json_img.get(FIELD_OUT_VIOLATION_TEXT, "")).strip()
                                txt_vt = str(record_fields.get(FIELD_OUT_VIOLATION_TEXT, "")).strip()
                                
                                is_img_vt_valid = img_vt and img_vt not in ["无", "暂无", "未发现"] and not img_vt.startswith("未发现")
                                is_txt_vt_valid = txt_vt and txt_vt not in ["无", "暂无", "未发现"] and not txt_vt.startswith("未发现")

                                if is_txt_vt_valid and is_img_vt_valid:
                                    record_fields[FIELD_OUT_VIOLATION_TEXT] = f"[文字侵权] {txt_vt}\n[图片侵权] {img_vt}"
                                elif is_img_vt_valid:
                                    record_fields[FIELD_OUT_VIOLATION_TEXT] = img_vt
                                elif is_txt_vt_valid:
                                    record_fields[FIELD_OUT_VIOLATION_TEXT] = txt_vt
                                else:
                                    record_fields[FIELD_OUT_VIOLATION_TEXT] = ""
                            
                            # 严格遵循用户给定的矩阵逻辑控制 "是否侵权" 和 "处理方式"
                            if is_manual_review:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                                record_fields[FIELD_OUT_AUDIT_STATUS] = "需人工核查"
                                pm = ""
                                if is_text_yes:
                                    pm += "[已检出文字违规需删减] "
                                if is_img_yes:
                                    pm += "[已检出部分侵权素材需删除] "

                                # 拼接详细的提示信息（比如大图、解析失败等）
                                detail_pm = ""
                                if "需人工处理" in text_is_violation:
                                    detail_pm += str(res_json_text.get(FIELD_OUT_PROCESS_METHOD, "")) + " "
                                if "需人工处理" in img_is_violation:
                                    detail_pm += str(res_json_img.get(FIELD_OUT_PROCESS_METHOD, ""))
                                record_fields[FIELD_OUT_PROCESS_METHOD] = (pm + detail_pm).strip()
                            elif is_text_yes and is_img_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权素材和关键词"
                            elif is_text_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权关键词"
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = []
                            elif is_img_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权素材"
                            else:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = ""
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = []
                            
                            # --- 补充逻辑：即便整体结论是“是”，如果有大图提示，也要追加到处理方式中 ---
                            if record_fields[FIELD_OUT_IS_VIOLATION] == "是" and oversize_indices:
                                msg = f"；注意：落地页图片第 {'、'.join(map(str, oversize_indices))} 张体积过大未审核，需人工核漏"
                                record_fields[FIELD_OUT_PROCESS_METHOD] += msg
                            
                            # --- 排查数据清洗：保证表格整洁 ---
                            # 如果判定为“否”，则强制清空“侵权文字”一列
                            if record_fields.get(FIELD_OUT_IS_VIOLATION) == "否":
                                record_fields[FIELD_OUT_VIOLATION_TEXT] = ""
                            else:
                                # 即使是“是”或“需处理”，如果 AI 返回了“未发现”之类的废话，也一并清空
                                vt_val = str(record_fields.get(FIELD_OUT_VIOLATION_TEXT, "")).strip()
                                if not vt_val or vt_val in ["无", "暂无", "未发现"] or vt_val.startswith("未发现"):
                                    record_fields[FIELD_OUT_VIOLATION_TEXT] = ""
                            
                            # 强制指回用户输入的排查商标内容
                            record_fields[FIELD_OUT_CHECK_TRADEMARK] = target_trademarks
                            # --- 用户检查：实时打印审核细节 ---
                            print(f"\n" + "="*50)
                            print(f"🕵️  【实时审核报告】: {product_display}")
                            print("-" * 20 + " [文本侵权审核] " + "-" * 20)
                            print(f"💭 思考过程: {text_raw_think}")
                            print(f"📊 文本部分结论: {text_is_violation}")
                            print("-" * 20 + " [图片侵权审核] " + "-" * 20)
                            print(f"💭 思考过程: {img_raw_think}")
                            print(f"📊 图片部分结论: {img_is_violation}")
                            print("-" * 20 + " [合并最终输出] " + "-" * 20)
                            print(f"⚖️ 是否侵权: {record_fields.get(FIELD_OUT_IS_VIOLATION, 'N/A')}")
                            print(f"⚖️ 处理方式: {record_fields.get(FIELD_OUT_PROCESS_METHOD, 'N/A')}")
                            print(f"⚖️ 排查商标: {record_fields.get(FIELD_OUT_CHECK_TRADEMARK, 'N/A')}")
                            print(f"⚖️ 处理尺度: {record_fields.get(FIELD_OUT_PROCESS_SCALE, 'N/A')}")
                            print(f"⚖️ 审核状态: {record_fields.get(FIELD_OUT_AUDIT_STATUS, 'N/A')}")
                            print("="*50 + "\n")
                    else:  
                        error_msg = str(crawl_res['error'])
                        record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                        
                        if "404" in error_msg:
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "404"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 页面返回404，需人工处理"
                        elif "403" in error_msg:
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "403"
                            record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 页面返回403，需人工处理"
                        else:
                            import re
                            safe_err = re.sub(r'[\r\n\t]+', ' ', error_msg).replace('"', '').replace("'", "")[:100]
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "审核错误"
                            record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = f"执行人工核查 (网页完全崩溃: {safe_err})"
                            print(f"\n🚨 [深度追踪] 已成功为崩溃链接捕捉 Timeout: {record_fields[FIELD_OUT_PROCESS_METHOD]}")
                else:
                    record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                    record_fields[FIELD_OUT_AUDIT_STATUS] = "审核错误"
                    record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 无效 URL"
                
                # 打印回传内容的 JSON 格式以供调试
                print(f"\nDEBUG - 即将写回飞书的数据 JSON:\n{json.dumps({'fields': record_fields}, indent=4, ensure_ascii=False)}")
                feishu_records[idx] = {"fields": record_fields}
                print(f"✅ 完成处理: {product_display}")
                return record_fields, local_paths if 'local_paths' in locals() else []
                
        async def worker(target_idx):
            """并发辅助工作者：负责单条记录的完整生命周期（处理+写回）"""
            # 1. 逐条执行处理任务
            processed_fields, local_paths = await process_row(target_idx)
            
            # 2. 状态检查与实时同步
            current_record = {"fields": processed_fields} if processed_fields else None
            
            if current_record is not None:
                print(f"\n📤 正在将第 {target_idx + 1} 条记录实时同步至飞书...")
                success = feishu.add_batch_records_to_bitable(app_token, table_id, [current_record])
                if success:
                    print(f"   ✅ 第 {target_idx + 1} 条记录同步成功")
                    # 上传成功后再清理本地图片
                    if local_paths:
                        import os
                        for p in local_paths:
                            try:
                                if os.path.exists(p): os.remove(p)
                            except: pass
                else:
                    print(f"   ❌ 第 {target_idx + 1} 条记录同步失败")
            else:
                print(f"   ⚠️  警告: 第 {target_idx + 1} 条记录处理结果为空，跳过同步。")

        try:
            total_records = len(landing_page_urls)
            _start_idx = max(0, start_row - 1)
            
            print(f"\n🚀 启动实时并发引擎 (并发度: {CONCURRENCY_LIMIT})，实时同步启动...")
            
            if _start_idx > 0:
                print(f"⏩ 已跳过前 {_start_idx} 条记录，直接从第 {start_row} 条继续执行...")
                
            # 创建并发任务流
            tasks = [worker(i) for i in range(_start_idx, total_records)]
            if tasks:
                await asyncio.gather(*tasks)
                    
        finally:
            await crawler.close_browser()
            print(f"\n✨ 全部审核与实时飞书同步完成！")

    except Exception as e:
        print(f"❌ 流程运行失败: {e}")

if __name__ == "__main__":
    asyncio.run(main_async())
