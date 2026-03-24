import asyncio
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
COL_URL = "落地页链接"
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
FIELD_OUT_URL = '落地页链接'
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

# ================= 固定的内置审核规则 =================

FIXED_TEXT_VIOLATION_RULE = """
#### 1. 刀具类文本审核
- **品类定义界定：** 仅允许宣传“厨房烹饪”、“办公/文具”、“儿童玩具（塑料/纸质）”三类用途；
- **武器/防身宣传违规（必须拦截）：** 文本中严禁出现“攻击（attack）”、“伤害”、“自卫/防身（self-defense/defense）”、“战术（tactical）”、“生存（survival）”、“户外露营（outdoor）”（若语境暗示攻击性）等词汇；一旦文案宣传上述内容，视为违规。
- **暗示违规：** 严禁提及“军用”、“特种兵”、“血槽”、“刺穿”等暗示高杀伤力的描述，视为违规。

#### 2. 武器/刑具类违规
- **黑名单关键词（一票否决）：** 标题或描述中出现“Weapon（武器）”、“Stun/Taser/Electric shock（电击枪/器/棒）”、“Pepper spray（防狼喷雾）”、“Mace（催泪剂/狼牙棒）”、“Knuckles（指虎）”、“Slingshot（弹弓）”、“Baton（警棍）”、“Handcuffs（手铐）”、“Crossbow/Compound bow（弩/复合弓）”、“Metal dart（金属飞镖）”等词汇，判定为违规。

#### 3. 常见品牌侵权排查
- 如果遇到标题、描述中包含世界知名大牌如"Nike", "Adidas", "Gucci", "Apple"等，请严格辨别是否为高仿假冒，也可单独标注提示侵权。
"""

FIXED_IMAGE_VIOLATION_RULE = """
#### 1. 刀具类图片审核
- **违规特征（禁止出现）：** 血槽、锯齿、战术涂层（迷彩/全黑）、快速开刀鳍（Flips）、指环扣；折叠刀（非办公用）、蝴蝶刀、户外求生刀。
- **违规场景（禁止出现）：** 切割硬物（非食材/纸张）、刺穿动作演示；背景为户外战术、野外求生场景。
- **合规范围（仅允许）：** 清晰可辨的厨房刀具（透明包装）、陶瓷刀、美工刀、塑料萝卜刀。

#### 2. 武器类图片审核
- **违规产品（禁止出现）：** 指虎（含戒指/饰品形状）、弹弓（含激光/红外/强力型）、飞镖（含金属飞镖）、狼牙棒（Mace club）、甩棍/警棍、弩/复合弓（Crossbow/Compound bow）。
- **违规功能（禁止出现）：** 外形似日常用品（手电筒/笔/梳子）但露出电击探头、电弧或隐藏刀刃；防狼喷雾（Pepper spray）、电击枪/电击器/电击棒（Taser/Electric shock device）、催泪剂（Mace）。
- **违规演示（禁止出现）：** 击破玻璃、电击人体/模型、攻击性握持姿态、喷射刺激性气体/液体。

#### 3. 刑具类图片审核
- **违规产品（禁止出现）：** 手铐（金属/硬质）、锁链、脚镣、枷锁或其他限制人身自由的器具演示或真实展示。
"""

# 大模型 API 配置
API_KEY = "78cbeed3-eea7-4832-ba9c-b3fefeff316f"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

# 模型配置
MODEL_TEXT_AUDIT = "doubao-seed-1-8-251228"
MODEL_IMAGE_AUDIT = "doubao-seed-1-8-251228"

# 初始化模型
llm_text = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_TEXT_AUDIT, temperature=0.0, timeout=60)
llm_vision = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_IMAGE_AUDIT, temperature=0.0)

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
    system_prompt = "你将扮演**专业商品落地页审核员（文本侵权专项）**，依据用户提供的《侵权审核规则》对商品文本字段进行**严格的合规性审核**。"
    user_prompt = f"""
请审核以下落地页文本：
<商品文本>
{{text_content}}
</商品文本>

### 一、审核规则（必须严格执行）
用户定义的文本侵权审核规则如下：
{violation_rule}

### 二、审核流程（按步骤执行）
1. **内容提取与状态判定：** 
   - 如果文本为空、显示“404/无法打开”、或者内容包含“{{{{error":"Not authorized."}}}}”、“Access Denied”、“Forbidden”等授权报错，判定结果为“无需处理”。
2. **规则匹配：** 扫描文本，若违反用户定义的文本侵权规则（例如出现了禁用的品牌名、侵权词汇等），判定为“侵权违规”。
3. **合格判定：** 若不涉及任何上述违规内容且内容非报错信息，判定为“合规通过”。

### 三、约束规则
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）： 

---思考过程---
[详细分析步骤]

---最终结论---
{{{{
  "{FIELD_OUT_CHECK_TRADEMARK}": "[提取到的疑似侵权商标或敏感词，如果没有填'无']",
  "{FIELD_OUT_IS_VIOLATION}": "必须从 [是, 否, 需人工处理] 中精准选择一个"
}}}}
"""
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", user_prompt)
    ])
    return prompt | llm_text | StrOutputParser()


async def audit_images_async(image_urls: List[str], violation_rule: str):
    """调用视觉模型审核图片 (图片侵权审核)"""
    if not image_urls:
        return {
            "think": "未提供产品图片，无法进行视觉分析",
            "result": f'{{ "{FIELD_OUT_IMAGE_RESULT}": "无需处理", "{FIELD_OUT_IMAGE_THINK}": "落地页未通过爬虫抓取到有效图片，跳过 AI 视觉审核" }}'
        }
    
    prompt_text = f"""你将扮演**专业商品图片审核员（图片侵权专项）**，依据《侵权审核规则》对商品图片进行精准审核。

### 一、审核规则（必严格遵循）
用户定义的图片侵权审核规则如下：
{violation_rule}

### 二、审核流程（按步骤执行）
1. **图片信息提取：** 识别图片中的物体核心特征、品牌Logo、外观设计、卡通形象等。
2. **状态判定：** 若图片内容显示为“404/无法显示/错误页面”，归类为“无需处理”。
3. **合格判定：** 若图片内容违反了用户设定的侵权规则（例如包含未经授权的商标、侵权的图案设计等），判定为“侵权违规”；否则判定为“合规通过”。

### 三、约束规则
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）：

---思考过程---
[详细分析步骤]

---最终结论---
{{
  "{FIELD_OUT_CHECK_TRADEMARK}": "[提取到的疑似侵权物体、形状或商标，如果没有填'无']",
  "{FIELD_OUT_IS_VIOLATION}": "必须从 [是, 否, 需人工处理] 中精准选择一个"
}}
"""
    content = [{"type": "text", "text": prompt_text}]
    for url in image_urls:
        content.append({
            "type": "image_url",
            "image_url": {
                "url": url
            }
        })
    
    try:
        response = await llm_vision.ainvoke([("user", content)])
        full_text = response.content
        
        think = full_text
        result = full_text
        if "---最终结论---" in full_text:
            parts = full_text.split("---最终结论---")
            think = parts[0].replace("---思考过程---", "").strip()
            result = parts[1].strip()
        
        return {"think": think, "result": result}
    except Exception as e:
        err_str = str(e)
        if "OversizeImage" in err_str:
            return {
                "think": f"图片体积过大(超过10MB)，AI 无法处理: {err_str}",
                "result": f'{{ "{FIELD_OUT_CHECK_TRADEMARK}": "无", "{FIELD_OUT_IS_VIOLATION}": "需人工处理", "{FIELD_OUT_PROCESS_METHOD}": "落地页包含超大体积图片(>10MB)，超出 AI 处理极限，需进入落地页手动审核" }}'
            }
        if "Timeout while downloading" in err_str:
            return {
                "think": f"AI 服务端下载图片超时: {err_str}",
                "result": f'{{ "{FIELD_OUT_CHECK_TRADEMARK}": "无", "{FIELD_OUT_IS_VIOLATION}": "需人工处理", "{FIELD_OUT_PROCESS_METHOD}": "AI 服务商调取图片URL超时(CDN响应慢或屏蔽)，无法自动获取图片，需手动审核" }}'
            }
        if "400" in err_str:
             return {
                "think": f"AI 接口参数错误或拒绝处理: {err_str}",
                "result": f'{{ "{FIELD_OUT_CHECK_TRADEMARK}": "无", "{FIELD_OUT_IS_VIOLATION}": "需人工处理", "{FIELD_OUT_PROCESS_METHOD}": "AI 接口返回400错误(可能涉及无法识别的格式)，建议手动核查" }}'
            }
            
        return {"think": f"图片审核过程报错: {e}", "result": f'{{ "{FIELD_OUT_CHECK_TRADEMARK}": "无", "{FIELD_OUT_IS_VIOLATION}": "需人工处理", "{FIELD_OUT_PROCESS_METHOD}": "审核失败，需排查" }}'}

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
            
        # 挂载内置规则
        text_violation_rule = FIXED_TEXT_VIOLATION_RULE
        image_violation_rule = FIXED_IMAGE_VIOLATION_RULE
        
        # 提取 Excel 文件名（去除后缀，如 "待审核商品名单"）
        import os
        excel_filename = os.path.splitext(os.path.basename(excel_path))[0]
        
        # 将用户指定的特定商标注入至模型最优先级判定规则
        if target_trademarks:
            trademark_appendix = f"\n\n#### 4. 用户指定特别排查商标/特征（最高优先级）\n- **重点排查：** 请严密核查商品中是否出现了以下特定商标、Logo图案或其高仿变异词：【{target_trademarks}】\n- 如果发现，必须判定为违规，并在 '{FIELD_OUT_CHECK_TRADEMARK}' 字段中准确提取出所识别到的商标名称。"
            text_violation_rule += trademark_appendix
            image_violation_rule += trademark_appendix

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
        
        # 控制并发数（建议设置为 3-5，防止浏览器占用过多内存抛出异常或触发 API 频控）
        CONCURRENCY_LIMIT = 3
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        # 2. 爬取与审核逻辑
        await crawler.init_browser()
        
        async def process_row(idx):
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
                    FIELD_OUT_CHECK_TRADEMARK: "",
                    FIELD_OUT_PROCESS_SCALE: excel_filename,
                    FIELD_OUT_IS_VIOLATION: "否",
                    FIELD_OUT_PROCESS_METHOD: "无需处理",
                    FIELD_OUT_VIOLATION_IMAGE: [],
                    FIELD_OUT_AUDIT_STATUS: "未执行"
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
                            record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "落地页返回授权错误，无法分析"
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "403"
                            print(f"⚠️  落地页授权错误: {c_text[:50]}")
                        elif "店铺关闭" in c_text or "shop is currently unavailable" in c_text_lower or "store is currently unavailable" in c_text_lower or "store is closed" in c_text_lower:
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "发现店铺已关闭"
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "店铺关闭"
                            print(f"⚠️  发现店铺关闭: {c_text[:50]}")
                        else:
                            # 1. 记录文字内容
                            record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "已审核"
                            
                            # 2. 同步图片附件 (抓取到多少图片就上传多少张)
                            image_urls = crawl_res["images"]
                            if image_urls:
                                print(f"🖼️  正在同步图片附件至飞书 (共 {len(image_urls)} 张)...")
                                file_tokens = []
                                for img_url in image_urls:
                                    try:
                                        token = feishu.download_and_upload_image(img_url, app_token)
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
                                text_raw = await text_audit_chain.ainvoke({"text_content": c_text})
                                
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
                                img_data = await audit_images_async(crawl_res["images"], image_violation_rule)
                                img_raw_think = img_data.get("think", "无")
                                
                                res_json_img_parsed = extract_json_from_text(img_data["result"])
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
                            
                            # 基础字段合并 (保留排查商标等字段)
                            record_fields.update(res_json_text)
                            if res_json_img.get(FIELD_OUT_CHECK_TRADEMARK) and res_json_img.get(FIELD_OUT_CHECK_TRADEMARK) != "无":
                                # 若图片也有特别的排查结果，做拼接
                                cur_tm = record_fields.get(FIELD_OUT_CHECK_TRADEMARK, "")
                                record_fields[FIELD_OUT_CHECK_TRADEMARK] = f"{cur_tm} | {res_json_img[FIELD_OUT_CHECK_TRADEMARK]}" if cur_tm and cur_tm != "无" else res_json_img[FIELD_OUT_CHECK_TRADEMARK]
                            
                            # 严格遵循用户给定的矩阵逻辑控制 "是否侵权" 和 "处理方式"
                            if is_text_yes and is_img_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权素材和关键词"
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = file_tokens_list
                            elif is_text_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权关键词"
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = []
                            elif is_img_yes:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "是"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = "删除侵权素材"
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = file_tokens_list
                            elif is_manual_review:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "需人工处理"
                                record_fields[FIELD_OUT_AUDIT_STATUS] = "审核错误"
                                pm = ""
                                if "需人工处理" in text_is_violation:
                                    pm += res_json_text.get(FIELD_OUT_PROCESS_METHOD, "")
                                if "需人工处理" in img_is_violation:
                                    pm += " " + res_json_img.get(FIELD_OUT_PROCESS_METHOD, "")
                                record_fields[FIELD_OUT_PROCESS_METHOD] = pm.strip()
                            else:
                                record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                                record_fields[FIELD_OUT_PROCESS_METHOD] = ""
                                record_fields[FIELD_OUT_VIOLATION_IMAGE] = []
                            
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
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 页面返回404"
                        elif "403" in error_msg:
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "403"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 页面返回403"
                        else:
                            record_fields[FIELD_OUT_AUDIT_STATUS] = "审核错误"
                            record_fields[FIELD_OUT_PROCESS_METHOD] = f"网页抓取失败: {error_msg}"
                else:
                    record_fields[FIELD_OUT_IS_VIOLATION] = "否"
                    record_fields[FIELD_OUT_AUDIT_STATUS] = "审核错误"
                    record_fields[FIELD_OUT_PROCESS_METHOD] = "跳过: 无效 URL"
                
                feishu_records[idx] = {"fields": record_fields}
                print(f"✅ 完成处理: {product_display}")
                
        try:
            # 创建所有的并发任务并执行
            tasks = [process_row(idx) for idx in range(len(landing_page_urls))]
            print(f"\n🚀 启动并发处理引擎，最大并发线程数：{CONCURRENCY_LIMIT}")
            await asyncio.gather(*tasks)
            
            # 过滤掉可能出现的 None (尽管通过了 try...except 理论上不会出现，但为安全起见)
            feishu_records = [r for r in feishu_records if r is not None]
        finally:
            await crawler.close_browser()

        # 3. 批量写回飞书
        if feishu_records:
            print(f"\n📤 正在将 {len(feishu_records)} 条记录同步至飞书多维表格...")
            BATCH_SIZE = 50
            for i in range(0, len(feishu_records), BATCH_SIZE):
                chunk = feishu_records[i : i + BATCH_SIZE]
                print(f"   正在同步第 {i//BATCH_SIZE + 1} 批...")
                success = feishu.add_batch_records_to_bitable(app_token, table_id, chunk)
                if success:
                    print(f"   ✅ 第 {i//BATCH_SIZE + 1} 批同步成功")
                else:
                    print(f"   ❌ 第 {i//BATCH_SIZE + 1} 批同步失败")
                await asyncio.sleep(1)
            print(f"\n✨ 全部审核与飞书同步完成！")
        else:
            print(f"\n✨ 没有需要同步的记录。")

    except Exception as e:
        print(f"❌ 流程运行失败: {e}")

if __name__ == "__main__":
    asyncio.run(main_async())
