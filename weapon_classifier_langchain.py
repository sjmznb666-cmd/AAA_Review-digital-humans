import openpyxl.worksheet.datavalidation
# Monkey patch to solve openpyxl DataValidation issue
old_init = openpyxl.worksheet.datavalidation.DataValidation.__init__
def new_init(self, *args, **kwargs):
    kwargs.pop('id', None)
    old_init(self, *args, **kwargs)
openpyxl.worksheet.datavalidation.DataValidation.__init__ = new_init

import asyncio
import json
import base64
import requests
from typing import List, Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from feishu_api import FeishuAPI
from excel_handler import ExcelHandler
from landing_page_crawler import LandingPageCrawler

# ================= 配置区域 =================
# EXCEL_FILE_PATH = r"D:\Downloads\售后审核列表_落地页审核_2026-02-27.xlsx" # 改为用户输入
# OUTPUT_EXCEL_PATH = "products_audited_langchain.xlsx" # 切换至飞书，Excel可选作备份

# 飞书应用配置 (来自 p308.js)
FEISHU_APP_ID = 'cli_a81fe4ed0730900c'
FEISHU_APP_SECRET = 'wffGOfPCemJA9I3b86e9rlStWWC7OAsf'

# Excel 输入列名定义
COL_PRODUCT_NAME = "产品名称"
COL_URL = "落地页链接"
COL_DEPT = "部门"
COL_VIRTUAL_SKU = "虚拟SKU编号"
COL_REAL_SKU = "真实SKU编号"
COL_OPERATOR = "运营"
COL_ORDER_COUNT = "订单数" 

# 飞书 Bitable 输出列名定义 (需与多维表格完全一致)
FIELD_OUT_PRODUCT_NAME = '产品名称'
FIELD_OUT_URL = '落地页链接'
FIELD_OUT_DEPT = '部门'
FIELD_OUT_VIRTUAL_SKU = '虚拟SKU编号'
FIELD_OUT_REAL_SKU = '真实SKU编号'
FIELD_OUT_OPERATOR = '运营'
FIELD_OUT_ORDER_COUNT = '订单数'

FIELD_OUT_TEXT_CONTENT = '落地页文字内容'
FIELD_OUT_IMAGE_CONTENT = '落地页图片内容'
FIELD_OUT_JUDGE_LABEL = '审核标签'
FIELD_OUT_PRODUCT_RESULT = '产品审核结果'
FIELD_OUT_PRODUCT_THINK = '产品审核思考过程'
FIELD_OUT_PROPAGANDA_RESULT = '宣传审核结果'
FIELD_OUT_PROPAGANDA_THINK = '宣传审核思考过程'

# 大模型 API 配置
API_KEY = "78cbeed3-eea7-4832-ba9c-b3fefeff316f"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

# 模型配置
MODEL_CLASSIFY = "doubao-seed-2-0-mini-260215"
MODEL_TEXT_AUDIT = "doubao-seed-2-0-mini-260215"
MODEL_IMAGE_AUDIT = "doubao-seed-2-0-pro-260215"

MAX_WORKERS = 5

# 初始化模型 (增加 timeout 防止挂起)
llm_classify = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_CLASSIFY, temperature=0.0, timeout=60)
llm_text = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_TEXT_AUDIT, temperature=0.0, timeout=60)
llm_vision = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_IMAGE_AUDIT, temperature=0.0)

def detect_browser_path():
    """广域探测本地浏览器路径 (C/D/E 盘及 AppData)"""
    import os
    
    # 获取用户本地 AppData 这种特殊的 Chrome 静默安装目录
    user_local_chrome = os.path.join(os.environ.get('LOCALAPPDATA', ''), r"Google\Chrome\Application\chrome.exe")
    
    possible_paths = [
        # --- C 盘官方安装 ---
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        user_local_chrome,
        
        # --- D 盘常用安装 ---
        r"D:\Program Files\Google\Chrome\Application\chrome.exe",
        r"D:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        
        # --- E 盘备选 ---
        r"E:\Program Files\Google\Chrome\Application\chrome.exe",
        r"E:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        
        # --- 备选 Edge (后备系统) ---
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]
    
    for p in possible_paths:
        if p and os.path.exists(p):
            print(f"✅ 系统已就绪，定位到浏览器: {p}")
            return p
            
    print("⚠️  警告：未能在默认目录发现浏览器，尝试使用系统默认调用...")
    return None

# 初始化工具类
feishu = FeishuAPI(FEISHU_APP_ID, FEISHU_APP_SECRET)
browser_path = detect_browser_path()
crawler = LandingPageCrawler(headless=True, executable_path=browser_path)

# ================= LangChain 构建 =================

def create_classify_chain():
    prompt = ChatPromptTemplate.from_messages([
        ("system", "你是一个严谨的商品审核助手。输出必须是'是刀具武器类'或'非刀具武器类'。"),
        ("user", "判断以下商品是否属于“刀具”或“武器”类：\n商品名称：{product_name}\n\n说明：1. 刀具/武器包含管制刀具、枪支、弓弩、匕首、警用装备、防身武器等。2. 日常菜刀等请依据语境判断。请严格只输出'是刀具武器类'或'非刀具武器类'。")
    ])
    return prompt | llm_classify | StrOutputParser()

def extract_json_from_text(text: str) -> dict:
    """从文本中提取并解析 JSON 对象"""
    if not text or not isinstance(text, str):
        return {}
    
    # 清理 Markdown 标签
    clean_text = text.replace("```json", "").replace("```", "").strip()
    
    try:
        # 定位第一个 { 和 最后一个 }
        start = clean_text.find("{")
        end = clean_text.rfind("}")
        if start != -1 and end != -1:
            json_str = clean_text[start:end+1]
            return json.loads(json_str)
    except:
        pass
    
    return {}

def create_text_audit_chain():
    """文本审核 Chain (专业商品文本审核员模式)"""
    system_prompt = "你将扮演**专业商品落地页审核员（文本专项）**，依据《审核规则》对商品文本字段进行**100%合规性审核**。核心目标是精准识别违规词汇与潜在攻击性用途导向。"
    user_prompt = f"""
请审核以下落地页文本：
<商品文本>
{{text_content}}
</商品文本>

### 一、审核规则（必须严格执行）
#### 1. 刀具类文本审核
- **品类定义界定：** 仅允许宣传**“厨房烹饪”、“办公/文具”、“儿童玩具（塑料/纸质）”**三类用途；
- **武器/防身宣传违规（必须拦截）：** 文本中严禁出现**“攻击（attack）”、“伤害”、“自卫/防身（self-defense/defense）”、“战术（tactical）”、“生存（survival）”、“户外露营（outdoor）”**（若语境暗示攻击性）等词汇；一旦文案宣传上述内容，视为【武器宣传违规】。
- **暗示违规：** 严禁提及**“军用”、“特种兵”、“血槽”、“刺穿”**等暗示高杀杀伤力的描述，视为"武器宣传违规"。

#### 2. 武器/刑具类违规
- **黑名单关键词（一票否决）：** 标题或描述中出现**“Weapon（武器）”、“Stun/Taser/Electric shock（电击枪/器/棒）”、“Pepper spray（防狼喷雾）”、“Mace（催泪剂/狼牙棒）”、“Knuckles（指虎）”、“Slingshot（弹弓）”、“Baton（警棍）”、“Handcuffs（手铐）”、“Crossbow/Compound bow（弩/复合弓）”、“Metal dart（金属飞镖）”**等词汇，判定为【武器宣传违规】。
- **打击、穿刺与隐蔽类工具违规：** 严禁宣传用于打击（如甩棍、鞭子）、穿刺（如军用刺刀、峨眉刺）以及具有隐蔽伪装特性的工具（如拐杖剑、皮带刀），一经发现判定为【武器宣传违规】。
- **攻击性动作识别：** 识别描述中出现的“破坏/暴力”相关功能演示，判定为武器宣传违规。

#### 3. 附加判定原则
- **武器优先原则：** 当产品同时具备刀具（有刃）和武器（暗器/战术）双重属性时，强制判定为"武器宣传违规"。
- **产品本质防误判：** 增加对产品本质的逻辑校验，如识别到“贴纸(Sticker)”、“印花”、“模型玩具(无杀伤力)”等关键词，明确产品仅是周边装饰或2D图案而非真实刀具武器时，不视为违规，判定为“宣传通过”。

### 二、审核流程（按步骤执行）
1. **规则匹配：** 扫描文本，命中上述违规词汇或用途，判定为“武器宣传违规”。
2. **合格判定：** 若不涉及任何上述违规内容，判定为“宣传通过”。

### 三、输出要求
输出请严格遵循以下格式：
---思考过程---
[详细分析步骤，包括是否命中违规词汇、违规类型等]

---最终结论---
[结果+理由，如“武器宣传违规，出现手铐等违规词汇”]
"""
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", user_prompt)
    ])
    return prompt | llm_text | StrOutputParser()

def create_image_audit_chain():
    """图片审核 Chain (多模态)""" 
    return llm_vision

# ================= 业务逻辑 =================

async def audit_images_async(image_urls: List[str]):
    """调用视觉模型审核图片 (专业商品图片审核员模式)"""
    if not image_urls:
        return {
            "think": "未提供产品图片，无法进行视觉分析",
            "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "无需处理", "{FIELD_OUT_PRODUCT_THINK}": "落地页未通过爬虫抓取到有效图片，跳过 AI 视觉审核" }}'
        }
    
    # 构造多模态消息
    prompt_text = f"""你将扮演专业商品图片审核员，依据《审核规则》对商品图片进行精准审核，核心目标是识别图片中的物体特征、材质、演示场景及图片内文字内容，输出合规性判定结果及整改方向。

以下是需要审核的商品图片信息：
<product_image_info>
{{{{PRODUCT_IMAGE_INFO}}}}
</product_image_info>

### 一、审核规则（必严格遵循）
#### 1. 刀具类图片审核
- **违规特征（禁止出现）**：血槽、锯齿、战术涂层（迷彩/全黑）、快速开刀鳍（Flips）、指环扣；折叠刀（非办公用）、蝴蝶刀、户外求生刀。
- **违规场景（禁止出现）**：切割硬物（非食材/纸张）、刺穿动作演示；背景为户外战术、野外求生场景。
- **合规范围（仅允许）**：清晰可辨的厨房刀具（透明包装）、陶瓷刀、美工刀、塑料萝卜刀。

#### 2. 武器类图片审核
- **违规产品（禁止出现）**：指虎（含戒指/饰品形状）、弹弓（含激光/红外/强力型）、飞镖（含金属飞镖）、狼牙棒（Mace club）、甩棍/警棍、弩/复合弓（Crossbow/Compound bow）。
- **打击、穿刺与隐蔽类违规（禁止出现）**：专用于打击的钝器或长鞭、专用于穿刺的尖锐武器（如峨眉刺、军用刺刀），以及伪装成日常用品（如拐杖伞中藏剑、皮带刃、梳子刀等）的隐蔽类危险工具。
- **违规功能（禁止出现）**：防狼喷雾（Pepper spray）、电击枪/电击器/电击棒（Taser/Electric shock device）、催泪剂（Mace）。
- **违规演示（禁止出现）**：击破玻璃、电击人体/模型、攻击性握持姿态、喷射刺激性气体/液体。
- **攻击性动作识别：** 识别描述或画面中出现的“破坏/暴力”相关功能演示，均视为违规。

#### 3. 刑具类图片审核
- **违规产品（禁止出现）**：手铐（金属/硬质）、锁链、脚镣、枷锁或其他限制人身自由的器具演示或真实展示。

#### 4. 图片内文字审核补充规则
- 若图片中出现文字，需单独提取并审核：若文字内容涉及武器宣传（如“威力强劲”“攻击必备”等暗示武器功能或用途的描述），则判定为“武器宣传违规”。

#### 5. 附加判定原则
- **武器优先原则：** 当产品同时具备刀具（有刃）和武器（暗器/战术）双重属性时，强制判定为"武器违规"。
- **产品本质防误判：** 增加对图片内容与产品本质的逻辑校验，如画面或文字表明实质为“贴纸(Sticker)”、“印花图案”、“无攻击性模型/玩具装饰”，即使图案涉及真实刀具/武器特征，也不视为违规，判定为“产品通过”。
- **视觉文本冲突处理：** 以图片实物形态为判定核心。当图片内的文字描述与实物的视觉表现发生冲突时（例如夸大宣传或规避描述），必须优先以实物的清晰物理形态作为判定是否违规的最终依据。

### 二、审核流程（按步骤执行）
1. **图片信息提取**：识别图片中的物体类型、核心特征、演示动作、背景场景及图片内所有文字内容。
2. **分权判定**：
   - 若图片物体违反刀具类规则，标记“刀具违规”；
   - 若图片物体违反武器类规则，标记“武器违规”；
   - 若图片物体违反刑具类规则，标记“刑具违规”；
   - 若图片内文字违反武器宣传规则，标记“武器宣传违规”；
   - 若图片物体及文字均无违规，标记“产品通过”；
   - 若图片物体违规且文字也违规（如武器违规+武器宣传违规），需同时输出两类违规结论。

### 三、输出格式要求（严格遵循）
直接输出符合要求的结论，格式为“结论+理由”。若存在多项违规，需依次列出结论并分别说明理由。示例：
- 仅物体违规：“刀具违规，出现锯齿特征”
- 仅文字违规：“武器宣传违规，文字含‘强力攻击’等宣传内容”
- 物体+文字均违规：“武器违规（出现电击棒），武器宣传违规（文字含‘电击威力大’）”
- 无违规：“产品通过，未发现违规特征及宣传内容”

请在<判断>标签内输出最终审核结果，不得包含任何额外内容。
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
        
        # 提取 <判断> 标签内的内容
        content_in_tag = ""
        import re
        match = re.search(r'<判断>(.*?)</判断>', full_text, re.DOTALL)
        if match:
            content_in_tag = match.group(1).strip()
        else:
            # 如果没找到标签，退而求其次使用全文
            content_in_tag = full_text.strip()
            
        # 按照新格式解析：结论 + 理由
        # 将第一个逗号或顿号前的内容视作结果，全文字视作理由（思考过程）
        # 示例："刀具违规，出现锯齿特征"
        if "，" in content_in_tag:
            result_val = content_in_tag.split("，")[0].strip()
        elif "," in content_in_tag:
            result_val = content_in_tag.split(",")[0].strip()
        else:
            result_val = content_in_tag # 只有结论的情况
            
        return {"think": content_in_tag, "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "{result_val}", "{FIELD_OUT_PRODUCT_THINK}": "{content_in_tag}" }}'}
    except Exception as e:
        err_str = str(e)
        # 专门处理图片过大的报错 (OversizeImage)
        if "OversizeImage" in err_str:
            return {
                "think": f"图片体积过大(超过10MB)，AI 无法处理: {err_str}",
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "落地页包含超大体积图片(>10MB)，超出 AI 处理极限，需进入落地页手动审核" }}'
            }
        # 处理 AI 服务端下载图片超时
        if "Timeout while downloading" in err_str:
            return {
                "think": f"AI 服务端下载图片超时: {err_str}",
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "AI 服务商调取图片URL超时(CDN响应慢或屏蔽)，无法自动获取图片，需手动审核" }}'
            }
        
        # 处理其他常见的 400 错误
        if "400" in err_str:
             return {
                "think": f"AI 接口参数错误或拒绝处理: {err_str}",
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "AI 接口返回400错误(可能涉及无法识别的格式)，建议手动核查" }}'
            }
            
        return {"think": f"图片审核过程报错: {e}", "result": "审核失败"}


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

        # 1. 读入 Excel 数据
        excel = ExcelHandler(excel_path)
        excel.read_excel()
        product_names = excel.get_column_data(COL_PRODUCT_NAME)
        landing_page_urls = excel.get_column_data(COL_URL)
        dept_data = excel.get_column_data(COL_DEPT)
        virtual_sku_data = excel.get_column_data(COL_VIRTUAL_SKU)
        real_sku_data = excel.get_column_data(COL_REAL_SKU)
        operator_data = excel.get_column_data(COL_OPERATOR)
        order_count_data = excel.get_column_data(COL_ORDER_COUNT)
        
        # 2. 意图分类 (平衡速度与稳定性)
        print(f"🚀 正在快速识别分类 (样本数: {len(product_names)})...")
        classify_chain = create_classify_chain()
        cleaned_classifications = []
        BATCH_CLASSIFY = 100 
        for i in range(0, len(product_names), BATCH_CLASSIFY):
            chunk = product_names[i : i + BATCH_CLASSIFY]
            print(f"   进度: {i}/{len(product_names)} ...")
            try:
                # 尝试极速模式 (并发 30)
                batch_res = await classify_chain.abatch(
                    [{"product_name": n} for n in chunk],
                    config={"max_concurrency": 30} 
                )
                # 归一化处理：防止模型输出“是”或“否”
                def normalize_label(label):
                    label = label.strip().replace('。', '')
                    if label == "是" or "是刀具" in label: return "是刀具武器类"
                    if label == "否" or "非刀具" in label: return "非刀具武器类"
                    return label
                cleaned_classifications.extend([normalize_label(c) for c in batch_res])
            except Exception as e:
                print(f"   ⚠️  批量(100)超时，正在以稳健模式(10并发)重试该小节...")
                # 降级为稳健模式，依然保持 10 并发
                try:
                    retry_res = await classify_chain.abatch(
                        [{"product_name": n} for n in chunk],
                        config={"max_concurrency": 10}
                    )
                    cleaned_classifications.extend([normalize_label(c) for c in retry_res])
                except:
                    print(f"   ❌ 稳健模式依然失败，该 100 条记录默认记为'非刀具武器类'")
                    cleaned_classifications.extend(["非刀具武器类"] * len(chunk))
        print(f"✅ 分类完成！共计 {len(cleaned_classifications)} 条记录。")

        # 准备结果容器
        feishu_records = []
        
        # 3. 爬取与审核逻辑
        await crawler.init_browser()
        try:
            for idx, res in enumerate(cleaned_classifications):
                # 即使没抓取也初始化飞书记录结构，包含所有原始列数据
                record_fields = {
                    FIELD_OUT_PRODUCT_NAME: product_names[idx],
                    FIELD_OUT_URL: landing_page_urls[idx],
                    FIELD_OUT_DEPT: dept_data[idx],
                    FIELD_OUT_VIRTUAL_SKU: virtual_sku_data[idx],
                    FIELD_OUT_REAL_SKU: real_sku_data[idx],
                    FIELD_OUT_OPERATOR: operator_data[idx],
                    FIELD_OUT_ORDER_COUNT: order_count_data[idx],
                    FIELD_OUT_JUDGE_LABEL: res,
                    FIELD_OUT_PRODUCT_RESULT: "产品通过" if res == "非刀具武器类" else "", 
                }
                # 初始化其他审核相关字段
                record_fields[FIELD_OUT_PRODUCT_THINK] = "未命中" if res == "非刀具武器类" else ""
                record_fields[FIELD_OUT_PROPAGANDA_RESULT] = "宣传通过" if res == "非刀具武器类" else ""
                record_fields[FIELD_OUT_PROPAGANDA_THINK] = "未命中" if res == "非刀具武器类" else ""
                record_fields[FIELD_OUT_TEXT_CONTENT] = ""
                record_fields[FIELD_OUT_IMAGE_CONTENT] = [] 

                if res == "是刀具武器类":
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

                    if url and url.lower() not in ["", "未知", "nan", "none"]:
                        print(f"🔍 发现疑似违规，正在抓取数据 [{idx+1}/{len(product_names)}]: {product_names[idx]} -> {url}")
                        crawl_res = await crawler.crawl(url)
                        
                        if not crawl_res.get("error"):
                            # 提前检查文本内容是否包含授权报错
                            c_text = crawl_res["text"][:50000]
                            if '{"error":"Not authorized."}' in c_text or "Access Denied" in c_text:
                                record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                                record_fields[FIELD_OUT_PRODUCT_RESULT] = "无需处理"
                                record_fields[FIELD_OUT_PRODUCT_THINK] = "落地页返回授权错误，无法分析图片"
                                record_fields[FIELD_OUT_PROPAGANDA_RESULT] = "无需处理"
                                record_fields[FIELD_OUT_PROPAGANDA_THINK] = "落地页返回授权错误，无法分析宣传"
                                print(f"⚠️  落地页授权错误: {c_text[:50]}")
                            else:
                                # 1. 记录文字内容
                                record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                                
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
                                print(f"🤖 正在调用大模型进行深度审核...")
                                
                                # A. 宣传内容审核
                                text_audit_chain = create_text_audit_chain()
                                text_raw = await text_audit_chain.ainvoke({"text_content": crawl_res["text"][:50000]})
                                
                                # 解析：从 ---最终结论--- 提取内容
                                propaganda_res_val = ""
                                if "---最终结论---" in text_raw:
                                    propaganda_res_val = text_raw.split("---最终结论---")[-1].strip()
                                else:
                                    propaganda_res_val = text_raw.strip()
                                
                                # 拆分：结果 + 理由
                                if "，" in propaganda_res_val:
                                    p_result = propaganda_res_val.split("，")[0].strip()
                                elif "," in propaganda_res_val:
                                    p_result = propaganda_res_val.split(",")[0].strip()
                                else:
                                    p_result = propaganda_res_val
                                    
                                record_fields[FIELD_OUT_PROPAGANDA_RESULT] = p_result 
                                record_fields[FIELD_OUT_PROPAGANDA_THINK] = propaganda_res_val

                                # B. 产品图片审核
                                img_data = await audit_images_async(crawl_res["images"])
                                
                                # 使用强力提取函数解析图片审核
                                res_json_img = extract_json_from_text(img_data["result"])
                                if res_json_img:
                                    record_fields.update(res_json_img)
                                else:
                                    record_fields[FIELD_OUT_PRODUCT_RESULT] = f"解析失败，原始输出: {img_data['result'][:100]}..."
                                    record_fields[FIELD_OUT_PRODUCT_THINK] = img_data["think"]
                                
                                # --- 用户检查：实时打印审核细节 ---
                                print(f"\n" + "="*50)
                                print(f"🕵️  【实时审核报告】: {product_names[idx]}")
                                print("-" * 20 + " [宣传内容审核] " + "-" * 20)
                                print(f"💭 思考过程: {record_fields.get(FIELD_OUT_PROPAGANDA_THINK, 'N/A')}")
                                print(f"📊 最终结论: {record_fields.get(FIELD_OUT_PROPAGANDA_RESULT, 'N/A')}")
                                print("-" * 20 + " [产品图片审核] " + "-" * 20)
                                print(f"💭 思考过程: {record_fields.get(FIELD_OUT_PRODUCT_THINK, 'N/A')}")
                                print(f"📊 最终结论: {record_fields.get(FIELD_OUT_PRODUCT_RESULT, 'N/A')}")
                                print("="*50 + "\n")
                        else:  
                            # 页面 404/403/无法打开等情况
                            record_fields[FIELD_OUT_PRODUCT_RESULT] = "无需处理"
                            record_fields[FIELD_OUT_PRODUCT_THINK] = f"网页抓取失败: {crawl_res['error']}"
                            record_fields[FIELD_OUT_PROPAGANDA_RESULT] = "无需处理"
                            record_fields[FIELD_OUT_PROPAGANDA_THINK] = "网页抓取失败，无法分析宣传内容"
                    else:
                        record_fields[FIELD_OUT_PRODUCT_RESULT] = "跳过: 无效 URL"
                else:
                    # 分类为“否”，不进行任何进一步操作，保持初始化的“未触发”状态
                    pass
                
                feishu_records.append({"fields": record_fields})
                print(f"✅ 完成处理: {product_names[idx]}")
        finally:
            await crawler.close_browser()

        # 4. 批量写回飞书
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
            # 适当延时，避免频率限制
            await asyncio.sleep(1)

        print(f"\n✨ 全部审核与飞书同步完成！")

    except Exception as e:
        print(f"❌ 流程运行失败: {e}")



if __name__ == "__main__":
    asyncio.run(main_async())

##