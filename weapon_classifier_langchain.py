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
MODEL_CLASSIFY = "doubao-seed-1-8-251228"
MODEL_TEXT_AUDIT = "doubao-seed-1-8-251228"
MODEL_IMAGE_AUDIT = "doubao-seed-1-8-251228"

MAX_WORKERS = 5

# 初始化模型 (增加 timeout 防止挂起)
llm_classify = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_CLASSIFY, temperature=0.0, timeout=60)
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
- **暗示违规：** 严禁提及**“军用”、“特种兵”、“血槽”、“刺穿”**等暗示高杀伤力的描述，视为【武器宣传违规】。

#### 2. 武器/刑具类违规
- **黑名单关键词（一票否决）：** 标题或描述中出现**“Weapon（武器）”、“Stun/Taser/Electric shock（电击枪/器/棒）”、“Pepper spray（防狼喷雾）”、“Mace（催泪剂/狼牙棒）”、“Knuckles（指虎）”、“Slingshot（弹弓）”、“Baton（警棍）”、“Handcuffs（手铐）”、“Crossbow/Compound bow（弩/复合弓）”、“Metal dart（金属飞镖）”**等词汇，判定为【武器宣传违规】。

### 二、审核流程（按步骤执行）
1. **内容提取与状态判定：** 
   - 如果文本为空、显示“404/无法打开”、或者内容包含“{{{{error":"Not authorized."}}}}”、“Access Denied”、“Forbidden”等授权报错，判定结果为“无需处理”。
2. **规则匹配：** 扫描文本，命中上述违规词汇或用途，判定为“武器宣传违规”。
3. **合格判定：** 若不涉及任何上述违规内容且内容非报错信息，判定为“宣传通过”。

### 三、约束规则
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）： 


---思考过程---
[详细分析步骤]

---最终结论---
{{{{
  "{FIELD_OUT_PROPAGANDA_RESULT}": "必须从 [无需处理, 武器宣传违规, 宣传通过, 需人工处理] 中精准选择一个",
  "{FIELD_OUT_PROPAGANDA_THINK}": "[这里填入你的思考过程分析]"
}}}}
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
    prompt_text = f"""你将扮演**专业商品图片审核员**，依据《审核规则》对商品图片进行精准审核，核心目标是识别图片中的物体特征、材质及演示场景，输出合规性判定结果及整改方向。

### 一、审核规则（必严格遵循）
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

### 二、审核流程（按步骤执行）
1. **图片信息提取：** 识别图片中的物体类型、核心特征、演示动作及背景场景。
2. **状态判定：** 若图片内容显示为“404/无法显示/错误页面”，归类为“无需处理”。
3. **分权判定：** 根据识别结果，将结论严格匹配至下述五个选项之一。

### 三、约束规则
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）：

---思考过程---
[详细分析步骤]

---最终结论---
{{
  "{FIELD_OUT_PRODUCT_RESULT}": "必须从 [无需处理, 刀具违规, 武器违规, 刑具违规, 产品通过, 需人工处理] 中精准选择一个",
  "{FIELD_OUT_PRODUCT_THINK}": "[这里填入你的分析过程]"
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
        
        # 拆分思考过程和结论
        think = full_text
        result = full_text
        if "---最终结论---" in full_text:
            parts = full_text.split("---最终结论---")
            think = parts[0].replace("---思考过程---", "").strip()
            result = parts[1].strip()
        
        return {"think": think, "result": result}
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
                                
                                # 使用强力提取函数解析文本审核
                                res_json_text = extract_json_from_text(text_raw)
                                if res_json_text:
                                    record_fields.update(res_json_text)
                                else:
                                    record_fields[FIELD_OUT_PROPAGANDA_RESULT] = f"解析失败，原始输出: {text_raw[:100]}..."
                                    record_fields[FIELD_OUT_PROPAGANDA_THINK] = text_raw

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