import asyncio
import json
import base64
import re
import requests
from datetime import datetime
from typing import List, Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from feishu_api import FeishuAPI
from excel_handler import ExcelHandler
from landing_page_crawler import LandingPageCrawler
import os

# ================= 配置区域 =================

# 飞书应用配置
FEISHU_APP_ID = 'cli_a97ad90b25799bda'
FEISHU_APP_SECRET = 'E2mpKPu906vO9K6N68keMbetyxXvbIqe'

# Excel 输入列名定义
COL_PRODUCT_NAME = "产品名称"
COL_URL = "落地页链接"
COL_DEPT = "部门"
COL_VIRTUAL_SKU = "虚拟SKU编号"
COL_REAL_SKU = "真实SKU编号"
COL_OPERATOR = "运营"
COL_ORDER_COUNT = "订单数"

# 飞书 Bitable 输出列名定义
FIELD_OUT_PRODUCT_NAME = '产品名称'
FIELD_OUT_URL = '落地页链接'
FIELD_OUT_DEPT = '部门'
FIELD_OUT_VIRTUAL_SKU = '虚拟SKU编号'
FIELD_OUT_REAL_SKU = '真实SKU编号'
FIELD_OUT_OPERATOR = '运营'
FIELD_OUT_ORDER_COUNT = '订单数'

FIELD_OUT_TEXT_CONTENT = '落地页文字'
FIELD_OUT_IMAGE_CONTENT = '落地页图片'
FIELD_OUT_JUDGE_LABEL = '审核标签'
FIELD_OUT_PRODUCT_RESULT = '产品审核结果'
FIELD_OUT_PRODUCT_THINK = '产品判断依据'
FIELD_OUT_AUDIT_TIME = '审核时间'

# 大模型 API 配置
API_KEY = "78cbeed3-eea7-4832-ba9c-b3fefeff316f"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"

# 模型配置
MODEL_CLASSIFY = "doubao-seed-1-8-251228"
MODEL_TEXT_AUDIT = "doubao-seed-1-8-251228"
MODEL_IMAGE_AUDIT = "doubao-seed-1-8-251228"

MAX_WORKERS = 5          # 同时处理几条商品记录
MAX_UPLOAD_WORKERS = 5  # 同时上传几张图片

# 初始化模型
llm_classify = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_CLASSIFY, temperature=0.0, timeout=60)
llm_text = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_TEXT_AUDIT, temperature=0.0, timeout=60)
llm_vision = ChatOpenAI(api_key=API_KEY, base_url=BASE_URL, model=MODEL_IMAGE_AUDIT, temperature=0.0)

# 注意：并发控制对象（browser_lock、upload_semaphore）
# 必须在事件循环启动后创建，放在 main_async 内部


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
crawler = LandingPageCrawler(headless=True)

# ================= LangChain 构建 =================


def create_classify_chain():
    prompt = ChatPromptTemplate.from_messages([
        ("system", "你是一个严谨的商品审核助手。输出必须是'是枪型武器类'或'非枪型武器类'。"),
        ("user", "判断以下商品是否属于枪型武器类：\n商品名称：{product_name}\n\n说明:1. 枪型武器具备枪支外形:与手枪、步枪等真枪结构相似的造型;枪管具有发射结构:必须有管状结构;发射物非毛绒材质。请严格只输出'是枪型武器类'或'非枪型武器类'。")
    ])
    return prompt | llm_classify | StrOutputParser()


def extract_json_from_text(text: str) -> dict:
    """从文本中提取并解析 JSON 对象"""
    if not text or not isinstance(text, str):
        return {}

    clean_text = text.replace("```json", "").replace("```", "").strip()

    try:
        start = clean_text.find("{")
        end = clean_text.rfind("}")
        if start != -1 and end != -1:
            json_str = clean_text[start:end + 1]
            return json.loads(json_str)
    except:
        pass

    return {}
async def audit_text_async(text_content: str, product_name: str):
    """调用文字模型审核落地页文字内容"""
    if not text_content or len(text_content.strip()) < 20:
        return {
            "think": "落地页文字内容过少，无法进行文字审核",
            "result": None  # 返回 None 表示不覆盖已有结果
        }

    # 截取前 8000 字符，避免超出 token 限制
    truncated_text = text_content[:8000]

    prompt_text = f"""你将扮演**专业商品文字审核员**，依据《审核规则》对落地页文字内容进行精准审核。

### 审核规则（必严格遵循）
#### 枪型武器类文字审核
判断落地页文字中是否存在以下特征：
1. **具备枪支外形描述**：出现手枪、步枪、手枪造型、枪型外形等描述
2. **具有发射结构描述**：出现发射、射击、水枪喷射、火焰喷射、激光发射等描述
3. **发射物非毛绒材质**：发射的物体为水、软弹、激光、火焰等非毛绒材质

以下为违规产品关键词示例：
- 水枪类：water gun、water pistol、squirt gun、水枪、喷水枪
- 枪型打火机：pistol lighter、gun lighter、torch lighter、枪型打火机
- 玩具枪：toy gun、nerf gun、laser gun、soft bullet gun
- 其他：propane torch（配合枪型描述）

### 审核商品信息
商品名称：{product_name}

### 落地页文字内容
{truncated_text}

### 输出格式
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）：

---思考过程---
[详细分析文字中涉及的关键词和特征]

---最终结论---
{{
  "{FIELD_OUT_PRODUCT_RESULT}": "必须从 [枪型武器违规, 产品通过, 需人工处理] 中精准选择一个",
  "{FIELD_OUT_PRODUCT_THINK}": "[这里填入你的分析过程]"
}}
"""

    try:
        response = await llm_text.ainvoke([("user", prompt_text)])
        full_text = response.content

        think = full_text
        result = full_text
        if "---最终结论---" in full_text:
            parts = full_text.split("---最终结论---")
            think = parts[0].replace("---思考过程---", "").strip()
            result = parts[1].strip()

        return {"think": think, "result": result}
    except Exception as e:
        return {
            "think": f"文字审核过程报错: {e}",
            "result": None
        }

# ================= 业务逻辑 =================
async def audit_images_async(image_urls: List[str]):
    """调用视觉模型审核图片"""
    if not image_urls:
        return {
            "think": "未提供产品图片，无法进行视觉分析",
            "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "无需处理", "{FIELD_OUT_PRODUCT_THINK}": "落地页未通过爬虫抓取到有效图片，跳过 AI 视觉审核" }}'
        }

    prompt_text = f"""你将扮演**专业商品图片审核员**，依据《审核规则》对商品图片进行精准审核，核心目标是识别图片中的物体特征、材质及演示场景，输出合规性判定结果及整改方向。

### 一、审核规则（必严格遵循）
#### 1. 枪型武器类类图片审核
- **具备枪支外形：与手枪、步枪等真枪结构相似的造型；
- **枪管具有发射结构：存在管状发射结构；
- **发射物非毛绒材质：从枪口发射的物体不属于毛绒材质（发射定义包括物理击发、激光发射、水/火发射等所有从枪口发射东西的情况）。
- 违规产品示例：手炮玩具枪（发射软弹）、激光枪（发射激光）、有发射功能的积木枪、玩具水枪、打火机型手枪、带真实发射功能的VR游戏枪配件。

### 二、审核流程（按步骤执行）
1. **图片信息提取：** 识别图片中的物体类型、核心特征、演示动作及背景场景。
2. **状态判定：** 若图片内容显示为"404/403/503/无法显示/错误页面"，归类为"无需处理"。
3. **分权判定：** 根据识别结果，将结论严格匹配至下述五个选项之一。
4. **合格判定：** 若不涉及任何上述违规内容且内容非报错信息，判定为"产品通过"。

### 三、约束规则
- 输出请严格遵循以下格式（JSON 中的键名必须与飞书列名完全一致）：

---思考过程---
[详细分析步骤]

---最终结论---
{{
  "{FIELD_OUT_PRODUCT_RESULT}": "必须从 [无需处理, 枪型武器违规, 产品通过, 需人工处理] 中精准选择一个",
  "{FIELD_OUT_PRODUCT_THINK}": "[这里填入你的分析过程]"
}}
"""
    content = [{"type": "text", "text": prompt_text}]
    for url in image_urls:
        content.append({
            "type": "image_url",
            "image_url": {"url": url}
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
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "落地页包含超大体积图片(>10MB)，超出 AI 处理极限，需进入落地页手动审核" }}'
            }
        if "Timeout while downloading" in err_str:
            return {
                "think": f"AI 服务端下载图片超时: {err_str}",
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "AI 服务商调取图片URL超时(CDN响应慢或屏蔽)，无法自动获取图片，需手动审核" }}'
            }
        if "400" in err_str:
            return {
                "think": f"AI 接口参数错误或拒绝处理: {err_str}",
                "result": f'{{ "{FIELD_OUT_PRODUCT_RESULT}": "需人工处理", "{FIELD_OUT_PRODUCT_THINK}": "AI 接口返回400错误(可能涉及无法识别的格式)，建议手动核查" }}'
            }
        return {"think": f"图片审核过程报错: {e}", "result": "审核失败"}


async def main_async():
    try:
        # ---- 自动清理临时图片目录 ----
        temp_dir = os.path.join(os.getcwd(), "temp_images")
        if os.path.exists(temp_dir):
            for f in os.listdir(temp_dir):
                try:
                    os.remove(os.path.join(temp_dir, f))
                except:
                    pass
            print(f"🧹 已清理临时目录: {temp_dir}")
        # ---- 清理结束 ----
        # ---- 在事件循环内创建并发控制对象 ----
        browser_lock = asyncio.Lock()
        upload_semaphore = asyncio.Semaphore(MAX_UPLOAD_WORKERS)

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

        # 过滤空行
        valid_indices = []
        for i, name in enumerate(product_names):
            if name and str(name).strip() and str(name).strip().lower() not in ["", "nan", "none"]:
                valid_indices.append(i)

        if not valid_indices:
            print("❌ Excel 中没有有效的产品数据")
            return

        product_names = [product_names[i] for i in valid_indices]
        landing_page_urls = [landing_page_urls[i] for i in valid_indices]
        dept_data = [dept_data[i] for i in valid_indices]
        virtual_sku_data = [virtual_sku_data[i] for i in valid_indices]
        real_sku_data = [real_sku_data[i] for i in valid_indices]
        operator_data = [operator_data[i] for i in valid_indices]
        order_count_data = [order_count_data[i] for i in valid_indices]
        print(f"📊 过滤空行后，有效数据: {len(product_names)} 条")

        # 2. 意图分类
        print(f"🚀 正在快速识别分类 (样本数: {len(product_names)})...")
        classify_chain = create_classify_chain()
        cleaned_classifications = []
        BATCH_CLASSIFY = 100
        for i in range(0, len(product_names), BATCH_CLASSIFY):
            chunk = product_names[i: i + BATCH_CLASSIFY]
            print(f"   进度: {i}/{len(product_names)} ...")
            try:
                batch_res = await classify_chain.abatch(
                    [{"product_name": n} for n in chunk],
                    config={"max_concurrency": 30}
                )

                def normalize_label(label):
                    label = label.strip().replace('。', '')
                    if label == "是" or "是枪型武器" in label:
                        return "是枪型武器类"
                    if label == "否" or "非枪型武器" in label:
                        return "非枪型武器类"
                    return label

                cleaned_classifications.extend([normalize_label(c) for c in batch_res])
            except Exception as e:
                print(f"   ⚠️  批量(100)超时，正在以稳健模式(10并发)重试该小节...")
                try:
                    retry_res = await classify_chain.abatch(
                        [{"product_name": n} for n in chunk],
                        config={"max_concurrency": 10}
                    )
                    cleaned_classifications.extend([normalize_label(c) for c in retry_res])
                except:
                    print(f"   ❌ 稳健模式依然失败，该 100 条记录默认记为'非枪型武器类'")
                    cleaned_classifications.extend(["非枪型武器类"] * len(chunk))
        print(f"✅ 分类完成！共计 {len(cleaned_classifications)} 条记录。")

        # 3. 并发爬取与审核
        semaphore = asyncio.Semaphore(MAX_WORKERS)

        async def _upload_one(img_url):
            """限流单张图片上传"""
            async with upload_semaphore:
                return await asyncio.to_thread(
                    feishu.download_and_upload_image, img_url, app_token
                )

        async def process_item(idx, res):
            """并行处理单条商品"""
            async with semaphore: 
                record_fields = {
                    FIELD_OUT_PRODUCT_NAME: product_names[idx],
                    FIELD_OUT_URL: landing_page_urls[idx],
                    FIELD_OUT_DEPT: dept_data[idx],
                    FIELD_OUT_VIRTUAL_SKU: virtual_sku_data[idx],
                    FIELD_OUT_REAL_SKU: real_sku_data[idx],
                    FIELD_OUT_OPERATOR: operator_data[idx],
                    FIELD_OUT_ORDER_COUNT: int(str(order_count_data[idx]).strip()) if str(order_count_data[idx]).strip().isdigit() else 0,
                    FIELD_OUT_JUDGE_LABEL: res,
                    FIELD_OUT_PRODUCT_RESULT: "产品通过" if res == "非枪型武器类" else "",
                    FIELD_OUT_PRODUCT_THINK: "未命中" if res == "非枪型武器类" else "",
                    FIELD_OUT_TEXT_CONTENT: "",
                    FIELD_OUT_IMAGE_CONTENT: [],
                    FIELD_OUT_AUDIT_TIME: int(datetime.now().timestamp() * 1000),
                }
    
                if res == "是枪型武器类":
                    raw_url = str(landing_page_urls[idx]).strip()
                    clean_url = raw_url.replace("\n", "").replace("\r", "").strip()
                    match = re.search(r'(https?://[^\s\u4e00-\u9fa5]+)', clean_url)
                    url = ""
                    if match:
                        url = match.group(1)
                    else:
                        if clean_url and clean_url.lower() not in ["未知", "nan", "none", ""]:
                            url = clean_url if clean_url.startswith("http") else "https://" + clean_url
                            url = re.split(r'[\s\u4e00-\u9fa5]', url)[0]
    
                    if url and url.lower() not in ["", "未知", "nan", "none"]:
                        print(f"🔍 发现疑似违规，正在抓取数据 [{idx + 1}/{len(product_names)}]: {product_names[idx]} -> {url}")
    
                        # 浏览器锁：同一时间只有一个任务在爬取页面
                        async with browser_lock:
                            crawl_res = await crawler.crawl(url)
    
                        if not crawl_res.get("error"):
                            c_text = crawl_res["text"][:50000]
                            if '{"error":"Not authorized."}' in c_text or "Access Denied" in c_text:
                                record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
                                record_fields[FIELD_OUT_PRODUCT_RESULT] = "无需处理"
                                record_fields[FIELD_OUT_PRODUCT_THINK] = "落地页返回授权错误，无法分析图片"
                                print(f"⚠️  落地页授权错误: {c_text[:50]}")
                            else:
                                record_fields[FIELD_OUT_TEXT_CONTENT] = c_text
    
                                # 并行上传图片（upload_semaphore 控制并发数）
                                image_urls = crawl_res["images"]
                                if image_urls:
                                    print(f"🖼️  正在并行同步图片附件至飞书 (共 {len(image_urls)} 张)...")
                                    upload_tasks = [_upload_one(img_url) for img_url in image_urls]
                                    upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
                                    file_tokens = []
                                    for r in upload_results:
                                        if r and not isinstance(r, Exception):
                                            file_tokens.append({"file_token": r})
                                    record_fields[FIELD_OUT_IMAGE_CONTENT] = file_tokens
    
                                # AI 图片审核
                                print(f"🤖 正在调用大模型进行图片审核...")
                                img_data = await audit_images_async(crawl_res["images"])
    
                                res_json_img = extract_json_from_text(img_data["result"])
                                if res_json_img:
                                    record_fields.update(res_json_img)
                                else:
                                    record_fields[FIELD_OUT_PRODUCT_RESULT] = f"解析失败，原始输出: {img_data['result'][:100]}..."
                                    record_fields[FIELD_OUT_PRODUCT_THINK] = img_data["think"]
                                # ---- 新增：AI 文字审核 ----
                                print(f"📝 正在调用大模型进行文字审核...")
                                text_data = await audit_text_async(c_text, product_names[idx])

                                if text_data["result"]:
                                    res_json_text = extract_json_from_text(text_data["result"])
                                    if res_json_text:
                                        # 如果文字审核判定为违规，且图片审核没判定为违规，则以文字审核为准
                                        img_result = record_fields.get(FIELD_OUT_PRODUCT_RESULT, "")
                                        text_result = res_json_text.get(FIELD_OUT_PRODUCT_RESULT, "")

                                        if text_result == "枪型武器违规" and img_result != "枪型武器违规":
                                            # 文字审核发现违规，覆盖图片审核结果
                                            record_fields.update(res_json_text)
                                            record_fields[FIELD_OUT_PRODUCT_THINK] = (
                                                f"[图片审核] {img_data['think']}\n\n"
                                                f"[文字审核] {res_json_text.get(FIELD_OUT_PRODUCT_THINK, '')}"
                                            )
                                            print(f"📝 文字审核覆盖了图片审核结果 → {text_result}")
                                        elif img_result == "枪型武器违规":
                                            # 图片已经判定违规，保留图片审核结果，追加文字审核依据
                                            record_fields[FIELD_OUT_PRODUCT_THINK] = (
                                                f"[图片审核] {record_fields.get(FIELD_OUT_PRODUCT_THINK, '')}\n\n"
                                                f"[文字审核] {res_json_text.get(FIELD_OUT_PRODUCT_THINK, '')}"
                                            )
                                            print(f"📝 文字审核确认了图片审核结果")
                                        else:
                                            # 两者都未发现违规，保留图片审核结果
                                            pass
                                        # ---- 文字审核结束 ----
                                print(f"\n" + "=" * 50)
                                print(f"🕵️  【实时审核报告】: {product_names[idx]}")
                                print(f"💭 思考过程: {record_fields.get(FIELD_OUT_PRODUCT_THINK, 'N/A')}")
                                print(f"📊 最终结论: {record_fields.get(FIELD_OUT_PRODUCT_RESULT, 'N/A')}")
                                print(f"⏰ 审核时间: {record_fields.get(FIELD_OUT_AUDIT_TIME, 'N/A')}")
                                print("=" * 50 + "\n")
                        else:
                            record_fields[FIELD_OUT_PRODUCT_RESULT] = "无需处理"
                            record_fields[FIELD_OUT_PRODUCT_THINK] = f"网页抓取失败: {crawl_res['error']}"
                    else:
                        record_fields[FIELD_OUT_PRODUCT_RESULT] = "跳过: 无效 URL"
    
                print(f"✅ 完成处理: {product_names[idx]}")
                return {"fields": record_fields}

        await crawler.init_browser()
        try:
            tasks = [process_item(idx, res) for idx, res in enumerate(cleaned_classifications)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            feishu_records = []
            for r in results:
                if isinstance(r, Exception):
                    print(f"❌ 处理异常: {r}")
                else:
                    feishu_records.append(r)
        finally:
            await crawler.close_browser()

        # # 4. 批量写回飞书
        # token = feishu.get_stored_token() or feishu.get_tenant_access_token()
        # try:
        #     headers = {"Authorization": f"Bearer {token}"}
        #     url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        #     resp = requests.get(url, headers=headers).json()
        #     if resp.get("code") == 0:
        #         fields = [f["field_name"] for f in resp["data"]["items"]]
        #         print("📋 飞书表格实际列名：", fields)
        #     else:
        #         print("⚠️ 获取表格列名失败：", resp)
        # except Exception as e:
        #     print("⚠️ 获取表格列名异常：", e)

        print(f"\n📤 正在将 {len(feishu_records)} 条记录同步至飞书多维表格...")
        BATCH_SIZE = 50
        for i in range(0, len(feishu_records), BATCH_SIZE):
            chunk = feishu_records[i: i + BATCH_SIZE]
            print(f"   正在同步第 {i // BATCH_SIZE + 1} 批...")
            # print("即将写入的第一条记录：", json.dumps(chunk[0], indent=2, ensure_ascii=False))
            success = await asyncio.to_thread(feishu.add_batch_records_to_bitable, app_token, table_id, chunk)
            if success:
                print(f"   ✅ 第 {i // BATCH_SIZE + 1} 批同步成功")
            else:
                print(f"   ❌ 第 {i // BATCH_SIZE + 1} 批同步失败")
            await asyncio.sleep(1)

        print(f"\n✨ 全部审核与飞书同步完成！")

    except Exception as e:
        print(f"❌ 流程运行失败: {e}")
    finally:
        # ---- 程序结束后再次清理 ----
        temp_dir = os.path.join(os.getcwd(), "temp_images")
        if os.path.exists(temp_dir):
            cleaned = 0
            for f in os.listdir(temp_dir):
                try:
                    os.remove(os.path.join(temp_dir, f))
                    cleaned += 1
                except:
                    pass
            if cleaned:
                print(f"🧹 已清理残留临时文件: {cleaned} 个")
        # ---- 清理结束 ----

if __name__ == "__main__":
    asyncio.run(main_async())
