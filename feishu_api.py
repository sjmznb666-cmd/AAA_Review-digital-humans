import os
import time
import json                          # ← 加这一行
import base64
import requests
from pathlib import Path
from datetime import datetime        # ← 加这一行
from urllib.parse import urlparse
import random

class FeishuAPI:
    """
    FeishuAPI 类 - 用于处理飞书 API 相关操作
    """
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = None

    def get_tenant_access_token(self):
        if not self.app_id or not self.app_secret:
            print("\n❌ Error: Please configure FEISHU_APP_ID and FEISHU_APP_SECRET.")
            return None

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        try:
            response = requests.post(url, json=payload)
            res_data = response.json()
            if res_data.get("code") == 0:
                self.tenant_access_token = res_data.get("tenant_access_token")
                print("✅ Successfully obtained tenant access token")
                return self.tenant_access_token
            else:
                print(f"\n❌ Failed to obtain tenant access token: {res_data}")
                return None
        except Exception as e:
            print(f"\n❌ An unexpected error occurred during Feishu authentication: {str(e)}")
            return None

    def get_stored_token(self):
        return self.tenant_access_token

    def download_image(self, image_url):
        try:
            print(f"📥 Downloading image: {image_url}")

            images_dir = Path.cwd() / "temp_images"
            images_dir.mkdir(parents=True, exist_ok=True)

            url_hash = base64.b64encode(image_url.encode()).decode()
            url_hash = url_hash.replace("/", "_").replace("+", "-")[:20]
            timestamp = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

            parsed_url = urlparse(image_url)
            file_ext = os.path.splitext(parsed_url.path)[1] or ".jpg"

            filename = f"image_{url_hash}_{timestamp}{file_ext}"
            file_path = images_dir / filename

            response = requests.get(image_url, stream=True, timeout=30)
            response.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"✅ Image downloaded to: {file_path}")
            return str(file_path)
        except Exception as e:
            print(f"❌ Error downloading image: {str(e)}")
            return None

    def upload_image_to_feishu(self, file_path, parent_node, max_retries=3):
        if not self.tenant_access_token:
            if not self.get_tenant_access_token():
                return None

        url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"

        retry_count = 0
        while retry_count <= max_retries:
            try:
                file_size = os.path.getsize(file_path)
                file_name = os.path.basename(file_path)

                with open(file_path, "rb") as f:
                    files = {"file": (file_name, f)}
                    data = {
                        "file_name": file_name,
                        "parent_type": "bitable_image",
                        "parent_node": parent_node,
                        "size": str(file_size)
                    }
                    headers = {"Authorization": f"Bearer {self.tenant_access_token}"}

                    print(f"🔄 Uploading file to Feishu (Attempt {retry_count + 1}/{max_retries + 1}): {file_name}")

                    response = requests.post(url, headers=headers, data=data, files=files, timeout=60)
                    res_data = response.json()

                    print(f"Feishu Upload Response Code: {res_data.get('code')}")

                    if res_data.get("code") == 0:
                        file_token = res_data.get("data", {}).get("file_token")
                        print(f"✅ File uploaded to Feishu: {file_token}")
                        return file_token
                    else:
                        print(f"❌ Feishu upload failed: {res_data.get('msg')}")
                        print(f"Feishu Upload Error Details: {res_data}")

                        retry_count += 1
                        if retry_count <= max_retries:
                            print("⏱️  Retrying in 2 seconds...")
                            time.sleep(2)
                        else:
                            print(f"❌ All upload attempts failed for file: {file_name}")
                            return None
            except Exception as e:
                print(f"❌ Error uploading file to Feishu (Attempt {retry_count + 1}): {str(e)}")
                retry_count += 1
                if retry_count <= max_retries:
                    print("⏱️  Retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    return None
        return None

    def download_and_upload_image(self, image_url, parent_node, delete_after_upload=True):
        try:
            file_path = self.download_image(image_url)
            if not file_path:
                return None

            file_token = self.upload_image_to_feishu(file_path, parent_node)

            if delete_after_upload and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑️  Deleted local file: {file_path}")
                except Exception as e:
                    print(f"⚠️  Failed to delete local file: {str(e)}")

            return file_token
        except Exception as e:
            print(f"❌ Error in download_and_upload_image: {str(e)}")
            return None

    # =====================================================================
    #  核心改动：add_batch_records_to_bitable 增加字段类型自动转换
    # =====================================================================
    def add_batch_records_to_bitable(self, app_token, table_id, records_array):
        """
        批量添加记录到多维表格
        - 自动探测列名 + 字段类型
        - 根据字段类型自动转换数据格式，防止 TextFieldConvFail
        """
        if not self.tenant_access_token:
            if not self.get_tenant_access_token():
                return False

        # --- A. 获取字段信息：名称 + 类型 ---
        #     飞书字段类型编号:
        #       1=文本  2=数字  3=单选  4=多选  5=日期
        #       7=复选框  11=人员  13=电话  15=超链接
        #       17=附件  20=公式  22=地理位置
        field_type_map = {}  # {字段名: 字段类型编号}
        fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {"Authorization": f"Bearer {self.tenant_access_token}"}
        try:
            fields_res = requests.get(fields_url, headers=headers).json()
            if fields_res.get("code") == 0:
                for f in fields_res.get("data", {}).get("items", []):
                    field_type_map[f["field_name"]] = f.get("type", 1)
                print(f"📋 飞书表格实际列名：{list(field_type_map.keys())}")
            else:
                raise RuntimeError(f"获取表格字段失败: {fields_res}")
        except Exception as e:
            print(f"❌ 无法获取表格字段，终止写入: {e}")
            return False

        # --- B. 按字段类型净化数据 ---
        TEXT_TYPES = {1, 3, 13, 15}         # 需要字符串值
        NUMBER_TYPES = {2}                   # 需要数字值
        DATE_TYPES = {5}                     # 需要毫秒时间戳
        ATTACHMENT_TYPES = {17}              # 需要 [{file_token: "xxx"}]

        sanitized_records = []
        for record in records_array:
            fields = record.get("fields", {})
            new_fields = {}

            for k, v in fields.items():
                # 跳过飞书中不存在的列
                if k not in field_type_map:
                    continue

                ft = field_type_map[k]

                if ft in ATTACHMENT_TYPES:
                    # 附件字段
                    if isinstance(v, list):
                        new_fields[k] = v
                    else:
                        new_fields[k] = []

                elif ft in TEXT_TYPES:
                    # 文本字段：强制转字符串
                    if isinstance(v, (dict, list)):
                        new_fields[k] = json.dumps(v, ensure_ascii=False) if v else ""
                    elif v is None:
                        new_fields[k] = ""
                    else:
                        new_fields[k] = str(v)

                elif ft in NUMBER_TYPES:
                    # 数字字段
                    if isinstance(v, (int, float)):
                        new_fields[k] = v
                    else:
                        try:
                            new_fields[k] = float(str(v).strip())
                        except (ValueError, TypeError):
                            new_fields[k] = 0

                elif ft in DATE_TYPES:
                    # 日期字段
                    if isinstance(v, (int, float)) and v > 1000000000000:
                        new_fields[k] = int(v)
                    else:
                        new_fields[k] = int(datetime.now().timestamp() * 1000)

                elif ft == 20:
                    # 公式字段：只读，跳过写入
                    continue

                else:
                    # 其他类型：原样写入
                    new_fields[k] = v

            sanitized_records.append({"fields": new_fields})

        # 打印被跳过的列
        sample_fields = set(records_array[0].get("fields", {}).keys()) if records_array else set()
        missing = sample_fields - set(field_type_map.keys())
        if missing:
            print(f"⚠️  飞书中缺失以下列，已跳过: {missing}")

        # --- C. 执行写入 ---
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers["Content-Type"] = "application/json; charset=utf-8"
        params = {"user_id_type": "open_id"}
        payload = {"records": sanitized_records}

        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            try:
                response = requests.post(url, headers=headers, params=params, json=payload, timeout=60)
                res_data = response.json()
                code = res_data.get("code")
                if code == 0:
                    print(f"✅ 成功向飞书写入 {len(sanitized_records)} 条记录")
                    return True

                if code == 1254290:
                    wait_time = (attempt + 1) * 2
                    print(f"⚠️  飞书限频，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    attempt += 1
                    continue

                print(f"❌ 飞书写入失败 (Code {code}): {res_data.get('msg')}")
                # 打印第一条记录帮助排查
                print(f"   调试数据: {json.dumps(sanitized_records[0], indent=2, ensure_ascii=False)[:800]}")
                return False
            except Exception as e:
                print(f"❌ 网络/API 异常: {e}")
                attempt += 1
                time.sleep(attempt * 2)
        return False
