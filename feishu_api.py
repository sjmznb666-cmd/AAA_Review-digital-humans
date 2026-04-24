import os
import time
import base64
import requests
from pathlib import Path
from urllib.parse import urlparse

class FeishuAPI:
    """
    FeishuAPI 类 - 用于处理飞书 API 相关操作
    """
    def __init__(self, app_id, app_secret):
        """
        构造函数
        :param app_id: 飞书应用 ID
        :param app_secret: 飞书应用密钥
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = None

    def get_tenant_access_token(self):
        """
        获取租户访问令牌
        :return: 返回访问令牌或 None
        """
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
        """
        获取当前存储的访问令牌
        """
        return self.tenant_access_token

    def download_image(self, image_url):
        """
        下载图片到本地
        :param image_url: 图片 URL
        :return: 返回本地文件路径或 None
        """
        try:
            print(f"📥 Downloading image: {image_url}")

            images_dir = Path.cwd() / "temp_images"
            images_dir.mkdir(parents=True, exist_ok=True)

            url_hash = base64.b64encode(image_url.encode()).decode()
            url_hash = url_hash.replace("/", "_").replace("+", "-")[:20]
            timestamp = int(time.time() * 1000)
            
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
        """
        上传图片到飞书
        :param file_path: 本地文件路径
        :param parent_node: 父节点 ID（如 bitable ID）
        :param max_retries: 最大重试次数
        """
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
                    files = {
                        "file": (file_name, f)
                    }
                    data = {
                        "file_name": file_name,
                        "parent_type": "bitable_image",
                        "parent_node": parent_node,
                        "size": str(file_size)
                    }
                    headers = {
                        "Authorization": f"Bearer {self.tenant_access_token}"
                    }

                    print(f"🔄 Uploading file to Feishu (Attempt {retry_count + 1}/{max_retries + 1}): {file_name}")
                    
                    response = requests.post(url, headers=headers, data=data, files=files, timeout=60)
                    res_data = response.json()

                    print(f"Feishu Upload Response Code: {res_data.get('code')}")

                    if res_data.get("code") == 0:
                        file_token = res_data.get("data", {}).get("file_token")
                        print(f"✅ File uploaded to Feishu: {file_token}")
                        return file_token
                    else:
                        msg = str(res_data.get('msg', '')).lower()
                        print(f"❌ Feishu upload failed: {res_data.get('msg')}")
                        if res_data.get("code") != 0:
                            print(f"Feishu Upload Error Details: {res_data}")
                            
                        # 如果是 Token 过期导致的失败，立即重新获取 Token 并更新 header
                        if "token" in msg or res_data.get("code") in [99991663, 99991668]:
                            print("🔄 检测到 Token 可能已过期，尝试重新获取...")
                            if self.get_tenant_access_token():
                                headers["Authorization"] = f"Bearer {self.tenant_access_token}"
                        
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
        """
        下载并上传图片到飞书（组合方法）
        """
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

    def add_batch_records_to_bitable(self, app_token, table_id, records_array):
        """
        批量添加记录到多维表格 (具备列名自适应能力)
        """
        if not self.tenant_access_token:
            if not self.get_tenant_access_token():
                return False

        # --- A. 自动探测目标表存在的列名 (预防 FieldNameNotFound) ---
        available_fields = set()
        fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {"Authorization": f"Bearer {self.tenant_access_token}"}
        try:
            fields_res = requests.get(fields_url, headers=headers).json()
            if fields_res.get("code") == 0:
                available_fields = {f["field_name"] for f in fields_res.get("data", {}).get("items", [])}
                # print(f"📊 探测到飞书表格实际包含的列: {list(available_fields)}")
        except Exception as e:
            print(f"⚠️  无法预检列名，将尝试直接同步: {e}")

        # --- B. 净化数据：剔除飞书中不存在的列 ---
        sanitized_records = []
        for record in records_array:
            fields = record.get("fields", {})
            if available_fields:
                # 只保留飞书里有的列
                new_fields = {k: v for k, v in fields.items() if k in available_fields}
                
                # 记录被过滤的列名（仅提示一次）
                missing = set(fields.keys()) - available_fields
                if missing and not hasattr(self, '_missing_reported'):
                    print(f"⚠️  警告: 飞书中缺失以下列，已自动跳过同步: {missing}")
                    print(f"💡 提示: 如果你需要这些数据，请在飞书多维表格中手动创建同名列。")
                    self._missing_reported = True
                sanitized_records.append({"fields": new_fields})
            else:
                sanitized_records.append(record)

        # --- C. 执行同步 ---
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
                
                # 处理限频
                if code == 1254290:
                    wait_time = (attempt + 1) * 2
                    print(f"⚠️  飞书限频，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    attempt += 1
                    continue
                
                print(f"❌ 飞书写入失败 (Code {code}): {res_data.get('msg')}")
                msg = str(res_data.get("msg", "")).lower()
                
                # 处理 Token 过期
                if "token" in msg or code in [99991663, 99991668]:
                    print("🔄 检测到 Token 可能已过期，尝试重新获取...")
                    if self.get_tenant_access_token():
                        headers["Authorization"] = f"Bearer {self.tenant_access_token}"
                    attempt += 1
                    continue
                    
                attempt += 1
                time.sleep(attempt * 2)
            except Exception as e:
                print(f"❌ 网络/API 异常: {e}")
                attempt += 1
                time.sleep(attempt * 2)
        return False
