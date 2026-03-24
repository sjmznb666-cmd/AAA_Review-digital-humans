const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

/**
 * FEISHU_API 类 - 用于处理飞书 API 相关操作
 */
class FEISHU_API {
    /**
     * 构造函数
     * @param {string} appId - 飞书应用 ID
     * @param {string} appSecret - 飞书应用密钥
     */
    constructor(appId, appSecret) {
        this.appId = appId;
        this.appSecret = appSecret;
        this.tenantAccessToken = null;
    }

    /**
     * 获取租户访问令牌
     * @returns {Promise<string|null>} 返回访问令牌或 null
     */
    async getTenantAccessToken() {
        if (!this.appId || !this.appSecret) {
            console.error('\n❌ Error: Please configure FEISHU_APP_ID and FEISHU_APP_SECRET.');
            return null;
        }

        const url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal';
        const requestBody = {
            app_id: this.appId,
            app_secret: this.appSecret
        };

        try {
            const response = await axios.post(url, requestBody);
            if (response.data.code === 0) {
                this.tenantAccessToken = response.data.tenant_access_token;
                console.log('✅ Successfully obtained tenant access token');
                return this.tenantAccessToken;
            } else {
                console.error('\n❌ Failed to obtain tenant access token:', response.data);
                return null;
            }
        } catch (error) {
            console.error('\n❌ An unexpected error occurred during Feishu authentication:', error.message);
            return null;
        }
    }

    /**
     * 获取当前存储的访问令牌
     * @returns {string|null}
     */
    getStoredToken() {
        return this.tenantAccessToken;
    }

    /**
     * 下载图片到本地
     * @param {string} imageUrl - 图片 URL
     * @returns {Promise<string|null>} 返回本地文件路径或 null
     */
    async downloadImage(imageUrl) {
        try {
            console.log(`📥 Downloading image: ${imageUrl}`);

            const imagesDir = path.join(process.cwd(), 'temp_images');
            if (!fs.existsSync(imagesDir)) {
                fs.mkdirSync(imagesDir, { recursive: true });
            }

            const urlHash = Buffer.from(imageUrl).toString('base64').replace(/\//g, '_').replace(/\+/g, '-').substring(0, 20);
            const timestamp = Date.now();
            const fileExt = path.extname(new URL(imageUrl).pathname) || '.jpg';
            const filename = `image_${urlHash}_${timestamp}${fileExt}`;
            const filePath = path.join(imagesDir, filename);

            const response = await axios.get(imageUrl, { responseType: 'stream' });
            const writer = fs.createWriteStream(filePath);

            await new Promise((resolve, reject) => {
                response.data.pipe(writer);
                writer.on('finish', resolve);
                writer.on('error', reject);
            });

            console.log(`✅ Image downloaded to: ${filePath}`);
            return filePath;
        } catch (error) {
            console.error(`❌ Error downloading image: ${error.message}`);
            return null;
        }
    }

    /**
     * 上传图片到飞书
     * @param {string} filePath - 本地文件路径
     * @param {string} parentNode - 父节点 ID（如 bitable ID）
     * @param {number} maxRetries - 最大重试次数，默认 3 次
     * @returns {Promise<string|null>} 返回文件 token 或 null
     */
    async uploadImageToFeishu(filePath, parentNode, maxRetries = 3) {
        // 如果没有 token，先获取
        let accessToken = this.tenantAccessToken;
        if (!accessToken) {
            accessToken = await this.getTenantAccessToken();
            if (!accessToken) {
                console.error('❌ Cannot upload image: Failed to obtain access token');
                return null;
            }
        }

        const url = 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all';
        const timeout = 10000;

        let retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                const fileStream = fs.createReadStream(filePath);
                const stats = fs.statSync(filePath);
                const fileSize = stats.size;
                const filename = path.basename(filePath);

                const form = new FormData();
                form.append('file_name', filename);
                form.append('parent_type', 'bitable_image');
                form.append('parent_node', parentNode);
                form.append('size', fileSize);
                form.append('file', fileStream);

                console.log(`🔄 Uploading file to Feishu (Attempt ${retryCount + 1}/${maxRetries + 1}): ${filename}`);

                const response = await axios.post(url, form, {
                    headers: {
                        'Authorization': `Bearer ${accessToken}`,
                        ...form.getHeaders()
                    },
                    timeout: timeout
                });

                console.log('Feishu Upload Response Code:', response.data.code);

                if (response.data.code !== 0) {
                    console.error('Feishu Upload Error Details:', JSON.stringify(response.data));
                }

                if (response.data.code === 0) {
                    console.log(`✅ File uploaded to Feishu: ${response.data.data.file_token}`);
                    return response.data.data.file_token;
                } else {
                    console.error(`❌ Feishu upload failed: ${response.data.msg}`);
                    retryCount++;
                    if (retryCount > maxRetries) {
                        console.error(`❌ All upload attempts failed for file: ${filename}`);
                        return null;
                    }
                    console.log(`⏱️  Retrying in 2 seconds...`);
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            } catch (error) {
                console.error(`❌ Error uploading file to Feishu (Attempt ${retryCount + 1}): ${error.message}`);
                retryCount++;
                if (retryCount > maxRetries) {
                    console.error(`❌ All upload attempts failed for file: ${path.basename(filePath)}`);
                    return null;
                }
                console.log(`⏱️  Retrying in 2 seconds...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }
        return null;
    }
    /**
     * 下载并上传图片到飞书（组合方法）
     * @param {string} imageUrl - 图片 URL
     * @param {string} parentNode - 父节点 ID（如 bitable ID）
     * @param {boolean} deleteAfterUpload - 上传后是否删除本地文件，默认 true
     * @returns {Promise<string|null>} 返回文件 token 或 null
     */
    async downloadAndUploadImage(imageUrl, parentNode, deleteAfterUpload = true) {
        try {
            // 下载图片
            const filePath = await this.downloadImage(imageUrl);
            if (!filePath) {
                return null;
            }

            // 上传到飞书
            const fileToken = await this.uploadImageToFeishu(filePath, parentNode);

            // 如果需要，删除本地文件
            if (deleteAfterUpload && fs.existsSync(filePath)) {
                try {
                    fs.unlinkSync(filePath);
                    console.log(`🗑️  Deleted local file: ${filePath}`);
                } catch (error) {
                    console.warn(`⚠️  Failed to delete local file: ${error.message}`);
                }
            }

            return fileToken;
        } catch (error) {
            console.error(`❌ Error in downloadAndUploadImage: ${error.message}`);
            return null;
        }
    }

    @param { string } filePath
@param { string } parentNode
@param { string } accessToken
@returns { Promise < string | null >}

async uploadFileFeishu(filePa, accessToken, parentNode, maxRetries = 3) {
    const url = 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all';
    const timeout = 10000;

    let retryCount = 0;
    while (retryCount <= maxRetries) {
        try {
            const fileStream = fs.createReadStream(filePath);
            const stats = fs.statSync(filePath);
            const fileSizeInBytes = stats.size;
            const fileName = path.basename(filePath);

            const form = new FormData();
            form.append('file_name', fileName);
            form.append('parent_type', 'bitable_image');
            form.append('parent_node', parentNode);
            form.append('size', fileSizeBytes);
            form.append('file', fileStream);

            console.log('🔄 Uploading file to Feishu (Attempt ${retryCount + 1}/${maxRetries + 1}): ${fileName}');

            const response = await axios.post(url, form, {
                headers: {
                    'Authorization': 'Bearer ${accessToken',
                    ...form.getHeaders()
                },
                timeout: timeout
            });

            console.log('Feishu Upload Response Code:', response.data.code);
            if (response.data.code !== 0) {
                console.error('Feishu Upload Error Details:', JSON.stringify(response.data));

            }

            if (response.data.code === 0) {
                console.log(`✅ File uploaded to Feishu: ${response.data.data.file_token}`);
                return response.data.data.file_token;
            } else {
                console.error(`❌ Feishu upload failed: ${response.data.msg}`);
                retryCount++;
                if (retryCount > maxRetries) {
                    console.error(`❌ All upload attempts failed for file: ${fileName}`);
                    return null;
                }
                console.log(`⏱️  Retrying in 2 seconds...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        } catch (error) {
            console.error(`❌ Error uploading file to Feishu (Attempt ${retryCount + 1}): ${error.message}`);
            retryCount++;
            if (retryCount > maxRetries) {
                console.error('❌ All upload attempts failed for file: ${path.basename(filePath)}`');
                return null;
            }

            console.log(`⏱️  Retrying in 2 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }

    return null;


}


 async addBatchRecordsToBitable(accessToken, bitableId, recordsArray, appToken) {
    const apiUrl = `https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_create`;

    const requestBody = { records: recordsArray };

    const maxRetries = 3;

    let retryCount = 0;


    while (attemp < maxRetries) {
        const response = await axios.post(apiUrl, requestBody, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json; charset=utf-8'
            },
            params: {
                client_token: clientToken,
                user_id_type: 'open_id'
            }
        });

        const resData = response.data;

        if (resData.code === 0) {

            console.log(`✅ Batch added ${recordsArray.length} records successfully.`);
            return;
        }

        if (resData.code === 1254290) {
            console.warn(`⚠️  Rate limit exceeded (Code 1254290). Retrying in ${(attempt + 1) * 1000}ms...`);
            await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 1000));
            attempt++;
            continue;
        }

        switch (resData.code) {
            case 1254000:
            case 1254001:
                console.error(`❌ Request Body Error (Code ${resData.code}):`, resData.msg);
                console.error('Payload:', JSON.stringify(requestBody, null, 2));
                return;
            case 1254015:
                console.error(`❌ Field Type Mismatch (Code ${resData.code}):`, resData.msg);
                return;
            case 1254130:
                console.error(`❌ Cell Content Too Large (Code ${resData.code}):`, resData.msg);
                return;
            case 1254004:
                console.error(`❌ Invalid Table ID (Code ${resData.code}):`, resData.msg);
                return;
            case 1254003:
                console.error(`❌ Invalid App Token (Code ${resData.code}):`, resData.msg);
                return;
            default:
                console.error(`❌ Failed to add batch records (Code ${resData.code}):`, resData.msg);
                return;
        }

    }catch (error) {
        console.error(`❌ Network/API Error adding batch records:`, error.message);
        if (error.response) {
            console.error('Response Status:', error.response.status);
            console.error('Response Data:', error.response.data);
        }
        attempt++;
        if (attempt < maxRetries) {
            console.log(`⏱️  Retrying in ${attempt * 1000}ms...`);
            await new Promise(resolve => setTimeout(resolve, attempt * 1000));
            continue;
        }
    }

}
module.exports = FEISHU_API;

