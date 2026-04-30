# FEISHU_API 类使用说明

## 概述
`FEISHU_API.js` 是一个封装了飞书 API 操作的类，用于简化飞书 API 的调用。支持获取访问令牌、下载图片、上传图片到飞书等功能。

## 文件结构
- `FEISHU_API.js` - 飞书 API 类文件
- `Test_main_V1.0.js` - 主程序文件，展示如何使用 FEISHU_API 类

## 使用方法

### 1. 引入 FEISHU_API 类
```javascript
const FEISHU_API = require('./FEISHU_API');
```

### 2. 创建 FEISHU_API 实例
```javascript
const FEISHU_APP_ID = 'your_app_id';
const FEISHU_APP_SECRET = 'your_app_secret';

const feishuAPI = new FEISHU_API(FEISHU_APP_ID, FEISHU_APP_SECRET);
```

### 3. 获取访问令牌
```javascript
async function example1() {
    // 调用 getTenantAccessToken 方法获取访问令牌
    const accessToken = await feishuAPI.getTenantAccessToken();
    
    if (accessToken) {
        console.log('✅ Access token obtained successfully');
        // 使用 accessToken 进行后续操作
    } else {
        console.error('❌ Failed to obtain access token');
    }
}
```

### 4. 下载图片到本地
```javascript
async function example2() {
    const imageUrl = 'https://example.com/image.jpg';
    const localFilePath = await feishuAPI.downloadImage(imageUrl);
    
    if (localFilePath) {
        console.log(`图片已下载到: ${localFilePath}`);
    }
}
```

### 5. 上传本地图片到飞书
```javascript
async function example3() {
    const filePath = './temp_images/image.jpg';
    const parentNode = 'your_bitable_id'; // 飞书 Bitable ID
    
    const fileToken = await feishuAPI.uploadImageToFeishu(filePath, parentNode);
    
    if (fileToken) {
        console.log(`图片已上传，文件 token: ${fileToken}`);
    }
}
```

### 6. 下载并上传图片（一步完成，推荐）
```javascript
async function example4() {
    const imageUrl = 'https://example.com/image.jpg';
    const parentNode = 'your_bitable_id';
    
    // 第三个参数表示上传后是否删除本地文件，默认为 true
    const fileToken = await feishuAPI.downloadAndUploadImage(imageUrl, parentNode, true);
    
    if (fileToken) {
        console.log(`图片已成功上传，文件 token: ${fileToken}`);
    }
}
```

### 7. 获取已存储的令牌
```javascript
// 如果之前已经调用过 getTenantAccessToken，可以直接获取存储的令牌
const storedToken = feishuAPI.getStoredToken();
```

## FEISHU_API 类方法

### constructor(appId, appSecret)
- **参数**:
  - `appId` (string): 飞书应用 ID
  - `appSecret` (string): 飞书应用密钥
- **说明**: 创建 FEISHU_API 实例

### getTenantAccessToken()
- **返回值**: `Promise<string|null>` - 返回访问令牌或 null
- **说明**: 获取飞书租户访问令牌，成功后会自动存储在实例中

### getStoredToken()
- **返回值**: `string|null` - 返回已存储的访问令牌或 null
- **说明**: 获取当前实例中存储的访问令牌

### downloadImage(imageUrl)
- **参数**:
  - `imageUrl` (string): 图片 URL
- **返回值**: `Promise<string|null>` - 返回本地文件路径或 null
- **说明**: 下载图片到本地 `temp_images` 目录

### uploadImageToFeishu(filePath, parentNode, maxRetries = 3)
- **参数**:
  - `filePath` (string): 本地文件路径
  - `parentNode` (string): 父节点 ID（如 bitable ID）
  - `maxRetries` (number): 最大重试次数，默认 3 次
- **返回值**: `Promise<string|null>` - 返回文件 token 或 null
- **说明**: 上传本地图片到飞书，支持自动重试。如果实例中没有访问令牌，会自动获取

### downloadAndUploadImage(imageUrl, parentNode, deleteAfterUpload = true)
- **参数**:
  - `imageUrl` (string): 图片 URL
  - `parentNode` (string): 父节点 ID（如 bitable ID）
  - `deleteAfterUpload` (boolean): 上传后是否删除本地文件，默认 true
- **返回值**: `Promise<string|null>` - 返回文件 token 或 null
- **说明**: 组合方法，一步完成下载和上传。推荐使用此方法

## 运行示例
```bash
node Test_main_V1.0.js
```

## 完整使用示例
```javascript
const FEISHU_API = require('./FEISHU_API');

async function main() {
    // 创建实例
    const feishuAPI = new FEISHU_API('your_app_id', 'your_app_secret');
    
    // 方式 1: 分步操作
    const accessToken = await feishuAPI.getTenantAccessToken();
    const filePath = await feishuAPI.downloadImage('https://example.com/image.jpg');
    const fileToken = await feishuAPI.uploadImageToFeishu(filePath, 'bitable_id');
    
    // 方式 2: 一步完成（推荐）
    const fileToken2 = await feishuAPI.downloadAndUploadImage(
        'https://example.com/image.jpg',
        'bitable_id',
        true  // 上传后删除本地文件
    );
    
    console.log('文件 token:', fileToken2);
}

main();
```

## 注意事项
1. 确保已安装所需依赖：
   ```bash
   npm install axios form-data
   ```
2. 请妥善保管您的 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`
3. 访问令牌有有效期限制，过期后需要重新获取（类会自动处理）
4. 下载的图片默认保存在 `temp_images` 目录
5. 使用 `downloadAndUploadImage` 方法时，默认会在上传成功后删除本地文件

## 错误处理
所有方法都包含完善的错误处理：
- 返回 `null` 表示操作失败
- 控制台会输出详细的错误信息
- 上传失败时会自动重试（最多 3 次）

## 依赖项
- `axios` - HTTP 请求
- `fs` - 文件系统操作
- `path` - 路径处理
- `form-data` - 表单数据处理
