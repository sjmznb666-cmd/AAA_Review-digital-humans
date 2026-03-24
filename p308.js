#!/usr/bin/env node
const puppeteer = require('puppeteer');
const readline = require('readline');
const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const axios = require('axios');
const crypto = require('crypto');
const FormData = require('form-data');
const { execSync } = require('child_process');

// ==========================================
// Local Logging Configuration
// ==========================================
const LOG_DIR = path.join(process.cwd(), 'logs');

// Create logs directory if it doesn't exist
if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Create a TXT log file for this run
const runTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
const txtLogFileName = `run_log_${runTimestamp}.txt`;
const txtLogFilePath = path.join(LOG_DIR, txtLogFileName);

// Write run start info to TXT log
const runStartMsg = `[${new Date().toLocaleString()}] 程序开始运行\n`;
try {
    fs.writeFileSync(txtLogFilePath, runStartMsg, 'utf8');
    console.log(`📄 TXT日志文件已创建: ${txtLogFilePath}`);
} catch (error) {
    console.error('❌ 创建TXT日志文件失败:', error.message);
}

// Redirect console output to TXT log file
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;
const originalConsoleInfo = console.info;

// Function to write to both console and TXT log
function logToFileAndConsole(...args) {
    // Convert args to string
    const output = args.map(arg => 
        typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
    ).join(' ');
    
    // Write to console
    originalConsoleLog.apply(console, args);
    
    // Write to TXT log file
    try {
        fs.appendFileSync(txtLogFilePath, output + '\n', 'utf8');
    } catch (error) {
        // Fallback: just log to console if file write fails
        originalConsoleError('❌ 写入TXT日志失败:', error.message);
    }
}

// Override console methods
console.log = logToFileAndConsole;
console.error = logToFileAndConsole;
console.warn = logToFileAndConsole;
console.info = logToFileAndConsole;

let allProcessedRecords = [];


// ==========================================
// Configuration Section
// ==========================================

// Feishu App Credentials
const FEISHU_APP_ID = 'cli_a81fe4ed0730900c';
const FEISHU_APP_SECRET = 'wffGOfPCemJA9I3b86e9rlStWWC7OAsf';

// Doubao API Key (Fixed Variable - PLEASE REPLACE WITH YOUR ACTUAL KEY)
const DOUBAO_API_KEY = '4eecf536-7998-448a-bbc5-120d657b1b32';

// Feishu Field Names
const URL_FIELD_NAME = '落地页链接';
const IMAGE_URL_FIELD_NAME = '落地页图片链接'; // New field for image links
const TEXT_CONTENT_FIELD_NAME = '落地页文字内容';
const SCREENSHOT_FIELD_NAME = '结算页截图'; // New field for screenshot
const FIELD_OPERATING_DEPARTMENT = '运营部门'; // New field for operating department

// Audit Result Fields
const FIELD_EXCHANGE = '详情页-换货期限承诺审核的结果';
const FIELD_LOCATION_TIME = '详情页-时长与地点承诺问题审核的结果';
const FIELD_REFUND = '详情页-退货/退款承诺审核的结果';
const FIELD_ORIGIN = '详情页-发货地等信息审核的结果';
const FIELD_TAX = '详情页-无关税/无额外费用承诺审核结果';
const FIELD_SERVICE_TIME = '详情页-售后服务时长的审核结果';
const FIELD_CONTENT_MYTH = '详情页-内容信息神化审核的结果';
const FIELD_NOTE = '备注';

// Doubao API Configuration
const DOUBAO_API_URL = 'https://ark.cn-beijing.volces.com/api/v3/chat/completions';
const DOUBAO_MODEL = 'doubao-seed-1-6-251015';

// ==========================================
// Helper Functions
// ==========================================

/**
 * Fetches the tenant_access_token from the Feishu API.
 */
async function getTenantAccessToken() {
    if (!FEISHU_APP_ID || !FEISHU_APP_SECRET) {
        console.error('\n❌ Error: Please configure FEISHU_APP_ID and FEISHU_APP_SECRET in the script.');
        return null;
    }
    const url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal';
    const requestBody = { app_id: FEISHU_APP_ID, app_secret: FEISHU_APP_SECRET };

    try {
        const response = await axios.post(url, requestBody);
        if (response.data.code === 0) {
            return response.data.tenant_access_token;
        } else {
            console.error('\n❌ Failed to obtain tenant access token:', response.data);
            return null;
        }
    } catch (error) {
        console.error('\nAn unexpected error occurred during Feishu authentication:', error.message);
        return null;
    }
}

/**
 * Downloads an image from a URL to a local file.
 * @param {string} imageUrl URL of the image to download.
 * @returns {Promise<string|null>} Local file path of the downloaded image or null on failure.
 */
async function downloadImage(imageUrl) {
    try {
        console.log(`Downloading image: ${imageUrl}`);

        // Create a temporary directory for images if it doesn't exist
        const imagesDir = path.join(process.cwd(), 'temp_images');
        if (!fs.existsSync(imagesDir)) {
            fs.mkdirSync(imagesDir, { recursive: true });
        }

        // Generate a unique filename based on the URL and timestamp
        const urlHash = Buffer.from(imageUrl).toString('base64').replace(/\//g, '_').replace(/\+/g, '-').substring(0, 20);
        const timestamp = Date.now();
        const fileExt = path.extname(new URL(imageUrl).pathname) || '.jpg';
        const fileName = `image_${urlHash}_${timestamp}${fileExt}`;
        const filePath = path.join(imagesDir, fileName);

        // Download the image
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
 * Uploads a local file to Feishu and returns the file_token.
 * @param {string} filePath Local path to the file.
 * @param {string} accessToken Tenant access token.
 * @param {string} parentNode The parent node (App Token).
 * @returns {Promise<string|null>} The file_token or null on failure.
 */
async function uploadFileToFeishu(filePath, accessToken, parentNode, maxRetries = 3) {
    const url = 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all';
    const timeout = 10000; // 10秒超时

    let retryCount = 0;
    while (retryCount <= maxRetries) {
        try {
            const fileStream = fs.createReadStream(filePath);
            const stats = fs.statSync(filePath);
            const fileSizeInBytes = stats.size;
            const fileName = path.basename(filePath);

            const form = new FormData();
            form.append('file_name', fileName);
            form.append('parent_type', 'bitable_image'); // Use bitable_image for Bitable attachments
            form.append('parent_node', parentNode); // Use dynamic parentNode (App Token)
            form.append('size', fileSizeInBytes);
            form.append('file', fileStream);

            console.log(`🔄 Uploading file to Feishu (Attempt ${retryCount + 1}/${maxRetries + 1}): ${fileName}`);
            
            const response = await axios.post(url, form, {
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    ...form.getHeaders()
                },
                timeout: timeout // 设置超时时间
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
                await new Promise(resolve => setTimeout(resolve, 2000)); // 等待2秒后重试
            }
        } catch (error) {
            console.error(`❌ Error uploading file to Feishu (Attempt ${retryCount + 1}): ${error.message}`);
            retryCount++;
            if (retryCount > maxRetries) {
                console.error(`❌ All upload attempts failed for file: ${path.basename(filePath)}`);
                return null;
            }
            console.log(`⏱️  Retrying in 2 seconds...`);
            await new Promise(resolve => setTimeout(resolve, 2000)); // 等待2秒后重试
        }
    }
    return null;
}



/**
 * Calls the Doubao API to audit/summarize text.
 */
/**
 * Calls the Doubao API to audit/summarize text.
 */
async function callDoubaoAPI(text) {
    if (!text || text.length === 0) return "无文本内容";

    // Truncate text to avoid token limits if necessary (simple truncation)
    const truncatedText = text.substring(0, 30000);

    const prompt = `You will act as an assistant reviewing landing pages's text content based on the rules outlined below. Your task is to carefully read the rules and examine the landing page text, conduct a comprehensive review of the images according to the rules, and provide review results along with corrective suggestions.
First, please carefully review the following rules and the refer the rules to examine the landing page text content:
<Rule Content>
### 1. Review of Mythical Claims in Content
- Landing pages and advertising post images must not use mythical or unrealistic claims about product functionality.
- This issue is commonly found in three major categories: skincare products, apparel, and tea bags. When reviewing these categories, strictly rectify exaggerated or mythical content.
    - Landing page images promoting products with Black Friday-level efficacy claims (e.g., medicinal disease treatment, direct weight loss effects [excluding exercise equipment], breast enhancement, liver repair, sexual function enhancement, aphrodisiac effects, lymphatic system detoxification) will be deemed [Rejected].
    - For skincare products claiming whitening, spot removal, mole removal, dark circle reduction, eye bag reduction, or wrinkle reduction, the effective period must be ≥7 days. Claims of effectiveness in less than 7 days are considered exaggerated, while claims of effectiveness within minutes or seconds are considered instant effect claims. Special attention must be paid to real-person demonstration materials. Any use of keywords implying instant results (“instant,” “immediately,” “instantly,” “temporary”) that describe immediate effectiveness will be deemed [Rejected].
    - Apparel product promotions claiming exaggerated elasticity, exaggerated butt-lifting effects, exaggerated tummy-control shaping, exaggerated puncture-resistant functionality, or exaggerated support/lifting effects for undergarments will be deemed [Rejected].
    - Tea bag product promotions claiming medicinal disease-curing effects, direct weight loss, breast enhancement, liver repair, sexual function enhancement, or virility boosting will be deemed [Rejected].

### 2. Exchange Period Commitment
- Landing pages mentioning exchanges must clearly state(if exchanges have mentioned,Note has stated the situarion clearly): Exchange requests are accepted for products damaged during shipping or with defects arising from normal use so on, and must be completed within 15 days of delivery confirmation. This policy does not apply after 15 days. Therefore, any exchange requests exceeding the 15-day period will be [Rejected],for instance," If your items arrive damaged usage, we will gladly issue out a within 90 days of normal replacement or refund" deems to [Rejected], while those within 15 days will be [approved].
- Note: Landing pages typically outline detailed exchange terms. If an explicit exchange period is stated, verify that all mentioned timeframes ≤ 15 days. If no explicit period appears (e.g., phrases like “Hassle-Free Returns You Can Count On”), this indicates an exchange policy exists. However, the absence of a precise timeframe for exchanges is also considered [Approved]. Do not confuse this with refund/after-sales periods, as they follow different review rules.
- Example: If your item arrives damaged or malfunctions within xxx days of normal use, we'll gladly replace it or issue a refund.

### 3. Shipping Duration Review
- If the shipping duration stated on the landing page is one week or longer, it is considered [Approved]. If the shipping duration is less than one week, it is considered [Not Approved]. Note: Vague phrasing such as “Fast Shipping TODAY ONLY,” “most efficient mode of transportation,” “Prioritize shipping from the nearest warehouse,” or “Please allow for processing time”—which does not explicitly guarantee delivery within a specific timeframe—is also considered [Passed].

### 4. Shipping Duration, Order Processing Duration, and Delivery Duration Review
- Landing pages or checkout pages displaying specific shipping durations, order processing times, or delivery timelines are deemed [Rejected],Note:"stop shipping time" is not the shipping duration,deeming to  [Approved]
- Examples: “Handling time >> Ship fastest after payment,” “Shipping time as soon as possible”—vague expressions not specifying exact dates/times are deemed [Approved],for instance,"shipping immediately" deems to  [Approved]; “Shipping within three days” is deemed [Rejected]. Expressions directly stating time, such as “Order processing time within 7 days,” are deemed [Rejected].

### 5. Origin/Manufacturing Location Review
- Acceptable [Pass] scenarios:
    - Displaying only a national flag without specifying the country of production or origin in or near the image.
    - Content that uses well-known ingredients from a specific region, renowned technology from a country, craftsmanship from a country, or a country's style, or where the product name includes a location: e.g., Jeju, Korea xx ingredient, German forging technology, Italian craftsmanship, Panama hat, Dutch tulip, German chamomile, Japanese matcha powder, etc.
- Non-Approved Cases:
  - Shipping Origin: Unreasonable shipping warehouse location promises mentioned in landing page/checkout page copy or visuals (all locations outside China). Note: Expressions like “we have warehouses all over the world” or “ships from the nearest warehouse”—which do not explicitly state a non-specific shipping origin—are also considered approved.
  - Country of Origin: Landing pages or checkout pages mentioning unreasonable manufacturing location promises (any location outside China).

### 6. No Tariff/No Additional Fees Commitment Review
- Landing pages must not include any promises regarding hidden fees, taxes, or “one-time payment” related to additional charges. Otherwise, it is deemed [Rejected].

### 7. Return/Refund Review and After-sales service duration
- After-sales service duration benchmark: If a clear timeframe is specified for the service cycle (e.g., returns/exchanges/ Warranty) and this duration does not exceed 180 days (6 months), it is deemed [Approved]; if it exceeds 180 days (6 months), it is deemed [Rejected],for instance,"EVERY PRODUCT INCLUDES A 24-MONTH, WORRY-FREE GUARANTEE" deems to [Rejected] .
- Common Unreasonable Scenarios: 100% unconditional full refunds (though not time-limited to 180 days, using exaggerated claims like “unconditional” or “no-questions-asked returns”), free returns (no 180-day limit), XX-year trade-in programs (no 180-day limit), or full refunds for undelivered items are all deemed [Fail]. However, special note: The following situations require priority consideration: When not directly modifying financial terms like “refund” or “money,” expressions such as “100% guaranteed refund” (indicating refund assurance but not full refund) or “100% risk-free purchase” (indicating purchase without risk) or '100% No-Risk Money Back Guarantee' or "if for ANY reason you are not satisfied with your purchase, we offer iron-clad money back guarantee"will be deemed [Approved]. Therefore, any 100% statement not directly modified by financial terms like ‘money’ or “refund” is considered [Approved]. “100% Refund Guarantee” or "refund your money no questiones asked" and so so...constitute a full refund commitment and will be deemed [Rejected].
- If the after-sales period does not explicitly state a specific timeframe (e.g., within 30 days, within six months, etc.) and does not include expressions of common unreasonable circumstances, it will also be deemed [Approved].

Now, carefully review the following landing page content and understand each element:
<Landing Page Content>
${truncatedText}
</Landing Page Content>

Output each review result in the following format, with one result per line:
For approved items: Output the review item (skill title) + result
详情页-时长与地点承诺问题审核的结果: 【Approved】

For rejected items: Output the review item + result + rejection reason
详情页-时长与地点承诺问题审核的结果: 【Rejected】, requires revision (involves explicit time commitments like “Ships within 1 business day”)

Final output example:
{   "落地页文字内容":${truncatedText},
    "详情页-内容信息神化审核的结果": "【Approved】",
    "详情页-换货期限承诺审核的结果": "【Rejected】, requires revision (mentions replacement within 90 days which exceeds the 15-day exchange period limit)",
    "详情页-时长与地点承诺问题审核的结果": "【Approved】",
    "详情页-售后服务时长的审核结果": "【Approved】",
    "详情页-发货地等信息审核的结果": "【Rejected】, requires revision (mentions USA Made and USA Shipped which are outside China)",
    "详情页-无关税/无额外费用承诺审核结果": "【Approved】",
    "详情页-退换货审核的结果": "【Approved】"
}

Restrictions:
- Responses must strictly adhere to webpage content review rules; do not fabricate information, use non-rule-based content as judgment criteria, or address unrelated topics.
- Responses must strictly adhere to the prescribed format for each rule and cannot deviate from requirements.
- Whether content passes review requires examining the entire sentence or paragraph in question, rather than making a judgment based solely on the presence of certain keywords so you should pay attention to the precondition.
- Do not fabricate landing page content; all responses must match the original landing page content and logical reasoning.
- If the landing page content does not involve or miss a specific rule, it is also considered 【Approved】.
- If could not find content involved the review rules, it is also considered 【Approved】,for example,if the landing page content does not mention the rule 1 and rule 3,but it mentions the other rules, it is also considered 【Approved】.
- Especiall,Obey the special notes first among the rules.`;


    try {
        const response = await axios.post(DOUBAO_API_URL, {
            model: DOUBAO_MODEL,
            max_completion_tokens: 32768,
            messages: [
                {
                    role: "user",
                    content: [
                        {
                            type: "text",
                            text: prompt
                        }
                    ]
                }
            ],
            reasoning_effort: "high"
        }, {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${DOUBAO_API_KEY}`
            }
        });

        if (response.data && response.data.choices && response.data.choices.length > 0) {
            return response.data.choices[0].message.content;
        } else {
            console.error('Doubao API returned unexpected format:', response.data);
            return "API调用失败或无返回";
        }
    } catch (error) {
        console.error('Doubao API Error:', error.message);
        if (error.response) {
            console.error('Response Data:', JSON.stringify(error.response.data, null, 2));
        }
        return `API Error: ${error.message}`;
    }
}

/**
 * Parses the Doubao API response to extract field values.
 */
/**
 * Parses the Doubao API response to extract field values.
 */
function parseDoubaoResponse(responseText) {
    let data = {};

    // Try parsing as JSON first
    try {
        data = JSON.parse(responseText);
    } catch (e) {
        // Fallback to Regex extraction if JSON parse fails
        const extract = (key) => {
            const jsonRegex = new RegExp(`"${key}"\\s*:\\s*"([^"]+)"`, 'i');
            const jsonMatch = responseText.match(jsonRegex);
            if (jsonMatch) return jsonMatch[1];

            const lineRegex = new RegExp(`${key}[：:]\\s*(.+)`, 'i');
            const lineMatch = responseText.match(lineRegex);
            if (lineMatch) {
                return lineMatch[1].replace(/,$/, '').trim();
            }
            return null;
        };

        data['详情页-内容信息神化审核的结果'] = extract('详情页-内容信息神化审核的结果');
        data['详情页-换货期限承诺审核的结果'] = extract('详情页-换货期限承诺审核的结果');
        data['详情页-时长与地点承诺问题审核的结果'] = extract('详情页-时长与地点承诺问题审核的结果');
        data['详情页-售后服务时长的审核结果'] = extract('详情页-售后服务时长的审核结果');
        data['详情页-发货地等信息审核的结果'] = extract('详情页-发货地等信息审核的结果');
        data['详情页-无关税/无额外费用承诺审核结果'] = extract('详情页-无关税/无额外费用承诺审核结果');

        let refundVal = extract('详情页-退货/退款承诺审核的结果');
        if (!refundVal) refundVal = extract('详情页-退换货审核的结果');
        data['详情页-退货/退款承诺审核的结果'] = refundVal;
    }

    const fields = {};

    // Direct mapping - if field is not present in response, default to Approved
    // This ensures we have a complete set of fields for validation
    fields[FIELD_CONTENT_MYTH] = data['详情页-内容信息神化审核的结果'] || "【Approved】";
    fields[FIELD_EXCHANGE] = data['详情页-换货期限承诺审核的结果'] || "【Approved】";
    fields[FIELD_LOCATION_TIME] = data['详情页-时长与地点承诺问题审核的结果'] || "【Approved】";
    fields[FIELD_SERVICE_TIME] = data['详情页-售后服务时长的审核结果'] || "【Approved】";
    fields[FIELD_ORIGIN] = data['详情页-发货地等信息审核的结果'] || "【Approved】";
    fields[FIELD_TAX] = data['详情页-无关税/无额外费用承诺审核结果'] || "【Approved】";

    // Handle Key Variation for Refund - if field is not present in response, default to Approved
    fields[FIELD_REFUND] = data['详情页-退换货审核的结果'] || data['详情页-退货/退款承诺审核的结果'] || "【Approved】";

    return { fields };
}

/**
 * Adds multiple records to Feishu Bitable using batch_create.
 * Accepts an array of record objects (each wrapped in { fields: ... }).
 */
async function addBatchRecordsToBitable(accessToken, appToken, tableId, recordsArray) {
    const apiUrl = `https://open.feishu.cn/open-apis/bitable/v1/apps/${appToken}/tables/${tableId}/records/batch_create`;

    const requestBody = { records: recordsArray };

    const maxRetries = 3;
    let attempt = 0;

    while (attempt < maxRetries) {
        const clientToken = crypto.randomUUID();

        try {
            const response = await axios.post(apiUrl, requestBody, {
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json; charset=utf-8',
                },
                params: {
                    client_token: clientToken,
                    user_id_type: 'open_id'
                }
            });

            const resData = response.data;

            if (resData.code === 0) {
                console.log(`✅ Batch added ${recordsArray.length} records successfully.`);
                return; // Success
            }

            // Handle Rate Limiting
            if (resData.code === 1254290) {
                console.warn(`⚠️  Rate limit exceeded (Code 1254290). Retrying in ${(attempt + 1) * 1000}ms...`);
                await new Promise(resolve => setTimeout(resolve, (attempt + 1) * 1000));
                attempt++;
                continue;
            }

            // Handle Specific Errors
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

        } catch (error) {
            console.error(`❌ Network/API Error adding batch records:`, error.message);
            if (error.response) {
                console.error('Response Status:', error.response.status);
                console.error('Response Data:', error.response.data);
            }
            attempt++;
            if (attempt < maxRetries) {
                console.log(`Retrying network error in 1s...`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }
}

// ==========================================
// Main Execution
// ==========================================
(async () => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    const ask = (q) => new Promise(r => rl.question(q, r));

    try {
        // 1. Inputs
        const excelPathInput = await ask('输入Excel文件路径: ');
        let excelPath = excelPathInput.trim().replace(/^"|"$/g, '') || 'urls_v3.xlsx';

        if (!excelPath.toLowerCase().endsWith('.xlsx')) excelPath += '.xlsx';
        if (!fs.existsSync(excelPath)) {
            console.error(`File not found: ${excelPath}`);
            return;
        }

        const appToken = (await ask('输入飞书 APP_TOKEN: ')).trim();
        const tableId = (await ask('输入飞书 TABLE_ID: ')).trim();

        if (!appToken || !tableId) {
            console.error('Missing required credentials.');
            return;
        }

        if (DOUBAO_API_KEY === 'YOUR_ARK_API_KEY') {
            console.warn('\n⚠️  WARNING: You are using the placeholder API Key. Please edit the script to set `DOUBAO_API_KEY`.\\n');
        }

        // 2. Read Excel
        const workbook = xlsx.readFile(excelPath);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1 });

        // Assuming URL is in the 2nd column (index 1) and Operating Department is in the 6th column (index 5)
        const urlData = data.slice(1).map(row => {
            const url = row[1];
            const operatingDepartment = row[5];
            return { url, operatingDepartment };
        }).filter(item => item.url && typeof item.url === 'string').map(item => ({
            url: item.url.startsWith('http') ? item.url : `http://${item.url}`,
            operatingDepartment: item.operatingDepartment || ''
        }));

        console.log(`Found ${urlData.length} URLs with operating department information.`);

        // 3. Setup Puppeteer - Enhanced Chrome Detection
        // Function to detect Chrome browser
        async function detectChrome() {
            // Common Chrome paths
            const commonPaths = [
                // Portable Chrome in the same directory as the executable
                path.join(path.dirname(process.execPath), 'chrome-win', 'chrome.exe'),
                path.join(path.dirname(process.execPath), 'chromium', 'chrome.exe'),

                // System installed Chrome
                'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
                'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
                path.join(process.env.LOCALAPPDATA || '', 'Google\\Chrome\\Application\\chrome.exe'),

                // Edge browser (alternative if Chrome not found)
                'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
                'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
                path.join(process.env.LOCALAPPDATA || '', 'Microsoft\\Edge\\Application\\msedge.exe'),
            ];

            // First try common paths
            for (const path of commonPaths) {
                if (fs.existsSync(path)) {
                    console.log(`Found Chrome/Edge at: ${path}`);
                    return path;
                }
            }

            // Try to find Chrome via Windows Registry
            try {
                console.log('Searching for Chrome in Registry...');
                // Chrome registry key
                const chromeOutput = execSync('reg query "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe" /ve', { encoding: 'utf8' }).toString();
                const chromeMatch = chromeOutput.match(/REG_SZ\s+(.*)/);
                if (chromeMatch && chromeMatch[1] && fs.existsSync(chromeMatch[1].trim())) {
                    const chromePath = chromeMatch[1].trim();
                    console.log(`Found Chrome in Registry: ${chromePath}`);
                    return chromePath;
                }
            } catch (e) {
                console.log('Chrome Registry lookup failed.');
            }

            // Try to find Edge via Windows Registry
            try {
                console.log('Searching for Edge in Registry...');
                const edgeOutput = execSync('reg query "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\App Paths\\msedge.exe" /ve', { encoding: 'utf8' }).toString();
                const edgeMatch = edgeOutput.match(/REG_SZ\s+(.*)/);
                if (edgeMatch && edgeMatch[1] && fs.existsSync(edgeMatch[1].trim())) {
                    const edgePath = edgeMatch[1].trim();
                    console.log(`Found Edge in Registry: ${edgePath}`);
                    return edgePath;
                }
            } catch (e) {
                console.log('Edge Registry lookup failed.');
            }

            // Try to find Chrome in Program Files
            try {
                console.log('Searching for Chrome in Program Files...');
                const programFilesDirs = [
                    'C:\\Program Files\\Google\\Chrome',
                    'C:\\Program Files (x86)\\Google\\Chrome',
                    'C:\\Program Files\\Google',
                    'C:\\Program Files (x86)\\Google',
                ];

                for (const dir of programFilesDirs) {
                    if (fs.existsSync(dir)) {
                        const chromeExe = path.join(dir, 'Application', 'chrome.exe');
                        if (fs.existsSync(chromeExe)) {
                            console.log(`Found Chrome in Program Files: ${chromeExe}`);
                            return chromeExe;
                        }
                    }
                }
            } catch (e) {
                console.log('Program Files search failed.');
            }

            // If we can't find Chrome automatically, ask the user
            console.log('\n❌ Could not find Chrome/Edge automatically.');
            console.log('Please provide the path to Chrome executable:');

            while (true) {
                const userInput = await ask('Chrome/Edge executable path: ');
                const trimmedPath = userInput.trim().replace(/^"|"$/g, '');

                if (trimmedPath && fs.existsSync(trimmedPath)) {
                    console.log(`Using provided Chrome path: ${trimmedPath}`);
                    return trimmedPath;
                } else {
                    console.log('❌ Invalid path. Please enter a valid path to Chrome/Edge executable.');
                    console.log('Example: C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe');
                }
            }
        }

        // Try to detect Chrome with enhanced logic
        let executablePath;
        try {
            console.log('Looking for Chrome/Edge browser...');
            executablePath = await detectChrome();
        } catch (e) {
            console.error('Error detecting Chrome:', e.message);
            console.log('Falling back to Puppeteer default...');
        }

        const browser = await puppeteer.launch({
            headless: true,// 无头模式
            defaultViewport: { width: 1980, height: 1080 },
            executablePath: executablePath,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        const page = await browser.newPage();

        // 4. Get Feishu Token
        let accessToken = await getTenantAccessToken();
        if (!accessToken) return;

        // 5. Process URLs
        for (let i = 0; i < urlData.length; i++) {
            // Refresh token every 40 URLs
            if (i > 0 && i % 10 === 0) {
                console.log(`\n🔄 [${i}/${urlData.length}] Refreshing Tenant Access Token (10 URLs processed)...`);
                const newToken = await getTenantAccessToken();
                if (newToken) {
                    accessToken = newToken;
                    console.log('✅ Token refreshed successfully.');
                } else {
                    console.warn('⚠️ Failed to refresh token. Continuing with old token...');
                }
            }
            const { url, operatingDepartment } = urlData[i];
            console.log(`\n[${i + 1}/${urlData.length}] Processing: ${url}`);
            console.log(`   Operating Department: ${operatingDepartment || 'Not specified'}`);

            try {
                // Set viewport explicitly for each page to ensure consistency
                await page.setViewport({ width: 1980, height: 1080 });
                
                // 添加超时处理
                let response;// 用于存储页面响应
                try {
                    response = await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
                } catch (gotoError) {
                    if (gotoError.name === 'TimeoutError' || gotoError.message.includes('Timeout')) {
                        console.warn(`⚠️ Timeout occurred when accessing ${url}. Adding to manual review list.`);
                        
                        // 创建包含超时信息的记录并写入飞书
                        const timeoutRecord = {
                            fields: {
                                [URL_FIELD_NAME]: url.replace(/^https?:\/\//, ''),
                                [FIELD_NOTE]: '需要人工审核该落地页 - 页面加载超时'
                            }
                        };
                        
                        // 立即写入飞书
                        await addBatchRecordsToBitable(accessToken, appToken, tableId, [timeoutRecord]);
                        
                        // 添加到已处理记录列表
                        allProcessedRecords.push({
                            url: url,
                            timestamp: new Date().toISOString(),
                            auditNote: '页面加载超时',
                            hasScreenshot: false,
                            uploadedImagesCount: 0,
                            recordData: timeoutRecord
                        });
                        
                        // 保存增量日志
                        saveLogsToLocal();
                        
                        // 继续处理下一个URL
                        continue;
                    }
                    throw gotoError; // 重新抛出非超时错误
                }

                // Check for 404 or Not Found
                const is404 = response.status() === 404;
                const pageTitle = await page.title();
                const pageBody = await page.evaluate(() => document.body.innerText.toLowerCase());
                const isNotFoundText = pageTitle.toLowerCase().includes('404') ||
                    pageTitle.toLowerCase().includes('not found') ||
                    pageBody.includes('404 not found') ||
                    pageBody.includes('page not found') ||
                    pageBody.includes('the store is currently closed. please contact the store administrator to open the store.');

                if (is404 || isNotFoundText) {
                    console.warn(`⚠️ Skipping ${url}: Page not found (404 or text match).`);
                    continue;
                }

                // ---------------------------------------------------------
                // Step 1: Text Extraction & Audit
                // ---------------------------------------------------------
                const bodyText = await page.evaluate(() => document.body.innerText);
                console.log(`Extracted ${bodyText.length} characters of raw text.`);

                // Clean up unnecessary spaces and newlines to reduce token usage
                const cleanText = bodyText
                    // Replace multiple spaces with single space
                    .replace(/\s+/g, ' ')
                    // Remove leading/trailing whitespace
                    .trim();

                console.log(`Cleaned text to ${cleanText.length} characters.`);

                // Special handling for "Not authorized" error
                if (cleanText.match(/Not\s+authorized/i)) {
                    console.log('⚠️ Detected "Not authorized" error in page content.');

                    // Create a basic record
                    const parsedRecord = {
                        fields: {
                            [URL_FIELD_NAME]: url.replace(/^https?:\/\//, ''),
                            [TEXT_CONTENT_FIELD_NAME]: cleanText.substring(0, 5000),
                            [FIELD_NOTE]: '需人工核验该落地页',
                            [FIELD_OPERATING_DEPARTMENT]: operatingDepartment
                        }
                    };

                    // Write the record to Feishu immediately
                    const recordsToSubmit = [parsedRecord];
                    console.log('Writing basic record to Feishu...');

                    // Batch submission with chunk size 1
                    const BATCH_SIZE = 1;
                    for (let i = 0; i < recordsToSubmit.length; i += BATCH_SIZE) {
                        const chunk = recordsToSubmit.slice(i, i + BATCH_SIZE);
                        await addBatchRecordsToBitable(accessToken, appToken, tableId, chunk);
                    }

                    // Skip to next URL
                    continue;
                }

                console.log('Calling Doubao API (Async)...');
                const auditPromise = callDoubaoAPI(cleanText);
                // const auditResult = await callDoubaoAPI(bodyText); // Deferred
                // console.log('Audit Result received.');

                // Parse deferred
                // const parsedRecord = parseDoubaoResponse(auditResult);

                // ---------------------------------------------------------
                // Step 2: Image Extraction
                // ---------------------------------------------------------
                const imageLinks = await page.evaluate(() => {
                    // 设定最小尺寸阈值
                    const MIN_SIZE = 300;

                    // =======================================================
                    // ✨ 核心函数：提取图片的唯一标识和尺寸数字
                    // 返回 { coreUrl: '不含尺寸的链接', size: 数字 }
                    // =======================================================
                    const getCoreUrlAndSize = (url) => {
                        if (!url) return { coreUrl: null, size: 0 };

                        let cleanedUrl = String(url).trim();
                        let size = 0;

                        // 1. 移除查询参数 (?w=300, ?q=90 等)
                        cleanedUrl = cleanedUrl.substring(0, cleanedUrl.indexOf('?') !== -1 ? cleanedUrl.indexOf('?') : cleanedUrl.length);

                        // 2. 查找并提取文件名中的尺寸标识 (-[数字].[扩展名])
                        const lastDotIndex = cleanedUrl.lastIndexOf('.');
                        if (lastDotIndex === -1) {
                            return { coreUrl: cleanedUrl, size: 0 };
                        }

                        const extension = cleanedUrl.substring(lastDotIndex);
                        const filenameWithoutExt = cleanedUrl.substring(0, lastDotIndex);

                        // 正则表达式：匹配文件名末尾的 '-[一个或多个数字]'
                        const match = filenameWithoutExt.match(/-\d+$/);

                        let coreUrl = cleanedUrl;
                        if (match) {
                            // 提取数字部分
                            size = parseInt(match[0].substring(1), 10); // match[0] 是 '-200', substring(1) 得到 '200'

                            // 移除尺寸标识，得到核心文件名
                            const coreFilename = filenameWithoutExt.replace(match[0], '');

                            // 重新组合核心 URL
                            coreUrl = coreFilename + extension;
                        }

                        return { coreUrl, size };
                    };

                    // 用于存储去重后链接的核心 Map： Key=核心链接, Value={url: 原始完整链接, size: 尺寸数字}
                    const uniqueLinksMap = new Map();

                    document.querySelectorAll('img').forEach(img => {
                        // ✨ 尺寸筛选机制
                        if (img.clientWidth < MIN_SIZE || img.clientHeight < MIN_SIZE) {
                            return;
                        }

                        // 1. 优先检查懒加载属性 data-url 或 data-src，并明确不再使用 img.src
                        const dataUrl = img.getAttribute('data-url') || img.getAttribute('data-src');
                        let finalUrl = dataUrl || null;

                        if (finalUrl) {
                            if (finalUrl.startsWith('//')) {
                                finalUrl = 'https:' + finalUrl;
                            } else if (!finalUrl.startsWith('http')) {
                                return; // 跳过相对路径或非网络链接
                            }

                            // 2. 解析链接，获取核心URL和尺寸
                            const { coreUrl, size } = getCoreUrlAndSize(finalUrl);

                            if (!coreUrl) return;

                            // 3. 核心筛选逻辑：只保留数字最大的链接
                            const existingEntry = uniqueLinksMap.get(coreUrl);

                            if (!existingEntry || size > existingEntry.size) {
                                // 如果是新链接，或新链接的尺寸数字大于已有的，则替换
                                uniqueLinksMap.set(coreUrl, { url: finalUrl, size: size });
                            }
                        }

                        // 4. (进阶) 检查 data-srcset 或 srcset 中的所有 URL 
                        const srcset = img.getAttribute('data-srcset') || img.srcset;
                        if (srcset) {
                            srcset.split(',').forEach(part => {
                                const url = part.trim().split(/\s+/)[0];
                                if (url && (url.startsWith('http') || url.startsWith('//'))) {
                                    let fullUrl = url.startsWith('//') ? 'https:' + url : url;

                                    const { coreUrl, size } = getCoreUrlAndSize(fullUrl);
                                    if (!coreUrl) return;

                                    const existingEntry = uniqueLinksMap.get(coreUrl);

                                    // 仅当新链接的尺寸数字大于已有的，才替换
                                    if (!existingEntry || size > existingEntry.size) {
                                        uniqueLinksMap.set(coreUrl, { url: fullUrl, size: size });
                                    }
                                }
                            });
                        }
                    });

                    // 最终返回 Map 中所有存储的原始完整链接 (Value.url)
                    return Array.from(uniqueLinksMap.values()).map(entry => entry.url);
                });

                // --------------------------------------------------------- 
                // Download images and get their local paths
                console.log(`\n📥 Downloading ${imageLinks.length} images to local cache...`);
                const downloadedImages = [];
                for (const imgLink of imageLinks) {
                    const localPath = await downloadImage(imgLink);
                    if (localPath) {
                        downloadedImages.push({ url: imgLink, path: localPath });
                    } else {
                        console.warn(`⚠️ Failed to download image: ${imgLink}`);
                    }
                }

                // Upload downloaded images to Feishu and get file tokens
                console.log(`\n📤 Uploading ${downloadedImages.length} cached images to Feishu...`);
                const uploadedImages = [];
                for (const img of downloadedImages) {
                    const fileToken = await uploadFileToFeishu(img.path, accessToken, appToken);
                    if (fileToken) {
                        uploadedImages.push({ url: img.url, path: img.path, fileToken: fileToken });
                        console.log(`✅ Image uploaded to Feishu. Token: ${fileToken}`);
                    } else {
                        console.warn(`⚠️ Failed to upload image to Feishu: ${img.url}`);
                    }
                }

                // ---------------------------------------------------------
                // Step 2.5: Checkout Automation & Screenshot
                // ---------------------------------------------------------
                let screenshotUrl = null;
                let checkoutAuditNote = null; // Initialize audit note for checkout errors
                try {
                    console.log('Starting Checkout Automation...');

                    // Helper for robust clicking
                    const safeClick = async (elementHandle, name) => {
                        try {
                            await elementHandle.evaluate(el => el.scrollIntoView({ block: 'center', inline: 'center' }));
                            await new Promise(r => setTimeout(r, 1000)); // Increased wait
                            await elementHandle.click();
                            console.log(`Clicked "${name}" (standard).`);
                        } catch (e) {
                            console.warn(`Standard click failed for "${name}": ${e.message}. Trying JS click...`);
                            try {
                                await elementHandle.evaluate(el => el.click());
                                console.log(`Clicked "${name}" (JS fallback).`);
                            } catch (e2) {
                                throw new Error(`Failed to click "${name}": ${e2.message}`);
                            }
                        }
                    };



                    // 1. Set Quantity to 5
                    console.log('Attempting to set quantity to 5...');
                    try {
                        const quantitySet = await page.evaluate(() => {
                            // Helper to find input
                            const findInput = () => {
                                // Priority 1: Exact matches by ID or Name
                                const exact = document.querySelector('input[name="quantity"], #Quantity, #quantity, .quantity-selector input');
                                if (exact) return exact;

                                // Priority 2: Label search
                                const labels = Array.from(document.querySelectorAll('label'));
                                const qtyLabel = labels.find(l => l.innerText.trim().toLowerCase() === 'quantity' || l.innerText.trim().toLowerCase() === 'qty' || l.innerText.trim() === '数量' || l.innerText.trim() === 'Menge');
                                if (qtyLabel) {
                                    if (qtyLabel.control) return qtyLabel.control;
                                    // Search nearby
                                    let sibling = qtyLabel.nextElementSibling;
                                    while (sibling) {
                                        if (sibling.tagName === 'INPUT') return sibling;
                                        const innerInput = sibling.querySelector('input');
                                        if (innerInput) return innerInput;
                                        sibling = sibling.nextElementSibling;
                                    }
                                }
                                return null;
                            };

                            const input = findInput();
                            if (input) {
                                input.value = '5';
                                input.dispatchEvent(new Event('input', { bubbles: true }));
                                input.dispatchEvent(new Event('change', { bubbles: true }));
                                input.dispatchEvent(new Event('blur', { bubbles: true }));
                                return true;
                            }
                            return false;
                        });

                        if (quantitySet) {
                            console.log('Quantity set to 5.');
                        } else {
                            console.warn('⚠️ Quantity input not found. Using default.');
                            checkoutAuditNote = '包邮政策未检验-需人工审核结算页面';
                        }
                    } catch (e) {
                        console.error('Error setting quantity:', e.message);
                        checkoutAuditNote = '包邮政策未检验-需人工审核结算页面';
                    }

                    // Wait for state update
                    await new Promise(r => setTimeout(r, 1000));

                    // 添加轻微滑动滚轮，方便找到购买按钮
                    console.log('Performing slight scroll to help locate purchase buttons...');
                    await page.evaluate(() => {
                        // 轻微向下滑动页面100像素，然后再向上滑动400像素，帮助找到购买按钮
                        window.scrollBy(0, 300);
                    });
                    await new Promise(r => setTimeout(r, 500));
                    await page.evaluate(() => {
                        window.scrollBy(0, -100);
                    });
                    await new Promise(r => setTimeout(r, 500));

                    // 2. Click "BUY IT NOW" or "ADD TO CART"
                    console.log('Looking for checkout buttons...');

                    let checkoutNavigated = false;

                    // Strategy: Look for "BUY IT NOW" first
                    // 1. Global Search
                    let buyItNowBtn = await page.evaluateHandle(() => {
                        const buttons = Array.from(document.querySelectorAll('button, a, input[type="submit"], div[role="button"]'));
                        return buttons.find(b => {
                            const t = b.innerText.trim().toUpperCase();
                            return (t.includes('BUY IT NOW') || t === 'BUY NOW' || t.includes('立即购买') || t === 'JETZT KAUFEN') && b.offsetParent !== null;
                        });
                    });

                    // 2. Relative Search (if global failed) - Look near "ADD TO CART"
                    if (!buyItNowBtn.asElement()) {
                        console.log('Global "BUY IT NOW" search failed. Trying relative search near "ADD TO CART"...');
                        buyItNowBtn = await page.evaluateHandle(() => {
                            // Find Add to Cart first
                            const buttons = Array.from(document.querySelectorAll('button, a, input[type="submit"], div[role="button"]'));
                            const addToCart = buttons.find(b => {
                                const t = b.innerText.trim().toUpperCase();
                                return (t.includes('ADD TO CART') || t.includes('ADD TO BAG') || t.includes('加入购物车') || t === 'IN DEN WARENKORB') && b.offsetParent !== null;
                            });

                            if (addToCart) {
                                // Look at next siblings
                                let sibling = addToCart.nextElementSibling;
                                while (sibling) {
                                    // Check the sibling itself
                                    if (sibling.innerText && (sibling.innerText.toUpperCase().includes('BUY IT NOW') || sibling.innerText.toUpperCase() === 'BUY NOW' || sibling.innerText.includes('立即购买') || sibling.innerText.toUpperCase() === 'JETZT KAUFEN')) {
                                        return sibling;
                                    }
                                    // Check children of sibling
                                    const childBtn = sibling.querySelector('button, a, input[type="submit"], div[role="button"]');
                                    if (childBtn) {
                                        const t = childBtn.innerText.trim().toUpperCase();
                                        if (t.includes('BUY IT NOW') || t === 'BUY NOW' || childBtn.innerText.includes('立即购买') || t === 'JETZT KAUFEN') return childBtn;
                                    }
                                    sibling = sibling.nextElementSibling;
                                }
                                // Also check parent's next sibling (if buttons are wrapped in divs)
                                if (addToCart.parentElement) {
                                    let parentSibling = addToCart.parentElement.nextElementSibling;
                                    while (parentSibling) {
                                        if (parentSibling.innerText && (parentSibling.innerText.toUpperCase().includes('BUY IT NOW') || parentSibling.innerText.toUpperCase() === 'BUY NOW' || parentSibling.innerText.includes('立即购买') || parentSibling.innerText.toUpperCase() === 'JETZT KAUFEN')) {
                                            return parentSibling;
                                        }
                                        const childBtn = parentSibling.querySelector('button, a, input[type="submit"], div[role="button"]');
                                        if (childBtn) {
                                            const t = childBtn.innerText.trim().toUpperCase();
                                            if (t.includes('BUY IT NOW') || t === 'BUY NOW' || childBtn.innerText.includes('立即购买') || t === 'JETZT KAUFEN') return childBtn;
                                        }
                                        parentSibling = parentSibling.nextElementSibling;
                                    }
                                }
                            }
                            return null;
                        });
                    }

                    if (buyItNowBtn.asElement()) {
                        console.log('Found "BUY IT NOW" button. Clicking...');
                        await buyItNowBtn.click();
                        checkoutNavigated = true;
                        // Wait for navigation
                        try {
                            await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 });
                        } catch (e) {
                            console.log('Navigation timeout or handled via SPA transition.');
                        }
                    } else {
                        console.log('"BUY IT NOW" not found. Looking for "ADD TO CART"...');

                        const addToCartBtn = await page.evaluateHandle(() => {
                            const buttons = Array.from(document.querySelectorAll('button, a, input[type="submit"], div[role="button"]'));
                            return buttons.find(b => {
                                const t = b.innerText.trim().toUpperCase();
                                return (t.includes('ADD TO CART') || t.includes('ADD TO BAG') || t.includes('加入购物车') || t === 'IN DEN WARENKORB') && b.offsetParent !== null;
                            });
                        });

                        if (addToCartBtn.asElement()) {
                            console.log('Found "ADD TO CART" button. Clicking...');
                            await addToCartBtn.click();

                            // Wait for cart drawer or notification
                            await new Promise(r => setTimeout(r, 3000));

                            // Now look for "CHECK OUT"
                            console.log('Looking for "CHECK OUT" / "结账" button...');
                            const checkoutBtn = await page.evaluateHandle(() => {
                                const buttons = Array.from(document.querySelectorAll('button, a, input[type="submit"], [name="checkout"], div[role="button"]'));
                                return buttons.find(b => {
                                    const text = b.innerText.trim(); // 获取原始文本
                                    const upperText = text.toUpperCase(); // 获取大写文本用于英文检查
                                    // 兼容英文和中文（结账）
                                    return (upperText === 'CHECK OUT' || upperText === 'CHECKOUT' || upperText.includes('PROCEED TO CHECKOUT') || text.includes('结账')) && b.offsetParent !== null;
                                });
                            });

                            if (checkoutBtn.asElement()) {
                                await safeClick(checkoutBtn, "CHECK OUT / 结账");
                                console.log('Found "CHECK OUT" / "结账" button. Clicking...');
                                checkoutNavigated = true;
                                try {
                                    const navResponse = await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 });
                                    // Check for 404 on checkout page after navigation
                                    if (navResponse && navResponse.status() === 404) {
                                        console.warn('⚠️ Checkout page returned 404 status code.');
                                        checkoutAuditNote = '页面404-需人工核验该结算页';
                                    }
                                } catch (e) {
                                    console.log('Navigation timeout or handled via SPA transition.');
                                }
                            } else {
                                console.warn('⚠️ "CHECK OUT" / "结账" button not found after adding to cart.');
                            }
                        } else {
                            console.warn('⚠️ "ADD TO CART" button not found.');
                            checkoutAuditNote = '页面404-需人工审核结算页面';
                        }
                    }

                    // Fallback: Direct navigation if not on checkout page
                    if (!page.url().includes('checkout')) {
                        console.log('Not on checkout page. Attempting direct navigation...');
                        try {
                            const directResponse = await page.goto(url + '/checkout', { waitUntil: 'networkidle2' });
                            // Check for 404 on direct checkout navigation
                            if (directResponse && directResponse.status() === 404) {
                                console.warn('⚠️ Direct checkout navigation returned 404 status code.');
                                checkoutAuditNote = '页面404-需人工核验该结算页';
                            }
                        } catch (e) {
                            console.log('Direct checkout navigation failed.');
                        }
                    }

                    // Additional check for 404 text indicators on checkout page
                    try {
                        const checkoutTitle = await page.title();
                        const checkoutBody = await page.evaluate(() => document.body.innerText.toLowerCase());
                        const isCheckout404 = checkoutTitle.toLowerCase().includes('404') ||
                            checkoutTitle.toLowerCase().includes('not found') ||
                            checkoutBody.includes('404 not found') ||
                            checkoutBody.includes('page not found');
                        if (isCheckout404) {
                            console.warn('⚠️ Checkout page contains 404 text indicators.');
                            checkoutAuditNote = '页面404-需人工核验该结算页';
                        }
                    } catch (e) {
                        console.log('Failed to check checkout page for 404 indicators.');
                    }

                    // 4. Fill Information
                    console.log('Filling Checkout Information...');
                    // Robust wait for checkout form to load (email field is usually first)
                    try {
                        await page.waitForSelector('#email, #checkout_email, input[name="checkout[email]"], input[type="email"]', { timeout: 15000 });
                        console.log('Checkout form detected.');
                    } catch (e) {
                        console.warn('⚠️ Checkout form fields not detected within timeout. Attempting to fill anyway...');
                    }
                    await new Promise(r => setTimeout(r, 1000)); // Small buffer

                    // ===============================================
                    // ⚠️ [新增通用逻辑] 处理下拉框选择
                    // ===============================================
                    const handleDropdown = async (selectors, fieldName, labelTexts, preferredIndex = 1) => {
                        try {
                            // 1. 尝试使用选择器直接查找下拉框元素
                            let dropdownElement = null;
                            for (const selector of selectors) {
                                dropdownElement = await page.$(selector);
                                if (dropdownElement) break;
                            }

                            // 2. 如果没有找到，尝试根据标签文本查找
                            if (!dropdownElement) {
                                const dropdownLabel = await page.evaluateHandle((labelTexts) => {
                                    const labels = Array.from(document.querySelectorAll('label'));
                                    // 查找包含指定文本的 label 或相关元素
                                    return labels.find(l => {
                                        const text = l.innerText.trim();
                                        return labelTexts.some(labelText => text.includes(labelText));
                                    });
                                }, labelTexts);

                                if (dropdownLabel.asElement()) {
                                    console.log(`Found ${fieldName} label/field container. Trying to find the click target.`);
                                    // 尝试点击 label/字段周围的元素来打开下拉列表（通常是 div/span）
                                    const fieldContainer = await dropdownLabel.evaluateHandle(el => el.closest('.field, .form-group, .select-wrapper'));
                                    if (fieldContainer.asElement()) {
                                        dropdownElement = await fieldContainer.asElement().querySelector('input[type="text"][readonly], .select-input, .select-dropdown, button[aria-haspopup="listbox"]');
                                    }
                                }
                            }

                            if (dropdownElement) {
                                const tagName = await page.evaluate(el => el.tagName.toLowerCase(), dropdownElement);

                                if (tagName === 'select') {
                                    // 传统的 <select> 标签
                                    let selectedValue = null;

                                    // 如果是国家/地区字段，优先查找United States(US)
                                    if (['国家/地区', 'Country', 'Country/Region'].includes(fieldName)) {
                                        selectedValue = await page.evaluate((selector) => {
                                            const select = document.querySelector(selector);
                                            if (select) {
                                                // 尝试查找United States(US)选项
                                                const usOption = Array.from(select.options).find(option =>
                                                    option.text.includes('United States') ||
                                                    option.text.includes('US') ||
                                                    option.text.includes('United States(US)') ||
                                                    option.value.includes('US') ||
                                                    option.value.includes('United States') ||
                                                    option.value.includes('United States(US)')
                                                );
                                                if (usOption) {
                                                    return usOption.value;
                                                }
                                            }
                                            return null;
                                        }, selectors[0]);
                                    }
                                    // 如果是州/省字段，优先查找California
                                    else if (['州/省', 'State', 'Province'].includes(fieldName)) {
                                        selectedValue = await page.evaluate((selector) => {
                                            const select = document.querySelector(selector);
                                            if (select) {
                                                // 尝试查找California选项
                                                const caOption = Array.from(select.options).find(option =>
                                                    option.text.includes('California') ||
                                                    option.text.includes('CA') ||
                                                    option.text.includes('California(CA)') ||
                                                    option.value.includes('CA') ||
                                                    option.value.includes('California') ||
                                                    option.value.includes('California(CA)')
                                                );
                                                if (caOption) {
                                                    return caOption.value;
                                                }
                                            }
                                            return null;
                                        }, selectors[0]);
                                    }

                                    // 如果没有找到目标选项或不是特定字段，使用原来的索引选择
                                    if (!selectedValue) {
                                        selectedValue = await page.evaluate((selector, preferredIndex) => {
                                            const select = document.querySelector(selector);
                                            if (select) {
                                                // 根据可用选项数量选择合适的索引
                                                const index = Math.min(preferredIndex, select.options.length - 1);
                                                return select.options[index].value;
                                            }
                                            return null;
                                        }, selectors[0], preferredIndex);
                                    }

                                    if (selectedValue) {
                                        await page.select(selectors[0], selectedValue);
                                        console.log(`✅ 传统${fieldName}下拉框：已选择选项，值: ${selectedValue}`);
                                    } else {
                                        console.warn(`⚠️ 传统${fieldName}下拉框选项不足或无值。`);
                                    }
                                } else {
                                    // 非 <select> 标签（例如自定义下拉框、div 模拟）

                                    // 1. 点击元素打开下拉列表
                                    await safeClick(dropdownElement, `${fieldName} Select`);
                                    await new Promise(r => setTimeout(r, 1500)); // 等待下拉列表打开

                                    // 2. 尝试选择指定索引的列表项
                                    const optionHandle = await page.evaluateHandle((preferredIndex, fieldName) => {
                                        // 查找所有可见的、非禁用的选项
                                        const options = Array.from(document.querySelectorAll('li:not([role="option"][aria-disabled="true"]), [role="option"]:not([aria-disabled="true"]), .select-option:not(.disabled), .dropdown-item:not(.disabled)'))
                                            .filter(o => o.offsetParent !== null && !o.disabled && o.offsetWidth > 0 && o.offsetHeight > 0);

                                        // 如果是国家/地区字段，优先查找United States(US)
                                        if (['国家/地区', 'Country', 'Country/Region'].includes(fieldName)) {
                                            const usOption = options.find(option => {
                                                const text = option.textContent || option.innerText;
                                                return text.includes('United States') || text.includes('US');
                                            });
                                            if (usOption) {
                                                return usOption;
                                            }
                                        }
                                        // 如果是州/省字段，优先查找California
                                        else if (['州/省', 'State', 'Province'].includes(fieldName)) {
                                            const caOption = options.find(option => {
                                                const text = option.textContent || option.innerText;
                                                return text.includes('California') || text.includes('CA');
                                            });
                                            if (caOption) {
                                                return caOption;
                                            }
                                        }

                                        // 未找到目标选项或不是特定字段，使用原来的索引选择
                                        const index = Math.min(preferredIndex, options.length - 1);
                                        return options[index];
                                    }, preferredIndex, fieldName);

                                    if (optionHandle.asElement()) {
                                        await safeClick(optionHandle, `Option for ${fieldName}`);
                                        console.log(`✅ 自定义${fieldName}下拉框：已选择选项。`);
                                    } else {
                                        console.warn(`⚠️ 找不到${fieldName}下拉列表的选项。继续填写其他字段。`);
                                    }
                                }
                            } else {
                                console.warn(`⚠️ 找不到${fieldName}下拉框元素。继续填写其他字段。`);
                            }
                        } catch (e) {
                            console.error(`❌ 处理${fieldName}选择时出错:`, e.message);
                        }
                    };

                    // 处理国家/地区下拉框（默认选择第五个选项）
                    await handleDropdown(
                        ['select[name*="country"], select[id*="country"], select[data-testid*="country"], select[name*="Country"], select[id*="Country"], select[data-testid*="Country"], select[name*="Country/Region"], select[id*="Country/Region"], select[data-testid*="Country/Region"]'],
                        '国家/地区',
                        ['国家/地区', 'Country', 'Country/Region'],
                        3 // 选择第四个选项（index 3）
                    );

                    // 处理州/省下拉框（默认选择第七个选项）
                    await handleDropdown(
                        ['select[name*="state"], select[id*="state"], select[name*="province"], select[id*="province"], select[data-testid*="state"], select[data-testid*="province"]'],
                        '州/省',
                        ['州', '省', 'State', 'Province'],
                        6 // 选择第七个选项（index 6）
                    );
                    // ===============================================

                    const typeField = async (selectors, value) => {
                        for (const selector of selectors) {
                            try {
                                const el = await page.$(selector);
                                if (el) {
                                    await el.evaluate(e => e.scrollIntoView({ block: 'center' }));

                                    // Check if this is an address field
                                    const isAddressField = selectors.some(sel =>
                                        sel.includes('address') || sel.includes('Address')
                                    );

                                    if (isAddressField) {
                                        // Special handling for address fields to avoid clicking dropdown suggestions
                                        await el.click({ clickCount: 3 });
                                        await el.press('Backspace');
                                        await el.type(value, { delay: 100 }); // Slower typing

                                        // Press Escape key to close any dropdown suggestions
                                        await el.press('Escape');
                                        console.log('✅ Address field filled, dropdown suggestions closed with Escape key');
                                    } else {
                                        // Normal field handling
                                        await el.click({ clickCount: 3 });
                                        await el.press('Backspace');
                                        await el.type(value, { delay: 100 }); // Slower typing
                                    }

                                    return true;
                                }
                            } catch (e) { }
                        }
                        return false;
                    };

                    await typeField(['#email', '#checkout_email', 'input[name="checkout[email]"]', 'input[type="email"]', 'input[placeholder="E-Mail-Adresse"]', 'input[placeholder="邮箱地址"]'], '1768558139@gmail.com');
                    await typeField(['#checkout_shipping_address_first_name', 'input[name="checkout[shipping_address][first_name]"]', 'input[placeholder="First name"]', 'input[placeholder="Vorname"]', 'input[placeholder="名"]'], 'Candace');
                    await typeField(['#checkout_shipping_address_last_name', 'input[name="checkout[shipping_address][last_name]"]', 'input[placeholder="Last name"]', 'input[placeholder="Nachname"]', 'input[placeholder="姓"]'], 'Rojas');
                    await typeField(['#checkout_shipping_address_address1', 'input[name="checkout[shipping_address][address1]"]', 'input[placeholder="Address"]', 'input[placeholder="Adresse"]', 'input[placeholder="地址"]', 'input[placeholder="Straße und Hausnummer"]'], '2435 Trade St SE Salem OR');
                    await typeField(['#checkout_shipping_address_city', 'input[name="checkout[shipping_address][city]"]', 'input[placeholder="City"]', 'input[placeholder="Stadt"]', 'input[placeholder="城市"]'], 'USA');
                    await typeField(['#checkout_shipping_address_zip', 'input[name="checkout[shipping_address][zip]"]', 'input[placeholder="ZIP code"]', 'input[placeholder="PLZ"]', 'input[placeholder="邮政编码"]', 'input[placeholder="邮编"]', 'input[placeholder="Postleitzahl"]', 'input[placeholder="Postal code"]', 'input[placeholder="Postcode"]'], '90001');
                    await typeField(['#checkout_shipping_address_phone', 'input[name="checkout[shipping_address][phone]"]', 'input[placeholder="Phone"]', 'input[placeholder="Telefon"]', 'input[placeholder="电话"]'], '15032698227');

                    // Scroll down a bit to ensure "Continue to payment" button is visible
                    await page.evaluate(() => {
                        window.scrollBy(0, 400); // Scroll down 300 pixels
                    });
                    await new Promise(r => setTimeout(r, 500)); // Wait a short moment for scrolling to complete

                    // 5. Click "Continue to payment"
                    const continueBtn = await page.evaluateHandle(() => {
                        const buttons = Array.from(document.querySelectorAll('button, input[type="submit"]'));
                        return buttons.find(b => b.innerText.toUpperCase().includes('CONTINUE') || b.innerText.toUpperCase().includes('PAYMENT') || b.innerText.toUpperCase().includes('SHIPPING') || b.innerText.toUpperCase().includes('WEITER ZUM VERSAND') || b.innerText.toUpperCase().includes('下一步物流'));
                    });

                    if (continueBtn && continueBtn.asElement()) {
                        await safeClick(continueBtn, "Continue");
                        console.log('Clicked Continue. Waiting 2 seconds for Shipping page to load...');
                        await new Promise(r => setTimeout(r, 2000));

                        // ===============================================
                        // 预警机制：检查国家和邮编是否填写错误
                        // ===============================================
                        console.log('🔍 检查国家和邮编填写错误...');

                        const hasCountryZipError = await page.evaluate(() => {
                            // 检查国家相关错误
                            const countryErrorSelectors = [
                                '#checkout_shipping_address_country + .error-message',
                                'select[name*="country"] + .error',
                                'select[name*="country"] ~ .error-message',
                                '.field-country .error',
                                '.field-country .error-message',
                                '.form-group.country .error',
                                '.form-group.country .error-message'
                            ];

                            // 检查邮编相关错误
                            const zipErrorSelectors = [
                                '#checkout_shipping_address_zip + .error-message',
                                'input[name*="zip"] + .error',
                                'input[name*="zip"] ~ .error-message',
                                '.field-zip .error',
                                '.field-zip .error-message',
                                '.form-group.zip .error',
                                '.form-group.zip .error-message',
                                'input[placeholder*="ZIP"] + .error',
                                'input[placeholder*="ZIP"] ~ .error-message',
                                'input[placeholder*="邮编"] + .error',
                                'input[placeholder*="邮编"] ~ .error-message'
                            ];

                            // 查找所有可能的错误元素
                            const allErrorSelectors = [...countryErrorSelectors, ...zipErrorSelectors];
                            let hasError = false;

                            for (const selector of allErrorSelectors) {
                                const errorElements = document.querySelectorAll(selector);
                                for (const element of errorElements) {
                                    if (element.textContent.trim() && element.style.display !== 'none') {
                                        console.log('找到错误提示:', element.textContent.trim());
                                        hasError = true;
                                        break;
                                    }
                                }
                                if (hasError) break;
                            }

                            // 额外检查：查看是否有任何元素显示"Enter a valid ZIP code"或类似错误
                            const allInputs = document.querySelectorAll('input, select');
                            for (const input of allInputs) {
                                const parent = input.closest('.field, .form-group, .form-field');
                                if (parent) {
                                    const errorText = parent.textContent.toLowerCase();
                                    if (errorText.includes('valid zip') || errorText.includes('valid postal') ||
                                        errorText.includes('zip code') && errorText.includes('valid') ||
                                        errorText.includes('country') && (errorText.includes('select') || errorText.includes('choose'))) {
                                        console.log('找到错误文本:', parent.textContent.trim());
                                        hasError = true;
                                        break;
                                    }
                                }
                            }

                            return hasError;
                        });

                        if (hasCountryZipError) {
                            console.warn('⚠️ 检测到国家或邮编填写错误');
                            checkoutAuditNote = '邮编或国家填写错误，需人工核验该结算页';
                        }
                    }

                    // Scroll back up by the same amount to restore view
                    await page.evaluate(() => {
                        window.scrollBy(0, -800); // Scroll up 400 pixels (same as previous scroll down)
                    });
                    await new Promise(r => setTimeout(r, 500)); // Wait a short moment for scrolling to complete

                    // 6. Screenshot
                    console.log('Taking screenshot of checkout/payment page...');
                    const screenshotPath = path.join(process.cwd(), `screenshot_${Date.now()}.png`);
                    let screenshotFailed = false;
                    let uploadFailed = false;

                    try {
                        await page.screenshot({ path: screenshotPath }); // Save to local file
                    } catch (screenshotError) {
                        console.error('❌ Failed to take screenshot:', screenshotError.message);
                        screenshotFailed = true;
                    }

                    if (!screenshotFailed) {
                        console.log('Uploading screenshot to Feishu...');
                        const fileToken = await uploadFileToFeishu(screenshotPath, accessToken, appToken);
                        if (fileToken) {
                            screenshotUrl = fileToken; // Store token instead of URL
                            console.log(`✅ Screenshot uploaded to Feishu. Token: ${fileToken}`);
                        } else {
                            console.error('❌ Failed to get file_token from Feishu upload');
                            uploadFailed = true;
                        }

                        // Clean up local file
                        try { fs.unlinkSync(screenshotPath); } catch (e) { }
                    }

                    // Add warning to audit note if screenshot or upload failed
                    if (screenshotFailed || uploadFailed) {
                        if (!checkoutAuditNote) {
                            checkoutAuditNote = '截图失败-需人工审核该结算页面';
                        } else if (!checkoutAuditNote.includes('需人工审核该结算页面')) {
                            checkoutAuditNote += '截图失败-需人工审核该结算页面';
                        }
                    }

                } catch (checkoutError) {
                    console.error('❌ Error during checkout automation:', checkoutError.message);
                    // Fallback: Take screenshot of where it failed
                    let fallbackScreenshotFailed = false;
                    let fallbackUploadFailed = false;
                    try {
                        console.log('Taking fallback screenshot of error state...');
                        const screenshotPath = path.join(process.cwd(), `screenshot_fallback_${Date.now()}.png`);

                        try {
                            await page.screenshot({ path: screenshotPath });
                        } catch (snapshotError) {
                            console.error('❌ Failed to take fallback screenshot:', snapshotError.message);
                            fallbackScreenshotFailed = true;
                        }

                        if (!fallbackScreenshotFailed) {
                            const fileToken = await uploadFileToFeishu(screenshotPath, accessToken, appToken);
                            if (fileToken) {
                                screenshotUrl = fileToken;
                                console.log(`✅ Fallback screenshot uploaded to Feishu. Token: ${fileToken}`);
                            } else {
                                console.error('❌ Failed to get file_token from fallback screenshot upload');
                                fallbackUploadFailed = true;
                            }
                            // Clean up local file
                            try { fs.unlinkSync(screenshotPath); } catch (e) { }
                        }
                    } catch (fallbackError) {
                        console.error('❌ Error in fallback screenshot process:', fallbackError.message);
                        fallbackScreenshotFailed = true;
                    }

                    // Add warning to audit note if fallback screenshot or upload failed
                    if (fallbackScreenshotFailed || fallbackUploadFailed) {
                        if (!checkoutAuditNote) {
                            checkoutAuditNote = '截图失败-需人工审核该结算页面';
                        } else if (!checkoutAuditNote.includes('需人工审核该结算页面')) {
                            checkoutAuditNote += '截图失败-需人工审核该结算页面';
                        }
                    }
                }

                // ---------------------------------------------------------
                // Step 3: Prepare Batch Records
                // ---------------------------------------------------------
                // ---------------------------------------------------------
                // Step 3: Prepare Batch Records
                // ---------------------------------------------------------

                // Now await the Doubao API result
                console.log('Waiting for Doubao API result...');
                let auditResult;
                try {
                    auditResult = await auditPromise;
                    console.log('Doubao API result received.');
                } catch (apiError) {
                    console.error('❌ Error from Doubao API:', apiError.message);
                    auditResult = "{}"; // Fallback to empty JSON string to avoid crash
                }

                let parsedRecord = parseDoubaoResponse(auditResult);

                // Add operating department information
                parsedRecord.fields[FIELD_OPERATING_DEPARTMENT] = operatingDepartment;

                // Check if all required fields are present in the parsed result
                const requiredFields = [
                    FIELD_CONTENT_MYTH,
                    FIELD_EXCHANGE,
                    FIELD_LOCATION_TIME,
                    FIELD_SERVICE_TIME,
                    FIELD_ORIGIN,
                    FIELD_TAX,
                    FIELD_REFUND
                ];

                const missingFields = requiredFields.filter(field => !parsedRecord.fields[field]);

                // If some fields are missing, retry the API call once
                if (missingFields.length > 0) {
                    console.warn(`⚠️  Missing ${missingFields.length} audit fields: ${missingFields.join(', ')}. Retrying API call...`);

                    try {
                        auditResult = await callDoubaoAPI(cleanText);
                        console.log('Doubao API retry result received.');
                        parsedRecord = parseDoubaoResponse(auditResult);

                        // Add operating department information again after retry
                        parsedRecord.fields[FIELD_OPERATING_DEPARTMENT] = operatingDepartment;

                        // Check again after retry
                        const stillMissingFields = requiredFields.filter(field => !parsedRecord.fields[field]);
                        if (stillMissingFields.length > 0) {
                            console.warn(`⚠️  Still missing ${stillMissingFields.length} audit fields after retry: ${stillMissingFields.join(', ')}.`);
                        } else {
                            console.log('✅ All fields retrieved after API retry.');
                        }
                    } catch (retryError) {
                        console.error('❌ Error from Doubao API retry:', retryError.message);
                    }
                }

                const recordsToSubmit = [];

                // 3a. Main Product Record
                // Remove http:// or https:// prefix from URL
                const cleanUrl = url.replace(/^https?:\/\//, '');
                parsedRecord.fields[URL_FIELD_NAME] = cleanUrl;
                parsedRecord.fields[TEXT_CONTENT_FIELD_NAME] = cleanText.substring(0, 5000);

                // Add Note if checkout issues occurred
                if (checkoutAuditNote) {
                    parsedRecord.fields[FIELD_NOTE] = checkoutAuditNote;
                }

                // Add Screenshot File Token if available (Attachment Field)
                if (screenshotUrl) {
                    console.log(`Attaching screenshot token: ${screenshotUrl}`);
                    // Feishu Attachment field expects an array of objects with file_token
                    parsedRecord.fields[SCREENSHOT_FIELD_NAME] = [{ file_token: screenshotUrl }];
                } else {
                    console.warn('⚠️ No screenshot URL/Token available for this record.');
                }

                // 3b. Add all image file tokens to the main record's attachment field
                if (uploadedImages.length > 0) {
                    console.log(`Attaching ${uploadedImages.length} image tokens to the main record`);
                    // Feishu Attachment field expects an array of objects with file_token
                    parsedRecord.fields[IMAGE_URL_FIELD_NAME] = uploadedImages.map(img => ({
                        file_token: img.fileToken
                    }));
                } else {
                    console.warn('⚠️ No image tokens available for this record.');
                }

                recordsToSubmit.push(parsedRecord);
                
                // Add to all processed records for local logging
                allProcessedRecords.push({
                    url: url,
                    timestamp: new Date().toISOString(),
                    auditNote: checkoutAuditNote,
                    hasScreenshot: !!screenshotUrl,
                    uploadedImagesCount: uploadedImages.length,
                    recordData: parsedRecord
                });
                
                // Write to TXT log immediately
                const txtLogEntry = `[${new Date().toLocaleString()}] URL: ${url}\n` +
// TXT日志已通过console重定向自动记录，无需额外处理
                
                // Save logs incrementally after each record
                console.log('\n💾 Saving incremental logs...');
                saveLogsToLocal();

                // Clean up local cached images
                console.log(`\n🧹 Cleaning up local image cache...`);
                for (const img of downloadedImages) {
                    try {
                        fs.unlinkSync(img.path);
                        console.log(`✅ Removed cached image: ${path.basename(img.path)}`);
                    } catch (e) {
                        console.warn(`⚠️ Failed to remove cached image: ${img.path}`);
                    }
                }

                // Also clean up the temporary images directory if it's empty
                try {
                    const imagesDir = path.join(process.cwd(), 'temp_images');
                    const files = fs.readdirSync(imagesDir);
                    if (files.length === 0) {
                        fs.rmdirSync(imagesDir);
                        console.log(`✅ Removed empty cache directory: ${imagesDir}`);
                    }
                } catch (e) {
                    console.warn(`⚠️ Failed to clean up cache directory: ${e.message}`);
                }

                console.log('--------------------------------------------------');
                console.log(`Prepared ${recordsToSubmit.length} records for batch submission.`);
                // Log the first record (Main) and first image record for debugging
                console.log('Main Record Sample:', JSON.stringify(recordsToSubmit[0], null, 2));
                if (recordsToSubmit.length > 1) {
                    console.log('Image Record Sample:', JSON.stringify(recordsToSubmit[1], null, 2));
                }
                console.log('--------------------------------------------------');

                console.log('Writing Batch Records to Feishu (Chunked)...');

                // 批量写入保护：分批写入，避免超过飞书 API 单次记录限制
                const BATCH_SIZE = 50;
                for (let i = 0; i < recordsToSubmit.length; i += BATCH_SIZE) {
                    const chunk = recordsToSubmit.slice(i, i + BATCH_SIZE);
                    console.log(`Submitting chunk ${Math.floor(i / BATCH_SIZE) + 1}: ${chunk.length} records`);
                    await addBatchRecordsToBitable(accessToken, appToken, tableId, chunk);

                    // 避免请求过于频繁
                    if (i + BATCH_SIZE < recordsToSubmit.length) {
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }

            } catch (err) {
                console.error(`Error processing ${url}:`, err.message);
            }
        }

        await browser.close();
        console.log('\nDone!');
    } catch (error) {
        console.error('[终止原因: 未捕获的全局异常]', error);
        console.error('中文错误标签: 程序执行过程中遇到未捕获的致命异常');
        console.log(`处理记录数量: ${allProcessedRecords.length}`);
        saveLogsToLocal();
        process.exit(1);
    } finally {
        // Save logs when program ends normally
        saveLogsToLocal();
        rl.close();
    }

    // Function to save logs to local file
    function saveLogsToLocal() {
        if (allProcessedRecords.length === 0) {
            console.log('\nNo records to save to logs.');
            return;
        }

        console.log('\n💾 Saving logs to local file...');
        
        try {
            // Create a timestamp for log file name
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const logFileName = `audit_log_${timestamp}.json`;
            const logFilePath = path.join(LOG_DIR, logFileName);
            
            // Prepare log data
            const logData = {
                timestamp: new Date().toISOString(),
                totalRecords: allProcessedRecords.length,
                records: allProcessedRecords
            };
            
            // Write logs to file
            fs.writeFileSync(logFilePath, JSON.stringify(logData, null, 2), 'utf8');
            console.log(`✅ Logs saved to: ${logFilePath}`);
        } catch (error) {
            console.error(`❌ Error saving logs: ${error.message}`);
            console.error('Error details:', error.stack);
            
            // Try a simpler log format as fallback
            try {
                const simpleLogFileName = `simple_log_${Date.now()}.txt`;
                const simpleLogFilePath = path.join(LOG_DIR, simpleLogFileName);
                const simpleLogContent = `${new Date().toISOString()}\nProcessed ${allProcessedRecords.length} records.\n\n${allProcessedRecords.map(r => `URL: ${r.url}\nNote: ${r.auditNote || 'None'}\nScreenshot: ${r.hasScreenshot ? 'Yes' : 'No'}\nImages: ${r.uploadedImagesCount}\n---`).join('\n')}`;
                fs.writeFileSync(simpleLogFilePath, simpleLogContent, 'utf8');
                console.log(`✅ Simple logs saved to: ${simpleLogFilePath}`);
            } catch (fallbackError) {
                console.error(`❌ Failed to save simple logs: ${fallbackError.message}`);
            }
        }
    }

    // Handle termination signals to save logs before exiting
    function setupTerminationHandlers() {
        // Handle Ctrl+C on Windows
        process.on('SIGINT', () => {
            console.log('\n📤 [终止原因: 键盘中断(Ctrl+C)] 收到中断信号，正在保存日志...');
            
            saveLogsToLocal();
            // Use process.exitCode instead of process.exit() to allow cleanup
            process.exitCode = 0; 
        });
        
        // Windows-specific exit event
        if (process.platform === 'win32') {
            const rl = require('readline').createInterface({
                input: process.stdin,
                output: process.stdout
            });
            
            rl.on('SIGINT', () => {
                process.emit('SIGINT');
            });
        }
        
        // Handle normal exit
        process.on('beforeExit', () => {
            console.log('\n📤 [终止原因: 程序正常运行结束] 正在保存日志...');
            console.log(`本次运行共处理 ${allProcessedRecords.length} 条记录`);
            
            saveLogsToLocal();
        });
        
        // Handle uncaught exceptions
        process.on('uncaughtException', (error) => {
            console.error('\n❌ [终止原因: 未捕获异常] 程序遇到致命错误，正在保存日志...');
            console.error('错误详情:', error);
            console.error('错误堆栈:', error.stack);
            console.log(`本次运行共处理 ${allProcessedRecords.length} 条记录`);
            
            saveLogsToLocal();
            process.exit(1);
        });
        
        // Handle unhandled promise rejections
        process.on('unhandledRejection', (reason, promise) => {
            console.error('\n❌ [终止原因: 未处理的Promise拒绝] 程序遇到未处理的异步错误，正在保存日志...');
            console.error('拒绝原因:', reason);
            console.error('相关Promise:', promise);
            console.log(`本次运行共处理 ${allProcessedRecords.length} 条记录`);
            
            saveLogsToLocal();
            process.exit(1);
        });
    }

    // Initialize termination handlers
    setupTerminationHandlers();
})();