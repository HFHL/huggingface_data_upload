import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Microsoft Azure应用程序凭据
CLIENT_ID = os.getenv("CLIENT_ID")  # 应用程序(客户端)ID
TENANT_ID = os.getenv("TENANT_ID")  # 租户ID

# Microsoft Graph API端点
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["Files.Read", "Files.Read.All"]  # 所需的权限范围

# 本地设置
DOWNLOAD_PATH = "downloads"  # 下载文件的本地目录
TOKEN_CACHE_FILE = "token_cache.json"  # 令牌缓存文件 