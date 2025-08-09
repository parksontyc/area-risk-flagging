import os
import re
import time
import datetime
from typing import  List, Set, Tuple, Dict
import ast


from tqdm import tqdm 
import pandas as pd
import requests
import numpy as np
import os
import math

# 預售屋查詢 - 網址組合function
def build_complete_urls(base_url, url_fragments):
    """將base URL與fragments組合成完整URL字典
    
    Args:
        base_url (str): 基礎URL
        url_fragments (dict): URL片段字典，key為城市名稱，value為URL片段
        
    Returns:
        dict: 完整URL字典, key為城市名稱, value為完整URL
    """
    return {city: base_url + fragment for city, fragment in url_fragments.items()}

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # 若有錯誤狀況，會引發例外
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"取得資料時發生錯誤：{e}")
        return pd.DataFrame()  # 回傳空的 DataFrame
    
# 取出建商
def extract_company_name(row):
    """
    比對id與idlist中的ID，提取匹配的公司名稱
    """
    target_id = row['編號']
    idlist = row['編號列表']
    
    # 如果idlist是字串格式的列表，需要先轉換
    if isinstance(idlist, str):
        try:
            # 嘗試用ast.literal_eval轉換字串格式的列表
            idlist = ast.literal_eval(idlist)
        except:
            # 如果轉換失敗，返回None
            return None
    
    # 遍歷idlist中的每個項目
    for item in idlist:
        # 按逗號分割字串
        parts = item.split(',')
        if len(parts) >= 3:  # 確保有足夠的部分
            item_id = parts[0].strip()  # ID部分
            company_name = parts[2].strip()  # 公司名稱部分
            
            # 比對ID
            if item_id == target_id:
                return company_name
    
    # 如果沒有找到匹配的ID，返回None
    return None


# 合併dataframe
def combined_df(url, input_time):
    # 建立一個空的列表存放各區的 DataFrame
    df_list = []
    city_counts = {}  # 用於記錄每個縣市的資料筆數
    
    print("開始處理各縣市資料：")
    
    # 迴圈走訪所有 URL，並新增表示地區和輸入時間的欄位
    for city_name, uni_url in url.items():
        print(f"處理 {city_name} 中...", end="", flush=True)
        
        df_temp = fetch_data(uni_url)
        if not df_temp.empty:
            df_temp["city_name"] = city_name      # 加入來源區域欄位，便於後續分析
            df_temp["input_time"] = input_time    # 加入從變數名稱提取的時間

            # 立刻重排欄位，讓 city_name 成為第一欄
            cols = df_temp.columns.tolist()
            cols = ["city_name"] + [c for c in cols if c != "city_name"]
            df_temp = df_temp[cols]
            
            # 記錄此縣市的資料筆數
            row_count = len(df_temp)
            city_counts[city_name] = row_count
            # 直接打印當前縣市的資料筆數
            print(f" 完成! 找到 {row_count} 筆資料")
        else:
            print(" 完成! 找到 0 筆資料")
            
        df_list.append(df_temp)
        time.sleep(1)  # 每次發出請求後暫停 1 秒

    # 利用 pd.concat 合併所有 DataFrame（重置索引）
    combined_df = pd.concat(df_list, ignore_index=True)
    
    # # 顯示各縣市資料筆數統計
    # print("\n各縣市資料筆數統計:")
    # for city, count in city_counts.items():
    #     print(f"{city}: {count} 筆")
    
    # 顯示合併後的總筆數
    total_rows = len(combined_df)
    print(f"\n合併後資料總筆數: {total_rows} 筆")
    
    return combined_df


# 由坐落地址欄位折分出行政區
def parse_admin_region(address):
    # 若不是字串或字串長度為0，直接回傳 None 或空值
    if not isinstance(address, str) or not address:
        return None
    
    # 判斷第二個字是否為「區」
    # 注意：Python 字串的索引從 0 開始
    if len(address) >= 2 and address[1] == "區":
        return address[:2]
    # 判斷第三個字是否為「區」
    elif len(address) >= 3 and address[2] == "區":
        return address[:3]
    # 其餘情況取前三個字
    elif len(address) >= 3:
        return address[:3]
    else:
        # 若字串不足三個字，就直接回傳原字串
        return address
    

# 定義一個函式來解析銷售期間，回傳 (自售期間, 代銷期間)
def parse_sale_period(s: str) -> Tuple[str, str]:
    """
    解析「銷售期間」字串，回傳 (自售時段, 代銷時段)。

    規則：
      1. 支援分隔符號：逗號(,), 頓號(，), 分號(;)。
      2. 若段落中含「自售」關鍵字，視為自售時段；含「代銷」關鍵字，視為代銷時段。
      3. 若無明確關鍵字，則第一段給自售、第二段給代銷。
      4. 「無」或空字串皆回傳空字串。
      5. 保留原始格式，不做日期驗證。

    範例：
      >>> parse_sale_period("自售:1100701~自完售止;代銷:1100801~售完為止")
      ("1100701~自完售止", "1100801~售完為止")
      >>> parse_sale_period("1100701~1110101,1120201~1120501")
      ("1100701~1110101", "1120201~1120501")
      >>> parse_sale_period("無")
      ("", "")
    """
    # 空值或「無」直接回空字串
    if not isinstance(s, str) or s.strip() in {"", "無"}:
        return "", ""

    # 統一分隔符，並切段
    normalized = re.sub(r"[；;，]", ",", s.strip())
    parts = [p.strip() for p in normalized.split(",") if p.strip()]

    self_period = ""
    agent_period = ""

    for part in parts:
        low = part.lower()
        # 明確標示「自售」
        if "自售" in low:
            # 去掉標籤，保留後面所有
            self_period = re.sub(r"(?i).*?自售[:：]?", "", part).strip()
        # 明確標示「代銷」
        elif "代銷" in low:
            agent_period = re.sub(r"(?i).*?代銷[:：]?", "", part).strip()
        else:
            # 無標籤，依序填入
            if not self_period:
                self_period = part
            elif not agent_period:
                agent_period = part
    return self_period, agent_period


# 定義函式：尋找自售期間及代銷期間的起始日，若沒有則回傳 None
def find_first_sale_time(text):
    """
    尋找自售期間及代銷期間的起始日，支援多種日期格式
    
    支援格式：
    - 1110701 (7位數字)
    - 111年07月01日 / 111年7月1日 / 111年8月1號
    - 111/07/01 / 111/7/1
    
    Returns:
        str: 標準化的7位數字日期格式 (如: '1110701')，若沒有找到則回傳 None
    """
    if not isinstance(text, str):
        return None
    
    # 1. 先檢查是否已經是7位數字格式
    seven_digit_match = re.search(r"\d{7}", text)
    if seven_digit_match:
        return seven_digit_match.group(0)
    
    # 2. 檢查 "111年07月01日"、"111年7月1日" 或 "111年8月1號" 格式
    year_month_day_match = re.search(r"(\d{3})年(\d{1,2})月(\d{1,2})[日號]", text)
    if year_month_day_match:
        year = year_month_day_match.group(1)
        month = year_month_day_match.group(2).zfill(2)  # 補零到2位
        day = year_month_day_match.group(3).zfill(2)    # 補零到2位
        return f"{year}{month}{day}"
    
    # 3. 檢查 "111/07/01" 或 "111/7/1" 格式
    slash_format_match = re.search(r"(\d{3})/(\d{1,2})/(\d{1,2})", text)
    if slash_format_match:
        year = slash_format_match.group(1)
        month = slash_format_match.group(2).zfill(2)    # 補零到2位
        day = slash_format_match.group(3).zfill(2)      # 補零到2位
        return f"{year}{month}{day}"
    
    # 4. 如果都沒有匹配，回傳 None
    return None


# 定義函式：依據規則決定建案的「銷售起始時間」
def sales_start_time(row) -> str:
    """
    根據以下規則決定「銷售起始時間」：
    
    1. 如果只有自售或只有代銷有值，就取有值的那個。
    2. 如果兩者皆有值，轉為整數後比較，取最小者（再轉回字串）。
    3. 如果自售和代銷都空：
       3.1. 若備查完成日期有值，就回傳備查完成日期；
       3.2. 否則回傳建照核發日。
    4. 其他情況（如格式錯誤等）回傳空字串。
    """
    self_time  = row.get("自售起始時間")
    agent_time = row.get("代銷起始時間")
    check_date = row.get("備查完成日期")
    permit_date= row.get("建照核發日")

    # 1. 只有一方有值
    if pd.isna(self_time) and pd.notna(agent_time):
        return str(agent_time).strip()
    if pd.isna(agent_time) and pd.notna(self_time):
        return str(self_time).strip()

    # 2. 兩者皆有值，取較早(數值最小)
    if pd.notna(self_time) and pd.notna(agent_time):
        try:
            self_val  = int(str(self_time).strip())
            agent_val = int(str(agent_time).strip())
            return str(min(self_val, agent_val))
        except ValueError:
            # 若格式無法轉整數，回空
            return ""

    # 3. 自售與代銷皆空
    if pd.isna(self_time) and pd.isna(agent_time):
        # 3.1 備查完成日期優先
        if pd.notna(check_date):
            return str(check_date).strip()
        # 3.2 否則建照核發日
        if pd.notna(permit_date):
            return str(permit_date).strip()
        # 都無值
        return ""

    # 4. 其他狀況
    return ""
    

def to_year_quarter(ts) -> str:
    if pd.isna(ts):
        return ""
    s = str(ts).strip()
    if len(s) < 5:
        return ""
    year = s[:3]
    try:
        month = int(s[3:5])
    except ValueError:
        return ""
    quarter = (month - 1) // 3 + 1
    if quarter < 1 or quarter > 4:
        return ""
    return f"{year}Y{quarter}S"


# 將原始csv檔拆成小檔案
def sample_csv_to_target_size(
    input_path,
    output_path="sample_10mb.csv",
    target_mb=10,
    encoding="utf-8-sig",
    random_state=42
):
    """
    從大型 CSV 中抽樣，產出接近指定 MB 大小的 CSV 檔案。
    
    Parameters:
    - input_path: 原始 CSV 路徑
    - output_path: 抽樣後儲存的 CSV 路徑
    - target_mb: 目標大小（以 MB 為單位，預設 10MB）
    - encoding: 輸出檔案的編碼（預設為 utf-8-sig）
    - random_state: 抽樣亂數種子（確保可重現）
    """
    # 讀取原始 CSV
    print(f" 讀取檔案中：{input_path}")
    df = pd.read_csv(input_path)

    # 計算平均每列大小
    avg_row_size = df.memory_usage(deep=True).sum() / len(df)
    target_bytes = target_mb * 1024 * 1024
    target_rows = int(target_bytes / avg_row_size)

    print(f" 平均每列大小：約 {avg_row_size:.2f} bytes")
    print(f" 目標大小：{target_mb}MB ≈ {target_rows} 筆資料")

    # 隨機抽樣
    sampled_df = df.sample(n=target_rows, random_state=random_state)

    # 儲存結果
    sampled_df.to_csv(output_path, index=False, encoding=encoding)
    actual_size = os.path.getsize(output_path) / (1024 * 1024)

    print(f" 已儲存檔案：{output_path}，實際大小約 {actual_size:.2f} MB")
    return sampled_df





# 型態轉成datetime64
def convert_mixed_date_columns(df, roc_cols=[], ad_cols=[], roc_slash_cols=[]):
    def parse_roc_integer(val):
        if pd.isna(val): return pd.NaT
        try:
            val = str(int(val)).zfill(7)
            y, m, d = int(val[:3]) + 1911, int(val[3:5]), int(val[5:7])
            return pd.Timestamp(f"{y}-{m:02d}-{d:02d}")
        except: return pd.NaT

    def parse_ad_integer(val):
        if pd.isna(val): return pd.NaT
        try:
            val = str(int(val)).zfill(8)
            return pd.to_datetime(val, format="%Y%m%d", errors='coerce')
        except: return pd.NaT

    def parse_roc_slash(val):
        if pd.isna(val): return pd.NaT
        try:
            parts = str(val).split('/')
            y, m, d = int(parts[0]) + 1911, int(parts[1]), int(parts[2])
            return pd.Timestamp(f"{y}-{m:02d}-{d:02d}")
        except: return pd.NaT

    for col in roc_cols:
        df[col] = df[col].apply(parse_roc_integer)

    for col in ad_cols:
        df[col] = df[col].apply(parse_ad_integer)

    for col in roc_slash_cols:
        df[col] = df[col].apply(parse_roc_slash)

    return df


# community_df取出編號列表中所有的備查編號
def extract_mixed_alphanumeric_ids(text):
    if pd.isna(text):
        return ''
    ids = re.findall(r'\b[A-Z0-9]{10,16}\b', text)
    return ', '.join(ids)


def parse_id_string(text: str) -> List[str]:
    """
    解析備查編號清單字串，返回清理後的編號列表
    
    Parameters:
    -----------
    text : str
        包含多個備查編號的字串，以逗號分隔
        
    Returns:
    --------
    List[str]
        清理後的備查編號列表
    """
    if pd.isna(text) or not isinstance(text, str) or text.strip() == '':
        return []
    
    return [s.strip().strip("'\"") for s in text.split(',') if s.strip()]


def create_transaction_lookup_structures(transaction_df: pd.DataFrame) -> Tuple[Dict[str, int], pd.Series]:
    """
    建立交易資料的查詢結構，優先基於備查編號統計
    
    Parameters:
    -----------
    transaction_df : pd.DataFrame
        交易資料，需包含備查編號、縣市、行政區、社區名稱欄位
        
    Returns:
    --------
    Tuple[Dict[str, int], pd.Series]
        (備查編號對應的交易筆數, composite_key對應的交易筆數)
    """
    # 第一優先：基於備查編號統計交易筆數
    id_transaction_counts = transaction_df['備查編號'].value_counts().to_dict()
    
    # 第二備援：基於複合鍵統計交易筆數（用於沒有備查編號匹配的情況）
    composite_keys = (
        transaction_df[['縣市', '行政區', '社區名稱']]
        .fillna('')
        .astype(str)
        .agg('|'.join, axis=1)
    )
    
    # 計算每個 composite key 的交易筆數
    composite_key_counts = composite_keys.value_counts()
    
    return id_transaction_counts, composite_key_counts


def count_transactions_for_community(
    row: pd.Series, 
    id_transaction_counts: Dict[str, int],
    composite_key_counts: pd.Series
) -> int:
    """
    計算單一社區的預售交易筆數
    
    Parameters:
    -----------
    row : pd.Series
        社區資料的一列
    id_transaction_counts : Dict[str, int]
        備查編號對應的交易筆數
    composite_key_counts : pd.Series
        composite key 對應的交易筆數
        
    Returns:
    --------
    int
        該社區的預售交易筆數
    """
    id_list = parse_id_string(row.get('備查編號清單', ''))
    
    # 方法1：優先使用備查編號統計
    if id_list:
        # 檢查是否有備查編號能在統計中找到
        found_ids = [id for id in id_list if id in id_transaction_counts]
        
        if found_ids:
            # 基於備查編號統計，累加所有匹配的備查編號交易筆數
            return sum(id_transaction_counts[id] for id in found_ids)
    
    # 方法2：備查編號無法匹配時，使用 composite key
    composite_key = f"{row.get('縣市', '')}|{row.get('行政區', '')}|{row.get('社區名稱', '')}"
    return composite_key_counts.get(composite_key, 0)


def calculate_presale_transaction_counts(community_df: pd.DataFrame, transaction_df: pd.DataFrame) -> pd.Series:
    """
    計算社區的預售交易筆數
    
    Parameters:
    -----------
    community_df : pd.DataFrame
        包含備查編號清單的社區資料，需包含以下欄位：
        - 備查編號清單: 逗號分隔的備查編號字串
        - 縣市: 縣市名稱
        - 行政區: 行政區名稱  
        - 社區名稱: 社區名稱
        
    transaction_df : pd.DataFrame  
        交易資料，需包含以下欄位：
        - 備查編號: 備查編號
        - 縣市: 縣市名稱
        - 行政區: 行政區名稱
        - 社區名稱: 社區名稱
        
    Returns:
    --------
    pd.Series
        每個社區對應的預售交易筆數，索引與 community_df 相同
        
    Raises:
    -------
    ValueError
        當必要欄位缺失時
    """
    # 檢查必要欄位
    required_community_cols = ['備查編號清單', '縣市', '行政區', '社區名稱']
    required_transaction_cols = ['備查編號', '縣市', '行政區', '社區名稱']
    
    missing_community_cols = [col for col in required_community_cols if col not in community_df.columns]
    missing_transaction_cols = [col for col in required_transaction_cols if col not in transaction_df.columns]
    
    if missing_community_cols:
        raise ValueError(f"community_df 缺少必要欄位: {missing_community_cols}")
    if missing_transaction_cols:
        raise ValueError(f"transaction_df 缺少必要欄位: {missing_transaction_cols}")
    
    # 建立查詢結構
    id_transaction_counts, composite_key_counts = create_transaction_lookup_structures(transaction_df)
    
    # 計算每個社區的交易筆數
    transaction_counts = community_df.apply(
        lambda row: count_transactions_for_community(
            row, id_transaction_counts, composite_key_counts
        ),
        axis=1
    )
    
    return transaction_counts


def calculate_cancellation_counts(community_df: pd.DataFrame, transaction_df: pd.DataFrame) -> pd.Series:
    """
    計算社區的解約次數
    
    Parameters:
    -----------
    community_df : pd.DataFrame
        包含備查編號清單的社區資料，需包含以下欄位：
        - 備查編號清單: 逗號分隔的備查編號字串
        - 縣市: 縣市名稱
        - 行政區: 行政區名稱  
        - 社區名稱: 社區名稱
        
    transaction_df : pd.DataFrame  
        交易資料，需包含以下欄位：
        - 備查編號: 備查編號
        - 縣市: 縣市名稱
        - 行政區: 行政區名稱
        - 社區名稱: 社區名稱
        - 解約情形: 解約狀態資訊
        
    Returns:
    --------
    pd.Series
        每個社區對應的解約次數，索引與 community_df 相同
        
    Raises:
    -------
    ValueError
        當必要欄位缺失時
    """
    # 檢查必要欄位
    required_community_cols = ['備查編號清單', '縣市', '行政區', '社區名稱']
    required_transaction_cols = ['備查編號', '縣市', '行政區', '社區名稱', '解約情形']
    
    missing_community_cols = [col for col in required_community_cols if col not in community_df.columns]
    missing_transaction_cols = [col for col in required_transaction_cols if col not in transaction_df.columns]
    
    if missing_community_cols:
        raise ValueError(f"community_df 缺少必要欄位: {missing_community_cols}")
    if missing_transaction_cols:
        raise ValueError(f"transaction_df 缺少必要欄位: {missing_transaction_cols}")
    
    # 建立解約查詢結構
    def create_cancellation_lookup_structures(transaction_df):
        # 第一優先：基於備查編號統計解約筆數
        cancellation_df = transaction_df[transaction_df['解約情形'].notna()]
        id_cancellation_counts = cancellation_df['備查編號'].value_counts().to_dict()
        
        # 第二備援：基於複合鍵統計解約筆數
        composite_keys = (
            cancellation_df[['縣市', '行政區', '社區名稱']]
            .fillna('')
            .astype(str)
            .agg('|'.join, axis=1)
        )
        composite_cancellation_counts = composite_keys.value_counts()
        
        return id_cancellation_counts, composite_cancellation_counts
    
    def count_cancellations_for_community(row, id_cancellation_counts, composite_cancellation_counts):
        id_list = parse_id_string(row.get('備查編號清單', ''))
        
        # 方法1：優先使用備查編號統計
        if id_list:
            found_ids = [id for id in id_list if id in id_cancellation_counts]
            if found_ids:
                return sum(id_cancellation_counts[id] for id in found_ids)
        
        # 方法2：備查編號無法匹配時，使用 composite key
        composite_key = f"{row.get('縣市', '')}|{row.get('行政區', '')}|{row.get('社區名稱', '')}"
        return composite_cancellation_counts.get(composite_key, 0)
    
    # 建立查詢結構
    id_cancellation_counts, composite_cancellation_counts = create_cancellation_lookup_structures(transaction_df)
    
    # 計算每個社區的解約次數
    cancellation_counts = community_df.apply(
        lambda row: count_cancellations_for_community(
            row, id_cancellation_counts, composite_cancellation_counts
        ),
        axis=1
    )
    
    return cancellation_counts


def create_fast_transaction_lookup(transaction_df: pd.DataFrame) -> tuple:
    """
    建立快速查詢結構，避免重複過濾和查詢
    
    Returns:
    --------
    tuple: (id_first_dates, composite_first_dates)
        - id_first_dates: 備查編號對應的最初交易日期字典
        - composite_first_dates: 複合鍵對應的最初交易日期字典
    """
    print("🔧 建立快速查詢結構...")
    
    # 一次性過濾正常交易（非解約）
    normal_transactions = transaction_df[transaction_df['解約情形'].isna()].copy()
    print(f"   正常交易筆數：{len(normal_transactions):,}")
    
    # 方法1：基於備查編號的最初交易日期查詢表
    id_first_dates = {}
    if '備查編號' in normal_transactions.columns:
        # 移除空值的備查編號
        valid_id_transactions = normal_transactions.dropna(subset=['備查編號'])
        if not valid_id_transactions.empty:
            # 使用 groupby 一次性計算所有備查編號的最初日期
            id_groups = valid_id_transactions.groupby('備查編號')['交易日期'].min()
            id_first_dates = id_groups.to_dict()
    
    print(f"   備查編號查詢表：{len(id_first_dates):,} 筆")
    
    # 方法2：基於複合鍵的最初交易日期查詢表
    composite_first_dates = {}
    if not normal_transactions.empty:
        # 建立複合鍵
        normal_transactions['composite_key'] = (
            normal_transactions['縣市'].fillna('') + '|' +
            normal_transactions['行政區'].fillna('') + '|' +
            normal_transactions['社區名稱'].fillna('')
        )
        
        # 使用 groupby 一次性計算所有複合鍵的最初日期
        composite_groups = normal_transactions.groupby('composite_key')['交易日期'].min()
        composite_first_dates = composite_groups.to_dict()
    
    print(f"   複合鍵查詢表：{len(composite_first_dates):,} 筆")
    print("✅ 查詢結構建立完成")
    
    return id_first_dates, composite_first_dates


def find_first_transaction_date_fast(
    row: pd.Series, 
    id_first_dates: dict,
    composite_first_dates: dict
) -> pd.Timestamp:
    """
    使用預建查詢表快速找出社區最初交易日期
    
    Parameters:
    -----------
    row : pd.Series
        社區資料的一列
    id_first_dates : dict
        備查編號對應最初交易日期的字典
    composite_first_dates : dict
        複合鍵對應最初交易日期的字典
        
    Returns:
    --------
    pd.Timestamp or pd.NaT
        該社區的最初交易日期
    """
    # 方法1：優先使用備查編號匹配
    id_list = parse_id_string(row.get('備查編號清單', ''))
    
    if id_list:
        # 找出所有匹配的備查編號對應的交易日期
        matched_dates = []
        for backup_id in id_list:
            if backup_id in id_first_dates:
                matched_dates.append(id_first_dates[backup_id])
        
        if matched_dates:
            return min(matched_dates)  # 返回最早的日期
    
    # 方法2：使用複合鍵匹配
    composite_key = f"{row.get('縣市', '')}|{row.get('行政區', '')}|{row.get('社區名稱', '')}"
    
    if composite_key in composite_first_dates:
        return composite_first_dates[composite_key]
    
    # 如果都找不到，返回 NaT
    return pd.NaT


def calculate_first_transaction_dates_fast(
    community_df: pd.DataFrame, 
    transaction_df: pd.DataFrame
) -> pd.Series:
    """
    高效能版本：計算所有社區的最初交易日期
    
    Parameters:
    -----------
    community_df : pd.DataFrame
        社區資料
    transaction_df : pd.DataFrame
        交易資料
        
    Returns:
    --------
    pd.Series
        每個社區對應的最初交易日期
    """
    start_time = time.time()
    
    # 檢查必要欄位
    required_community_cols = ['備查編號清單', '縣市', '行政區', '社區名稱']
    required_transaction_cols = ['備查編號', '交易日期', '解約情形']
    
    missing_community_cols = [col for col in required_community_cols if col not in community_df.columns]
    missing_transaction_cols = [col for col in required_transaction_cols if col not in transaction_df.columns]
    
    if missing_community_cols:
        raise ValueError(f"community_df 缺少必要欄位: {missing_community_cols}")
    if missing_transaction_cols:
        raise ValueError(f"transaction_df 缺少必要欄位: {missing_transaction_cols}")
    
    # 建立快速查詢結構
    id_first_dates, composite_first_dates = create_fast_transaction_lookup(transaction_df)
    
    # 使用向量化操作計算最初交易日期
    print("🚀 開始計算最初交易日期...")
    
    # 顯示進度條
    tqdm.pandas(desc="計算進度")
    
    first_dates = community_df.progress_apply(
        lambda row: find_first_transaction_date_fast(row, id_first_dates, composite_first_dates),
        axis=1
    )
    
    elapsed_time = time.time() - start_time
    print(f"⚡ 計算完成！耗時：{elapsed_time:.2f} 秒")
    
    return first_dates


def correct_sales_start_date(df, sales_col='銷售起始時間', first_tx_col='最初交易日期'):
    """
    修正銷售起始時間 > 最初交易日期的情況，僅當兩者皆非 NaT 時進行替換。

    參數:
        df (pd.DataFrame): 含有日期欄位的 DataFrame。
        sales_col (str): 銷售起始時間欄位名稱，預設為 '銷售起始時間'。
        first_tx_col (str): 最初交易日期欄位名稱，預設為 '最初交易日期'。

    回傳:
        修正後的 DataFrame。
    """
    # 建立條件
    mask = (
        df[sales_col].notna() &
        df[first_tx_col].notna() &
        (df[sales_col] > df[first_tx_col])
    )

    # 顯示修正筆數
    print(f"🔁 共修正 {mask.sum()} 筆銷售起始時間")

    # 執行替代
    df.loc[mask, sales_col] = df.loc[mask, first_tx_col]
    
    return df


# 處理重複社區
def process_duplicate_communities(df):
    """
    處理重複社區的主函數 v2.1
    
    識別條件：行政區相同 + 建照執照相同 + 經度相同
    處理規則：
    1. 春豆子案例：戶數相加
    2. 一般重複：取銷售起始時間較晚的記錄
    
    關連編號邏輯 (v2.1修正版)：
    - 社區有效性欄位：1(有效)/0(無效)
    - 關連編號欄位：有效社區=被合併的無效社區編號列表 / 無效社區=空值
    
    返回：
    - processed_df: 處理後的DataFrame（包含新增欄位）
    - report: 處理報告（包含關連編號完整性檢查）
    """
    
    # 創建工作副本
    df_processed = df.copy()
    
    # 新增社區有效性欄位和關連編號欄位（預設為有效）
    df_processed['社區有效性'] = 1
    df_processed['關連編號'] = None
    
    # 步驟1：識別重複社區群組
    print("🔍 識別重複社區群組...")
    duplicate_groups = identify_duplicate_groups(df_processed)
    
    # 步驟2：處理每個重複群組
    print("⚙️ 處理重複社區...")
    process_results = []
    
    for group_id, group_indices in duplicate_groups.items():
        if len(group_indices) > 1:  # 只處理真正的重複群組
            group_data = df_processed.loc[group_indices]
            result = process_single_group(df_processed, group_indices, group_data)
            process_results.append(result)
    

    
    return df_processed

def identify_duplicate_groups(df):
    """
    識別重複社區群組
    條件：行政區 + 建照執照 + 經度相同
    """
    duplicate_groups = {}
    group_id = 0
    
    # 建立分組條件
    df['group_key'] = df['行政區'].astype(str) + '|' + \
                     df['建照執照'].astype(str) + '|' + \
                     df['經度'].astype(str)
    
    # 找出重複群組
    group_counts = df['group_key'].value_counts()
    duplicate_keys = group_counts[group_counts > 1].index
    
    for key in duplicate_keys:
        indices = df[df['group_key'] == key].index.tolist()
        if len(indices) > 1:
            duplicate_groups[group_id] = indices
            group_id += 1
    
    # 清理臨時欄位
    df.drop('group_key', axis=1, inplace=True)
    
    print(f"發現 {len(duplicate_groups)} 個重複群組")
    return duplicate_groups

def process_single_group(df, group_indices, group_data):
    """
    處理單一重複群組
    """
    group_info = {
        'group_indices': group_indices,
        'communities': group_data[['編號', '社區名稱', '銷售起始時間', '戶數', '預售交易筆數', '解約筆數']].to_dict('records'),
        'processing_type': '',
        'valid_community': None,
        'invalid_communities': [],
        'merged_data': {}
    }
    
    # 檢查是否為春豆子特殊案例
    if is_spring_bean_case(group_data):
        result = process_spring_bean_case(df, group_indices, group_data)
        group_info.update(result)
        group_info['processing_type'] = '春豆子特例（戶數相加）'
    else:
        result = process_general_case(df, group_indices, group_data)
        group_info.update(result)
        group_info['processing_type'] = '一般重複（取較晚銷售日期）'
    
    return group_info

def is_spring_bean_case(group_data):
    """
    判斷是否為春豆子案例
    """
    community_names = group_data['社區名稱'].tolist()
    return ('春豆子-公寓' in community_names and '春豆子-透天' in community_names)

def process_spring_bean_case(df, group_indices, group_data):
    """
    處理春豆子特殊案例：戶數相加
    """
    # 選擇第一個記錄作為有效社區
    valid_idx = group_indices[0]
    invalid_indices = group_indices[1:]
    valid_code = df.loc[valid_idx, '編號']
    
    # 計算合併後的數值
    total_units = group_data['戶數'].sum()
    total_transactions = group_data['預售交易筆數'].sum()
    total_cancellations = group_data['解約筆數'].sum()
    
    # 合併備查編號
    backup_numbers = group_data['備查編號清單'].dropna().tolist()
    merged_backup_numbers = ', '.join(backup_numbers)
    
    # 收集無效社區編號
    invalid_codes = [df.loc[idx, '編號'] for idx in invalid_indices]
    merged_invalid_codes = ', '.join(invalid_codes)
    
    # 更新有效社區的數據
    df.loc[valid_idx, '戶數'] = total_units
    df.loc[valid_idx, '預售交易筆數'] = total_transactions
    df.loc[valid_idx, '解約筆數'] = total_cancellations
    df.loc[valid_idx, '備查編號清單'] = merged_backup_numbers
    df.loc[valid_idx, '關連編號'] = merged_invalid_codes  # 有效社區記錄無效社區編號
    
    # 標記無效社區，關連編號保持為空
    for idx in invalid_indices:
        df.loc[idx, '社區有效性'] = 0
        df.loc[idx, '關連編號'] = None  # 無效社區關連編號為空
    
    return {
        'valid_community': valid_idx,
        'valid_code': valid_code,
        'invalid_communities': invalid_indices,
        'invalid_codes': invalid_codes,
        'merged_data': {
            '戶數': total_units,
            '預售交易筆數': total_transactions,
            '解約筆數': total_cancellations,
            '備查編號清單': merged_backup_numbers,
            '關連編號': merged_invalid_codes
        }
    }

def process_general_case(df, group_indices, group_data):
    """
    處理一般重複案例：取銷售起始時間較晚的記錄
    """
    # 轉換銷售起始時間為日期格式進行比較
    group_data_copy = group_data.copy()
    group_data_copy['銷售起始時間_date'] = pd.to_datetime(group_data_copy['銷售起始時間'], errors='coerce')
    
    # 找出銷售起始時間最晚的記錄，如果時間相同則選交易筆數較多的
    valid_idx = None
    latest_date = group_data_copy['銷售起始時間_date'].max()
    latest_records = group_data_copy[group_data_copy['銷售起始時間_date'] == latest_date]
    
    if len(latest_records) == 1:
        valid_idx = latest_records.index[0]
    else:
        # 時間相同時，選擇交易筆數較多的
        valid_idx = latest_records['預售交易筆數'].idxmax()
    
    invalid_indices = [idx for idx in group_indices if idx != valid_idx]
    valid_code = df.loc[valid_idx, '編號']
    
    # 計算合併後的數值
    total_transactions = group_data['預售交易筆數'].sum()
    total_cancellations = group_data['解約筆數'].sum()
    
    # 合併備查編號
    backup_numbers = group_data['備查編號清單'].dropna().tolist()
    merged_backup_numbers = ', '.join(backup_numbers)
    
    # 收集無效社區編號
    invalid_codes = [df.loc[idx, '編號'] for idx in invalid_indices]
    merged_invalid_codes = ', '.join(invalid_codes) if invalid_codes else None
    
    # 更新有效社區的數據
    df.loc[valid_idx, '預售交易筆數'] = total_transactions
    df.loc[valid_idx, '解約筆數'] = total_cancellations
    df.loc[valid_idx, '備查編號清單'] = merged_backup_numbers
    df.loc[valid_idx, '關連編號'] = merged_invalid_codes  # 有效社區記錄無效社區編號
    # 戶數保持原值（較晚記錄的戶數）
    
    # 標記無效社區，關連編號保持為空
    for idx in invalid_indices:
        df.loc[idx, '社區有效性'] = 0
        df.loc[idx, '關連編號'] = None  # 無效社區關連編號為空
    
    return {
        'valid_community': valid_idx,
        'valid_code': valid_code,
        'invalid_communities': invalid_indices,
        'invalid_codes': invalid_codes,
        'merged_data': {
            '戶數': df.loc[valid_idx, '戶數'],  # 保持原值
            '預售交易筆數': total_transactions,
            '解約筆數': total_cancellations,
            '備查編號清單': merged_backup_numbers,
            '關連編號': merged_invalid_codes
        }
    }