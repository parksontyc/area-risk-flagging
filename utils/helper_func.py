import os
import re
import time
import datetime
from typing import List, Set, Tuple
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


def create_transaction_lookup_structures(transaction_df: pd.DataFrame) -> Tuple[Set[str], pd.Series]:
    """
    建立交易資料的查詢結構以提升效能
    
    Parameters:
    -----------
    transaction_df : pd.DataFrame
        交易資料，需包含備查編號、縣市、行政區、社區名稱欄位
        
    Returns:
    --------
    Tuple[Set[str], pd.Series]
        (有效備查編號集合, composite_key對應的交易筆數)
    """
    # 建立有效備查編號集合
    valid_ids = set(transaction_df['備查編號'].dropna().astype(str))
    
    # 建立 composite key 並統計交易筆數
    composite_keys = (
        transaction_df[['縣市', '行政區', '社區名稱']]
        .fillna('')
        .astype(str)
        .agg('|'.join, axis=1)
    )
    
    # 計算每個 composite key 的交易筆數
    composite_key_counts = composite_keys.value_counts()
    
    return valid_ids, composite_key_counts


def count_transactions_for_community(
    row: pd.Series, 
    valid_ids: Set[str], 
    composite_key_counts: pd.Series,
    transaction_df: pd.DataFrame
) -> int:
    """
    計算單一社區的預售交易筆數
    
    Parameters:
    -----------
    row : pd.Series
        社區資料的一列
    valid_ids : Set[str]
        有效的備查編號集合
    composite_key_counts : pd.Series
        composite key 對應的交易筆數
    transaction_df : pd.DataFrame
        交易資料（用於精確匹配）
        
    Returns:
    --------
    int
        該社區的預售交易筆數
    """
    id_list = parse_id_string(row.get('備查編號清單', ''))
    
    # 方法1：優先使用備查編號匹配
    if id_list:
        matched_ids = [i for i in id_list if i in valid_ids]
        if matched_ids:
            return transaction_df['備查編號'].isin(matched_ids).sum()
    
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
    valid_ids, composite_key_counts = create_transaction_lookup_structures(transaction_df)
    
    # 計算每個社區的交易筆數
    transaction_counts = community_df.apply(
        lambda row: count_transactions_for_community(
            row, valid_ids, composite_key_counts, transaction_df
        ),
        axis=1
    )
    
    return transaction_counts


