import os
import re
import time
import datetime
from typing import Tuple

from tqdm import tqdm 
import pandas as pd
import requests

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
    if not isinstance(text, str):
        return None
    match = re.search(r"\d{7}", text)
    if match:
        return match.group(0)  # 取出第一個符合 7 位數字的字串
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