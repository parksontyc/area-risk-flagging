import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ===== 日期轉換核心函數 =====

def parse_mixed_date_to_datetime(date_value) -> pd.Timestamp:
    """統一的日期轉換函數，支援多種格式轉換為datetime64"""
    if pd.isna(date_value) or date_value == '' or date_value is None:
        return pd.NaT
    
    try:
        # 如果已經是datetime類型，直接返回
        if isinstance(date_value, (pd.Timestamp, np.datetime64)):
            return pd.Timestamp(date_value)
        
        date_str = str(date_value).strip()
        
        # 處理民國年整數格式 (如: 1110701)
        if date_str.isdigit() and len(date_str) == 7:
            year = int(date_str[:3]) + 1911  # 民國年轉西元年
            month = int(date_str[3:5])
            day = int(date_str[5:7])
            return pd.Timestamp(year, month, day)
        
        # 處理民國年斜線格式 (如: "111/07/01")
        elif '/' in date_str and len(date_str.split('/')[0]) == 3:
            parts = date_str.split('/')
            year = int(parts[0]) + 1911  # 民國年轉西元年
            month = int(parts[1])
            day = int(parts[2])
            return pd.Timestamp(year, month, day)
        
        # 處理西元年格式 (如: "2022-07-01", "2022/07/01")
        else:
            return pd.to_datetime(date_str, errors='coerce')
            
    except Exception as e:
        return pd.NaT


def datetime_to_roc_quarter(dt_value) -> str:
    """將datetime64轉換為民國年季格式 (如: 111Y2S)"""
    if pd.isna(dt_value):
        return None
    
    try:
        ts = pd.Timestamp(dt_value)
        roc_year = ts.year - 1911  # 轉為民國年
        quarter = ts.quarter
        return f"{roc_year}Y{quarter}S"
    except:
        return None


# ===== 輔助函數 =====

def parse_id_string(text: str) -> List[str]:
    """解析備查編號清單字串，返回清理後的編號列表"""
    if pd.isna(text) or not isinstance(text, str) or text.strip() == '':
        return []
    
    return [s.strip().strip("'\"") for s in text.split(',') if s.strip()]


# ===== 價格異常值過濾 =====

def filter_price_outliers_enhanced(transaction_df: pd.DataFrame, 
                                 outlier_threshold: float = 0.03) -> pd.DataFrame:
    """
    強化版價格異常值過濾，去除極端值（使用建物單價）
    
    Parameters:
    -----------
    transaction_df : pd.DataFrame
        交易資料
    outlier_threshold : float
        異常值閾值，預設3%（上下各1.5%）
        
    Returns:
    --------
    pd.DataFrame
        過濾後的交易資料
    """
    df = transaction_df.copy()
    
    print("📊 === 價格異常值過濾（使用建物單價）===")
    
    # 檢查建物單價欄位是否存在
    if '建物單價' not in df.columns:
        print("  ❌ 缺少'建物單價'欄位，無法進行價格分析")
        return pd.DataFrame()
    
    # 過濾掉建物單價異常的記錄
    valid_mask = (df['建物單價'] > 0) & (df['建物單價'].notna())
    df_valid = df[valid_mask]
    
    print(f"  有效交易記錄：{len(df_valid)}/{len(df)} 筆")
    
    if len(df_valid) == 0:
        print("  ⚠️ 沒有有效的建物單價資料")
        return pd.DataFrame()
    
    # 計算百分位數，去除極端值
    lower_percentile = outlier_threshold / 2
    upper_percentile = 1 - (outlier_threshold / 2)
    
    price_lower = df_valid['建物單價'].quantile(lower_percentile)
    price_upper = df_valid['建物單價'].quantile(upper_percentile)
    
    print(f"  建物單價範圍：{price_lower:,.0f} ~ {price_upper:,.0f} 萬/坪")
    
    # 過濾極端值
    df_filtered = df_valid[
        (df_valid['建物單價'] >= price_lower) & 
        (df_valid['建物單價'] <= price_upper)
    ]
    
    filtered_rate = (len(df) - len(df_filtered)) / len(df) * 100
    print(f"  過濾結果：{len(df_filtered)}/{len(df)} 筆 (過濾 {filtered_rate:.1f}%)")
    
    return df_filtered


# ===== 量價查詢結構建立 =====

def create_quarterly_volume_price_lookup_structures(transaction_df: pd.DataFrame) -> Tuple[Dict, Dict, Dict, Dict]:
    """
    建立按季度分組的交易量價查詢結構（使用建物單價）
    
    Returns:
    --------
    Tuple[Dict, Dict, Dict, Dict]
        (成交筆數查詢表, 價格統計查詢表, 複合鍵成交筆數, 複合鍵價格統計)
    """
    print("🔧 建立季度量價查詢結構（使用建物單價）...")
    
    # 處理交易資料
    df_processed = transaction_df.copy()
    
    # 檢查建物單價欄位
    if '建物單價' not in df_processed.columns:
        print("  ❌ 缺少'建物單價'欄位")
        return {}, {}, {}, {}
    
    # 確保交易日期是datetime格式
    if not pd.api.types.is_datetime64_any_dtype(df_processed['交易日期']):
        df_processed['交易日期'] = df_processed['交易日期'].apply(parse_mixed_date_to_datetime)
    
    # 轉換為年季
    df_processed['交易年季'] = df_processed['交易日期'].apply(datetime_to_roc_quarter)
    
    # 移除無效年季的記錄
    df_processed = df_processed[df_processed['交易年季'].notna()]
    
    # 區分正常交易
    df_processed['是否正常交易'] = df_processed['解約情形'].isna()
    normal_transactions = df_processed[df_processed['是否正常交易']]
    
    print(f"  有效正常交易：{len(normal_transactions)} 筆")
    
    # 初始化結果字典
    id_quarterly_counts = defaultdict(lambda: defaultdict(int))
    id_quarterly_prices = defaultdict(lambda: defaultdict(list))
    composite_quarterly_counts = defaultdict(lambda: defaultdict(int))
    composite_quarterly_prices = defaultdict(lambda: defaultdict(list))
    
    # 按備查編號統計量價
    for _, row in normal_transactions.iterrows():
        backup_id = row['備查編號']
        quarter = row['交易年季']
        unit_price = row['建物單價']  # 改用建物單價
        
        if pd.notna(backup_id) and backup_id != '' and pd.notna(unit_price) and unit_price > 0:
            id_quarterly_counts[backup_id][quarter] += 1
            id_quarterly_prices[backup_id][quarter].append(unit_price)
    
    # 按複合鍵統計量價
    for _, row in normal_transactions.iterrows():
        composite_key = f"{row.get('縣市', '')}|{row.get('行政區', '')}|{row.get('社區名稱', '')}"
        quarter = row['交易年季']
        unit_price = row['建物單價']  # 改用建物單價
        
        if pd.notna(unit_price) and unit_price > 0:
            composite_quarterly_counts[composite_key][quarter] += 1
            composite_quarterly_prices[composite_key][quarter].append(unit_price)
    
    print(f"  建立完成：{len(id_quarterly_counts)} 個備查編號，{len(composite_quarterly_counts)} 個複合鍵")
    
    return (dict(id_quarterly_counts), dict(id_quarterly_prices), 
            dict(composite_quarterly_counts), dict(composite_quarterly_prices))


# ===== 社區量價統計計算 =====

def calculate_community_quarterly_volume_price(
    row: pd.Series, 
    quarter: str,
    id_quarterly_counts: Dict,
    id_quarterly_prices: Dict,
    composite_quarterly_counts: Dict,
    composite_quarterly_prices: Dict
) -> Dict:
    """
    計算單一社區在特定季度的量價統計
    
    Returns:
    --------
    Dict
        包含成交筆數、平均單價、中位數單價等統計資料
    """
    id_list = parse_id_string(row.get('備查編號清單', ''))
    
    # 初始化結果
    result = {
        '該季成交筆數': 0,
        '該季平均單價': 0,
        '該季中位數單價': 0,
        '該季最高單價': 0,
        '該季最低單價': 0,
        '該季單價標準差': 0
    }
    
    all_prices = []
    total_count = 0
    
    # 方法1：優先使用備查編號統計
    if id_list:
        found_any = False
        
        for backup_id in id_list:
            if backup_id in id_quarterly_counts and quarter in id_quarterly_counts[backup_id]:
                count = id_quarterly_counts[backup_id][quarter]
                prices = id_quarterly_prices[backup_id][quarter]
                
                total_count += count
                all_prices.extend(prices)
                found_any = True
        
        if found_any:
            result['該季成交筆數'] = total_count
            if all_prices:
                all_prices = [p for p in all_prices if pd.notna(p) and p > 0]
                if all_prices:
                    result['該季平均單價'] = np.mean(all_prices)
                    result['該季中位數單價'] = np.median(all_prices)
                    result['該季最高單價'] = np.max(all_prices)
                    result['該季最低單價'] = np.min(all_prices)
                    result['該季單價標準差'] = np.std(all_prices) if len(all_prices) > 1 else 0
            return result
    
    # 方法2：使用複合鍵
    composite_key = f"{row.get('縣市', '')}|{row.get('行政區', '')}|{row.get('社區名稱', '')}"
    
    if composite_key in composite_quarterly_counts and quarter in composite_quarterly_counts[composite_key]:
        count = composite_quarterly_counts[composite_key][quarter]
        prices = composite_quarterly_prices[composite_key][quarter]
        
        result['該季成交筆數'] = count
        if prices:
            prices = [p for p in prices if pd.notna(p) and p > 0]
            if prices:
                result['該季平均單價'] = np.mean(prices)
                result['該季中位數單價'] = np.median(prices)
                result['該季最高單價'] = np.max(prices)
                result['該季最低單價'] = np.min(prices)
                result['該季單價標準差'] = np.std(prices) if len(prices) > 1 else 0
    
    return result


# ===== 主要計算函數 =====

def calculate_community_quarterly_volume_price_analysis(
    community_df: pd.DataFrame, 
    transaction_df: pd.DataFrame
) -> pd.DataFrame:
    """
    計算每個社區在每個季度的量價完整分析
    
    Returns:
    --------
    pd.DataFrame
        包含每個社區每個季度成交筆數和價格統計的DataFrame
    """
    print("🚀 === 社區季度量價分析 ===")
    
    # 檢查必要欄位
    required_community_cols = ['編號', '備查編號清單', '縣市', '行政區', '社區名稱', '戶數', '銷售起始時間']
    required_transaction_cols = ['備查編號', '縣市', '行政區', '社區名稱', '交易日期', '解約情形', '建物單價']
    
    missing_community_cols = [col for col in required_community_cols if col not in community_df.columns]
    missing_transaction_cols = [col for col in required_transaction_cols if col not in transaction_df.columns]
    
    if missing_community_cols:
        raise ValueError(f"community_df 缺少必要欄位: {missing_community_cols}")
    if missing_transaction_cols:
        raise ValueError(f"transaction_df 缺少必要欄位: {missing_transaction_cols}")
    
    # 1. 過濾價格異常值
    # filtered_transaction_df = filter_price_outliers_enhanced(transaction_df)
    filtered_transaction_df = transaction_df
    
    if len(filtered_transaction_df) == 0:
        print("❌ 沒有有效的交易資料")
        return pd.DataFrame()
    
    # 2. 建立量價查詢結構
    (id_quarterly_counts, id_quarterly_prices, 
     composite_quarterly_counts, composite_quarterly_prices) = create_quarterly_volume_price_lookup_structures(filtered_transaction_df)
    
    # 3. 取得所有可用的季度
    all_quarters = set()
    valid_transactions = filtered_transaction_df[filtered_transaction_df['交易日期'].notna()]
    if len(valid_transactions) > 0:
        quarters_series = valid_transactions['交易日期'].apply(datetime_to_roc_quarter)
        transaction_quarters = quarters_series.dropna().unique()
        all_quarters.update(transaction_quarters)
    
    all_quarters = sorted(list(all_quarters))
    print(f"📊 分析時間範圍：{all_quarters[0] if all_quarters else 'None'} ~ {all_quarters[-1] if all_quarters else 'None'} ({len(all_quarters)} 個季度)")
    
    # 4. 處理社區資料
    community_processed = community_df.copy()
    
    # 確保銷售起始時間是datetime格式
    if not pd.api.types.is_datetime64_any_dtype(community_processed['銷售起始時間']):
        community_processed['銷售起始時間'] = community_processed['銷售起始時間'].apply(parse_mixed_date_to_datetime)
    
    community_processed['銷售起始年季'] = community_processed['銷售起始時間'].apply(datetime_to_roc_quarter)
    
    # 5. 計算每個社區每個季度的量價統計
    results = []
    
    print("🔄 計算各社區季度量價統計...")
    processed_count = 0
    
    for _, community_row in community_processed.iterrows():
        community_id = community_row['編號']
        sales_start_quarter = community_row['銷售起始年季']
        
        for quarter in all_quarters:
            # 只處理銷售起始季度之後的季度
            if sales_start_quarter and quarter >= sales_start_quarter:
                # 計算該社區該季度的量價統計
                volume_price_stats = calculate_community_quarterly_volume_price(
                    community_row, quarter, 
                    id_quarterly_counts, id_quarterly_prices,
                    composite_quarterly_counts, composite_quarterly_prices
                )
                
                # 只有當有交易時才添加記錄
                if volume_price_stats['該季成交筆數'] > 0:
                    result_row = {
                        '社區編號': community_id,
                        '縣市': community_row['縣市'],
                        '行政區': community_row['行政區'],
                        '社區名稱': community_row['社區名稱'],
                        '總戶數': community_row['戶數'],
                        '年季': quarter,
                        '銷售起始年季': sales_start_quarter,
                        **volume_price_stats  # 展開量價統計
                    }
                    results.append(result_row)
        
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"  已處理 {processed_count}/{len(community_processed)} 個社區")
    
    result_df = pd.DataFrame(results)
    
    if len(result_df) > 0:
        # 四捨五入價格欄位
        price_columns = ['該季平均單價', '該季中位數單價', '該季最高單價', '該季最低單價', '該季單價標準差']
        for col in price_columns:
            result_df[col] = result_df[col].round(2)
        
        # 按社區和季度排序
        result_df = result_df.sort_values(['社區編號', '年季']).reset_index(drop=True)
        
        print(f"✅ 計算完成！共產生 {len(result_df)} 筆季度量價記錄")
        print(f"📈 涵蓋 {result_df['社區編號'].nunique()} 個社區")
        
        # 基本統計
        print(f"📊 整體建物單價統計：")
        print(f"  平均建物單價：{result_df['該季平均單價'].mean():.2f} 萬/坪")
        print(f"  中位數建物單價：{result_df['該季平均單價'].median():.2f} 萬/坪")
        print(f"  建物單價範圍：{result_df['該季平均單價'].min():.2f} ~ {result_df['該季平均單價'].max():.2f} 萬/坪")
        
    else:
        print("⚠️ 未找到任何有效的季度量價記錄")
    
    return result_df


# ===== 橫向表格轉換（量價版本）=====

def pivot_quarterly_volume_price_horizontal(quarterly_df: pd.DataFrame) -> pd.DataFrame:
    """
    將季度量價資料轉換為橫向格式，包含成交筆數和平均單價
    
    Returns:
    --------
    pd.DataFrame
        橫向格式的量價資料表格
    """
    if quarterly_df.empty:
        print("⚠️ 輸入資料為空")
        return pd.DataFrame()
    
    print("🔄 轉換季度量價資料為橫向格式...")
    
    # 基本資訊欄位
    index_cols = ['社區編號', '社區名稱', '縣市', '行政區', '總戶數', '銷售起始年季']
    
    # 轉換成交筆數
    volume_pivot = quarterly_df.pivot_table(
        index=index_cols,
        columns='年季',
        values='該季成交筆數',
        fill_value=0,
        aggfunc='sum'
    ).reset_index()
    
    # 轉換平均建物單價
    price_pivot = quarterly_df.pivot_table(
        index=index_cols,
        columns='年季',
        values='該季平均單價',
        fill_value=0,
        aggfunc='mean'
    ).reset_index()
    
    # 合併量價資料
    quarters = [col for col in volume_pivot.columns if col not in index_cols]
    
    # 建立最終結果DataFrame
    result_df = volume_pivot.copy()
    result_df.columns.name = None
    
    # 為每個季度添加價格欄位
    for quarter in quarters:
        # 重命名成交筆數欄位
        volume_col = f"{quarter}_成交筆數"
        price_col = f"{quarter}_平均建物單價"
        
        result_df = result_df.rename(columns={quarter: volume_col})
        result_df[price_col] = price_pivot[quarter].round(2)
        
        # 調整欄位順序（成交筆數在前，價格在後）
        cols = result_df.columns.tolist()
        if price_col in cols and volume_col in cols:
            # 找到volume_col的位置，將price_col放在其後
            volume_idx = cols.index(volume_col)
            cols.remove(price_col)
            cols.insert(volume_idx + 1, price_col)
            result_df = result_df[cols]
    
    # 按社區編號排序
    result_df = result_df.sort_values('社區編號').reset_index(drop=True)
    
    quarter_count = len([col for col in result_df.columns if '_成交筆數' in col])
    print(f"✅ 轉換完成！共 {len(result_df)} 個社區，{quarter_count} 個季度（量價雙指標）")
    
    return result_df


# ===== 統計分析擴展（量價版本）=====

def add_volume_price_summary_statistics(horizontal_df: pd.DataFrame) -> pd.DataFrame:
    """
    為橫向量價表格添加綜合統計欄位
    
    Returns:
    --------
    pd.DataFrame
        添加統計欄位後的表格
    """
    df = horizontal_df.copy()
    
    print("📊 添加量價綜合統計欄位...")
    
    # 識別基本資訊欄位和季度欄位
    info_columns = ['社區編號', '社區名稱', '縣市', '行政區', '總戶數', '銷售起始年季']
    volume_columns = [col for col in df.columns if '_成交筆數' in col]
    price_columns = [col for col in df.columns if '_平均建物單價' in col]
    
    if not volume_columns or not price_columns:
        print("⚠️ 找不到有效的量價欄位")
        return df
    
    print(f"  找到 {len(volume_columns)} 個成交量欄位，{len(price_columns)} 個價格欄位")
    
    # === 成交量統計 ===
    df['累積成交筆數'] = df[volume_columns].sum(axis=1)
    df['平均季成交筆數'] = df[volume_columns].mean(axis=1).round(2)
    df['最大季成交筆數'] = df[volume_columns].max(axis=1)
    df['有交易季數'] = (df[volume_columns] > 0).sum(axis=1)
    
    # === 去化率分析 ===
    df['累積去化率(%)'] = ((df['累積成交筆數'] / df['總戶數']) * 100).round(2)
    df['平均季去化率(%)'] = ((df['平均季成交筆數'] / df['總戶數']) * 100).round(2)
    
    # === 價格統計 ===
    # 計算加權平均建物單價（以成交筆數為權重）
    def calculate_weighted_avg_price(row):
        total_amount = 0
        total_volume = 0
        
        for vol_col, price_col in zip(volume_columns, price_columns):
            volume = row[vol_col]
            price = row[price_col]
            
            if volume > 0 and price > 0:
                total_amount += volume * price
                total_volume += volume
        
        return total_amount / total_volume if total_volume > 0 else 0
    
    df['加權平均建物單價'] = df.apply(calculate_weighted_avg_price, axis=1).round(2)
    
    # 簡單平均建物單價（忽略成交量）
    price_data = df[price_columns].replace(0, np.nan)
    df['簡單平均建物單價'] = price_data.mean(axis=1).round(2)
    df['最高建物單價'] = price_data.max(axis=1).round(2)
    df['最低建物單價'] = price_data.min(axis=1).round(2)
    df['建物單價標準差'] = price_data.std(axis=1).round(2)
    
    # === 價格趨勢分析 ===
    if len(price_columns) >= 2:
        # 首季和末季比較
        first_price_col = price_columns[0]
        last_price_col = price_columns[-1]
        
        df['首季建物單價'] = df[first_price_col]
        df['末季建物單價'] = df[last_price_col]
        
        # 計算建物單價變化率
        def calculate_price_change_rate(row):
            first_price = row[first_price_col]
            last_price = row[last_price_col]
            
            if first_price > 0 and last_price > 0:
                return round(((last_price - first_price) / first_price * 100), 2)
            return 0
        
        df['建物單價變化率(%)'] = df.apply(calculate_price_change_rate, axis=1)
        
        # 價格趨勢判斷
        def analyze_price_trend(change_rate):
            if change_rate > 10:
                return "📈 大幅上漲"
            elif change_rate > 3:
                return "📈 溫和上漲"
            elif change_rate > -3:
                return "➡️ 價格穩定"
            elif change_rate > -10:
                return "📉 溫和下跌"
            else:
                return "📉 大幅下跌"
        
        df['建物單價趨勢'] = df['建物單價變化率(%)'].apply(analyze_price_trend)
    
    # === 去化效率評級（結合價格因素）===
    def evaluate_volume_price_efficiency(row):
        absorption_rate = row['累積去化率(%)']
        active_quarters = row['有交易季數']
        avg_quarterly_rate = row['平均季去化率(%)']
        price_trend = row.get('建物單價變化率(%)', 0)
        
        # 基礎效率評級
        if absorption_rate >= 100:
            if active_quarters <= 8:
                base_rating = "🚀 高效完售"
            elif active_quarters <= 12:
                base_rating = "⭐ 正常完售"
            else:
                base_rating = "⚠️ 緩慢完售"
        elif absorption_rate >= 80:
            if avg_quarterly_rate >= 3:
                base_rating = "🚀 高效去化"
            elif avg_quarterly_rate >= 1.5:
                base_rating = "⭐ 正常去化"
            else:
                base_rating = "⚠️ 緩慢去化"
        elif absorption_rate >= 50:
            if avg_quarterly_rate >= 2:
                base_rating = "⭐ 正常去化"
            elif avg_quarterly_rate >= 1:
                base_rating = "⚠️ 緩慢去化"
            else:
                base_rating = "🐌 滯銷狀態"
        else:
            base_rating = "🔴 嚴重滯銷"
        
        # 加入價格趨勢修正
        if price_trend < -10:
            base_rating += "(價跌)"
        elif price_trend > 10:
            base_rating += "(價漲)"
            
        return base_rating
    
    df['量價效率評級'] = df.apply(evaluate_volume_price_efficiency, axis=1)
    
    # === 市場定位分析 ===
    def analyze_market_position(row):
        avg_price = row['加權平均建物單價']
        absorption_rate = row['累積去化率(%)']
        
        if avg_price >= 80 and absorption_rate >= 70:
            return "🏆 高價熱銷"
        elif avg_price >= 80 and absorption_rate < 50:
            return "💎 高價滯銷"
        elif avg_price < 40 and absorption_rate >= 70:
            return "🎯 平價熱銷"
        elif avg_price < 40 and absorption_rate < 50:
            return "⚠️ 平價滯銷"
        else:
            return "📊 中價位產品"
    
    df['市場定位'] = df.apply(analyze_market_position, axis=1)
    
    print("✅ 量價綜合統計欄位添加完成")
    
    return df


# ===== 完整工作流程 =====

def complete_volume_price_analysis_workflow(
    community_file: str, 
    transaction_file: str, 
    target_city: str = None,
    output_file: str = None
) -> pd.DataFrame:
    """
    完整的量價分析工作流程
    
    Parameters:
    -----------
    community_file : str
        社區資料檔案路徑
    transaction_file : str
        交易資料檔案路徑
    target_city : str, optional
        指定分析的縣市
    output_file : str, optional
        輸出檔案名稱
        
    Returns:
    --------
    pd.DataFrame
        完整的量價分析結果
    """
    print("🚀 === 社區季度量價分析完整工作流程 ===")
    
    # 1. 載入資料
    print("\n📁 Step 1: 載入資料...")
    community_df = pd.read_csv(community_file, encoding='utf-8-sig')
    transaction_df = pd.read_csv(transaction_file, encoding='utf-8-sig')
    
    print(f"社區資料：{len(community_df)} 筆")
    print(f"交易資料：{len(transaction_df)} 筆")
    
    # 2. 計算季度量價統計
    print(f"\n🔄 Step 2: 計算季度量價統計...")
    quarterly_results = calculate_community_quarterly_volume_price_analysis(community_df, transaction_df)
    
    if len(quarterly_results) == 0:
        print("❌ 沒有產生有效的分析結果")
        return pd.DataFrame()
    
    # 3. 篩選特定縣市（如果指定）
    if target_city:
        print(f"\n📍 Step 3: 篩選縣市【{target_city}】...")
        quarterly_results = quarterly_results[quarterly_results['縣市'] == target_city]
        print(f"篩選後記錄數：{len(quarterly_results)}")
    
    # 4. 轉換為橫向格式
    print(f"\n🔄 Step 4: 轉換為橫向量價格式...")
    horizontal_df = pivot_quarterly_volume_price_horizontal(quarterly_results)
    
    # 5. 添加統計分析
    print(f"\n📊 Step 5: 添加量價綜合統計...")
    enhanced_df = add_volume_price_summary_statistics(horizontal_df)
    
    # 6. 匯出結果
    if output_file is None:
        city_suffix = f"_{target_city}" if target_city else "_全國"
        output_file = f'社區季度量價分析{city_suffix}_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
    
    print(f"\n📁 Step 6: 匯出分析結果...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 主要分析報告
        enhanced_df.to_excel(writer, sheet_name='量價分析報告', index=False)
        
        # 季度明細資料
        quarterly_results.to_excel(writer, sheet_name='季度明細資料', index=False)
        
        # 橫向量價表格
        horizontal_df.to_excel(writer, sheet_name='橫向量價表格', index=False)
        
        # 摘要統計
        summary_stats = generate_summary_statistics(enhanced_df)
        summary_stats.to_excel(writer, sheet_name='摘要統計', index=False)
    
    print(f"✅ 分析完成！結果已匯出至：{output_file}")
    print(f"📊 最終分析了 {len(enhanced_df)} 個社區的量價資料")
    
    return enhanced_df


def generate_summary_statistics(enhanced_df: pd.DataFrame) -> pd.DataFrame:
    """生成摘要統計表"""
    summary_data = []
    
    # 整體統計
    summary_data.append({
        '統計項目': '社區總數',
        '數值': len(enhanced_df),
        '單位': '個'
    })
    
    summary_data.append({
        '統計項目': '平均戶數',
        '數值': enhanced_df['總戶數'].mean(),
        '單位': '戶'
    })
    
    summary_data.append({
        '統計項目': '平均累積去化率',
        '數值': enhanced_df['累積去化率(%)'].mean(),
        '單位': '%'
    })
    
    summary_data.append({
        '統計項目': '平均加權建物單價',
        '數值': enhanced_df['加權平均建物單價'].mean(),
        '單位': '萬/坪'
    })
    
    # 效率分級統計
    efficiency_dist = enhanced_df['量價效率評級'].value_counts()
    for rating, count in efficiency_dist.items():
        summary_data.append({
            '統計項目': f'效率分級-{rating}',
            '數值': count,
            '單位': '個'
        })
    
    return pd.DataFrame(summary_data)


# ===== 使用範例 =====

def example_usage():
    """使用範例"""
    print("📝 === 量價分析系統使用範例 ===")
    print("""
# 1. 快速完整分析（推薦）
result_df = complete_volume_price_analysis_workflow(
    'community_data_proc.csv',
    'transection_data_test.csv',
    target_city='臺北市',  # 可選：指定縣市
    output_file='台北市量價分析.xlsx'  # 可選：指定輸出檔案
)

# 2. 分步驟分析
# 載入資料
community_df = pd.read_csv('community_data_proc.csv', encoding='utf-8-sig')
transaction_df = pd.read_csv('transection_data_test.csv', encoding='utf-8-sig')

# 計算季度量價統計
quarterly_results = calculate_community_quarterly_volume_price_analysis(community_df, transaction_df)

# 轉換為橫向格式
horizontal_df = pivot_quarterly_volume_price_horizontal(quarterly_results)

# 添加綜合統計
enhanced_df = add_volume_price_summary_statistics(horizontal_df)

# 3. 檢視結果重點欄位
key_columns = [
    '社區編號', '社區名稱', '縣市', '行政區', '總戶數',
    '累積成交筆數', '累積去化率(%)', '加權平均建物單價',
    '建物單價變化率(%)', '建物單價趨勢', '量價效率評級', '市場定位'
]
print(enhanced_df[key_columns].head())

# 4. 分析應用範例
# 高價熱銷社區
high_price_hot = enhanced_df[enhanced_df['市場定位'] == '🏆 高價熱銷']
print(f"高價熱銷社區：{len(high_price_hot)} 個")

# 建物單價上漲且去化良好的社區
price_rising_good = enhanced_df[
    (enhanced_df['建物單價變化率(%)'] > 5) & 
    (enhanced_df['累積去化率(%)'] > 60)
]
print(f"建物單價上漲且去化好的社區：{len(price_rising_good)} 個")

# 各縣市平均建物單價比較
city_price_comparison = enhanced_df.groupby('縣市').agg({
    '加權平均建物單價': 'mean',
    '累積去化率(%)': 'mean',
    '社區編號': 'count'
}).round(2)
print(city_price_comparison)
    """)


if __name__ == "__main__":
    example_usage()