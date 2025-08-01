"""
預售屋市場去化率分析程式 (含縣市行政區深度分析)
考慮銷售時間因素的進階分析
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

import sys
from pathlib import Path
project_root = Path.cwd().parent
print(project_root)
sys.path.append(str(project_root))
from matplotlib.font_manager import fontManager
import matplotlib as mlp
font_path = Path(project_root) / "utils" / "ChineseFont.ttf"
fontManager.addfont(str(font_path))
mlp.rc('font', family="ChineseFont")

# 設定中文字體
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class PresaleMarketAnalysis:
    def __init__(self, analysis_date=None):
        self.transaction_data = None
        self.community_data = None
        self.analysis_result = None
        if analysis_date:
            self.current_date = analysis_date
        else:
            self.current_date = datetime(2025, 6, 30)
        print(f"分析基準日期設定為: {self.current_date.strftime('%Y-%m-%d')}")
        
    def load_data(self, transaction_file, community_file):
        """載入交易資料和社區資料"""
        print("正在載入資料...")
        print("📋 分析策略：僅分析在兩個檔案中都有對應編號的社區")
        print("   • 交易檔案的 '備查編號' 欄位")
        print("   • 社區檔案的 '編號' 欄位")
        print("   • 確保資料完整性和分析準確性")
        
        self.transaction_data = pd.read_csv(transaction_file)
        print(f"✅ 交易資料載入完成: {len(self.transaction_data)} 筆記錄")
        
        self.community_data = pd.read_csv(community_file)
        print(f"✅ 社區資料載入完成: {len(self.community_data)} 個社區")
        
        return self
    
    def parse_date_columns(self):
        """解析日期相關欄位"""
        print("正在解析日期資料...")
        
        date_columns = ['銷售起始時間', '自售起始時間', '代銷起始時間', '備查完成日期', '建照核發日']
        
        for col in date_columns:
            if col in self.community_data.columns:
                self.community_data[col] = pd.to_numeric(self.community_data[col], errors='coerce')
                self.community_data[f'{col}_date'] = self.community_data[col].apply(self.convert_taiwan_date)
        
        if '交易年月' in self.transaction_data.columns:
            self.transaction_data['交易年月'] = pd.to_numeric(self.transaction_data['交易年月'], errors='coerce')
            self.transaction_data['交易日期_parsed'] = self.transaction_data['交易年月'].apply(self.convert_taiwan_yearmonth)
        
        print("日期解析完成")
        return self

    def analyze_quarterly_trends(self):
        """分析季度趨勢"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return None
        
        print("正在分析季度趨勢...")
        
        if '交易年季' not in self.transaction_data.columns:
            print("⚠️ 交易資料中缺少 '交易年季' 欄位，無法進行季度分析")
            return None
        
        transaction_quarterly = self.transaction_data.dropna(subset=['交易年季', '備查編號'])
        quarterly_sales = transaction_quarterly.groupby(['備查編號', '交易年季']).size().reset_index(name='季度銷售戶數')
        
        community_with_id = self.community_data.dropna(subset=['編號'])
        community_with_id['編號'] = community_with_id['編號'].astype(str)
        quarterly_sales['備查編號'] = quarterly_sales['備查編號'].astype(str)
        
        quarterly_analysis = pd.merge(
            quarterly_sales,
            community_with_id[['編號', '縣市', '行政區', '社區名稱', '戶數']],
            left_on='備查編號',
            right_on='編號',
            how='inner'
        )
        
        quarterly_analysis = quarterly_analysis.sort_values(['備查編號', '交易年季'])
        quarterly_analysis['累積銷售戶數'] = quarterly_analysis.groupby('備查編號')['季度銷售戶數'].cumsum()
        quarterly_analysis['累積去化率'] = (quarterly_analysis['累積銷售戶數'] / quarterly_analysis['戶數']) * 100
        quarterly_analysis['累積去化率'] = quarterly_analysis['累積去化率'].round(2)
        
        self.quarterly_analysis = quarterly_analysis
        print(f"✅ 季度趨勢分析完成")
        return quarterly_analysis

    def analyze_city_detailed(self, city_name=None):
        """詳細分析特定縣市或所有縣市"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        if not hasattr(self, 'quarterly_analysis'):
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
        
        cities_to_analyze = [city_name] if city_name else self.analysis_result['縣市'].unique()
        self.city_detailed_analysis = {}
        
        for city in cities_to_analyze:
            if pd.isna(city):
                continue
                
            print(f"\n正在分析 {city}...")
            
            city_data = self.analysis_result[self.analysis_result['縣市'] == city].copy()
            city_quarterly = None
            if hasattr(self, 'quarterly_analysis') and self.quarterly_analysis is not None:
                city_quarterly = self.quarterly_analysis[self.quarterly_analysis['縣市'] == city].copy()
            
            district_analysis = city_data.groupby('行政區').agg({
                '戶數': 'sum',
                '已售戶數': 'sum',
                '去化率': 'mean',
                '月均去化率': 'mean',
                '銷售天數': 'mean'
            }).reset_index()
            
            district_analysis['整體去化率'] = (district_analysis['已售戶數'] / district_analysis['戶數']) * 100
            district_analysis['整體去化率'] = district_analysis['整體去化率'].round(2)
            district_analysis['社區數量'] = city_data.groupby('行政區').size().values
            district_analysis = district_analysis.sort_values('整體去化率', ascending=False)
            
            quarterly_district_ranking = None
            if city_quarterly is not None and len(city_quarterly) > 0:
                quarterly_district = city_quarterly.groupby(['交易年季', '行政區'])['累積去化率'].mean().reset_index()
                quarterly_district_ranking = quarterly_district.pivot(index='行政區', columns='交易年季', values='累積去化率')
                
                for quarter in quarterly_district_ranking.columns:
                    quarterly_district_ranking[f'{quarter}_排名'] = quarterly_district_ranking[quarter].rank(ascending=False)
            
            district_community_ranking = {}
            for district in city_data['行政區'].unique():
                if pd.isna(district):
                    continue
                    
                district_communities = city_data[city_data['行政區'] == district].copy()
                district_communities = district_communities.sort_values('去化率', ascending=False)
                
                high_performing = district_communities[
                    (district_communities['去化率'] > 60) | 
                    (district_communities.index.isin(district_communities.head(5).index))
                ]
                
                low_performing = district_communities[
                    (district_communities['去化率'] < 30) | 
                    (district_communities.index.isin(district_communities.tail(5).index))
                ].sort_values('去化率')
                
                district_community_ranking[district] = {
                    'high_performing': high_performing,
                    'low_performing': low_performing,
                    'total_communities': len(district_communities)
                }
            
            self.city_detailed_analysis[city] = {
                'basic_stats': {
                    'total_communities': len(city_data),
                    'total_units': city_data['戶數'].sum(),
                    'sold_units': city_data['已售戶數'].sum(),
                    'overall_absorption_rate': (city_data['已售戶數'].sum() / city_data['戶數'].sum()) * 100,
                    'avg_monthly_rate': city_data['月均去化率'].mean(),
                    'districts_count': len(city_data['行政區'].unique())
                },
                'district_analysis': district_analysis,
                'quarterly_district_ranking': quarterly_district_ranking,
                'district_community_ranking': district_community_ranking
            }
        
        print(f"✅ 完成 {len(cities_to_analyze)} 個縣市的詳細分析")
        return self.city_detailed_analysis

    def generate_city_detailed_report(self, city_name=None):
        """生成詳細的縣市分析報告"""
        if not hasattr(self, 'city_detailed_analysis'):
            print("請先執行詳細縣市分析")
            return
        
        cities_to_report = [city_name] if city_name else list(self.city_detailed_analysis.keys())
        
        for city in cities_to_report:
            if city not in self.city_detailed_analysis:
                print(f"❌ 找不到 {city} 的分析資料")
                continue
            
            analysis = self.city_detailed_analysis[city]
            
            print("\n" + "="*80)
            print(f"🏙️ {city} 詳細市場分析報告")
            print("="*80)
            
            stats = analysis['basic_stats']
            print(f"\n📊 {city} 市場基本概況")
            print(f"行政區數量: {stats['districts_count']} 個")
            print(f"社區總數: {stats['total_communities']} 個")
            print(f"總戶數: {stats['total_units']:,} 戶")
            print(f"已售戶數: {stats['sold_units']:,} 戶")
            print(f"整體去化率: {stats['overall_absorption_rate']:.2f}%")
            print(f"平均月去化率: {stats['avg_monthly_rate']:.2f}%")
            
            district_analysis = analysis['district_analysis']
            print(f"\n🏘️ {city} 行政區去化率排名")
            print(f"{'排名':<4} {'行政區':<12} {'整體去化率':<10} {'社區數':<6} {'總戶數':<8} {'已售戶數':<12} {'月均去化率':<12}")
            print("-" * 90)
            
            for idx, row in district_analysis.iterrows():
                rank = district_analysis.index.get_loc(idx) + 1
                print(f"{rank:<4} {row['行政區']:<12} {row['整體去化率']:<8.1f}% "
                    f"{row['社區數量']:<6} {row['戶數']:<10,} {row['已售戶數']:<10,} "
                    f"{row['月均去化率']:<8.2f}%")

    def create_city_visualizations(self, city_name):
        """為特定縣市創建視覺化圖表"""
        if not hasattr(self, 'city_detailed_analysis') or city_name not in self.city_detailed_analysis:
            print(f"請先執行 {city_name} 的詳細分析")
            return
        
        analysis = self.city_detailed_analysis[city_name]
        district_analysis = analysis['district_analysis']
        
        try:
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'{city_name} 市場分析視覺化', fontsize=16, fontweight='bold')
            
            axes[0,0].barh(district_analysis['行政區'], district_analysis['整體去化率'], color='skyblue')
            axes[0,0].set_title(f'{city_name} 行政區去化率排名')
            axes[0,0].set_xlabel('整體去化率 (%)')
            
            for i, v in enumerate(district_analysis['整體去化率']):
                axes[0,0].text(v + 1, i, f'{v:.1f}%', va='center')
            
            axes[0,1].pie(district_analysis['社區數量'], labels=district_analysis['行政區'], 
                        autopct='%1.1f%%', startangle=90)
            axes[0,1].set_title(f'{city_name} 各行政區社區數量分布')
            
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"⚠️ {city_name} 視覺化圖表生成失敗: {str(e)}")

    def analyze_single_city(self, city_name):
        """分析單一縣市的詳細報告"""
        if self.analysis_result is None:
            print("請先執行基本去化率計算")
            return
        
        available_cities = self.analysis_result['縣市'].unique()
        if city_name not in available_cities:
            print(f"❌ 找不到縣市 '{city_name}'")
            print(f"可用的縣市: {', '.join(available_cities)}")
            return
        
        print(f"🎯 開始分析 {city_name}...")
        
        if not hasattr(self, 'quarterly_analysis'):
            self.analyze_quarterly_trends()
        
        self.analyze_city_detailed(city_name)
        self.generate_city_detailed_report(city_name)
        
        try:
            self.create_city_visualizations(city_name)
        except Exception as e:
            print(f"⚠️ {city_name} 視覺化失敗: {str(e)}")
        
        print(f"✅ {city_name} 分析完成！")
        return self.city_detailed_analysis.get(city_name)
    
    def check_data_matching(self):
        """檢查兩個檔案的編號比對情況"""
        print("\n" + "="*50)
        print("資料比對檢查報告")
        print("="*50)
        
        if '備查編號' not in self.transaction_data.columns:
            print("❌ 交易資料中找不到 '備查編號' 欄位")
            return
        
        if '編號' not in self.community_data.columns:
            print("❌ 社區資料中找不到 '編號' 欄位")
            return
        
        transaction_with_id = self.transaction_data.dropna(subset=['備查編號'])
        community_with_id = self.community_data.dropna(subset=['編號'])
        
        transaction_ids = set(transaction_with_id['備查編號'].astype(str))
        community_ids = set(community_with_id['編號'].astype(str))
        matched_ids = transaction_ids.intersection(community_ids)
        
        only_in_transaction = transaction_ids - community_ids
        only_in_community = community_ids - transaction_ids
        
        print(f"📊 編號比對統計：")
        print(f"交易資料編號總數: {len(transaction_ids)}")
        print(f"社區資料編號總數: {len(community_ids)}")
        print(f"✅ 成功比對編號數: {len(matched_ids)}")
        print(f"⚠️ 僅存在於交易資料: {len(only_in_transaction)}")
        print(f"⚠️ 僅存在於社區資料: {len(only_in_community)}")
        print(f"📈 比對成功率: {len(matched_ids)/max(len(transaction_ids), len(community_ids))*100:.1f}%")

    def analyze_district_performance(self):
        """分析行政區去化表現"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return None
        
        print("正在進行行政區分析...")
        
        district_stats = self.analysis_result.groupby(['縣市', '行政區']).agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean',
            '銷售天數': 'mean'
        }).reset_index()
        
        district_stats['整體去化率'] = (district_stats['已售戶數'] / district_stats['戶數']) * 100
        district_stats['整體去化率'] = district_stats['整體去化率'].round(2)
        
        district_community_count = self.analysis_result.groupby(['縣市', '行政區']).size().reset_index(name='社區數量')
        district_stats = pd.merge(district_stats, district_community_count, on=['縣市', '行政區'])
        
        def categorize_district_performance(rate):
            if rate >= 80:
                return '優異'
            elif rate >= 60:
                return '良好'
            elif rate >= 40:
                return '普通'
            elif rate >= 20:
                return '待改善'
            else:
                return '困難'
        
        district_stats['表現等級'] = district_stats['整體去化率'].apply(categorize_district_performance)
        district_stats = district_stats.sort_values('整體去化率', ascending=False)
        
        self.district_analysis = district_stats
        print(f"✅ 行政區分析完成，共分析 {len(district_stats)} 個行政區")
        return district_stats
    
    def analyze_city_performance(self):
        """分析縣市去化表現"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return None
        
        print("正在進行縣市分析...")
        
        city_stats = self.analysis_result.groupby('縣市').agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean',
            '銷售天數': 'mean'
        }).reset_index()
        
        city_stats['整體去化率'] = (city_stats['已售戶數'] / city_stats['戶數']) * 100
        city_stats['整體去化率'] = city_stats['整體去化率'].round(2)
        
        city_district_count = self.analysis_result.groupby('縣市')['行政區'].nunique().reset_index()
        city_district_count.columns = ['縣市', '行政區數量']
        
        city_community_count = self.analysis_result.groupby('縣市').size().reset_index(name='社區數量')
        
        city_stats = pd.merge(city_stats, city_district_count, on='縣市')
        city_stats = pd.merge(city_stats, city_community_count, on='縣市')
        
        def categorize_city_performance(rate):
            if rate >= 70:
                return '熱門'
            elif rate >= 50:
                return '穩健'
            elif rate >= 30:
                return '平穩'
            else:
                return '冷淡'
        
        city_stats['市場熱度'] = city_stats['整體去化率'].apply(categorize_city_performance)
        city_stats = city_stats.sort_values('整體去化率', ascending=False)
        
        self.city_analysis = city_stats
        print(f"✅ 縣市分析完成，共分析 {len(city_stats)} 個縣市")
        return city_stats
    
    def convert_taiwan_date(self, date_val):
        """將民國年日期轉換為西元年日期"""
        if pd.isna(date_val) or date_val == 0:
            return None
        
        try:
            date_str = str(int(date_val))
            if len(date_str) == 7:
                year = int(date_str[:3]) + 1911
                month = int(date_str[3:5])
                day = int(date_str[5:7])
                return datetime(year, month, day)
            elif len(date_str) == 6:
                year = int(date_str[:3]) + 1911
                month = int(date_str[3:5])
                day = int(date_str[5:6]) if len(date_str[5:]) == 1 else int(date_str[5:7])
                return datetime(year, month, day)
        except:
            return None
        
        return None
    
    def convert_taiwan_yearmonth(self, yearmonth_val):
        """將民國年月轉換為西元年月"""
        if pd.isna(yearmonth_val):
            return None
        
        try:
            yearmonth_str = str(int(yearmonth_val))
            if len(yearmonth_str) >= 5:
                year = int(yearmonth_str[:3]) + 1911
                month = int(yearmonth_str[3:5])
                return datetime(year, month, 1)
        except:
            return None
        
        return None
    
    def preprocess_data(self):
        """數據預處理"""
        print("正在進行數據預處理...")
        
        self.parse_date_columns()
        
        self.transaction_data = self.transaction_data.dropna(subset=['社區名稱', '縣市', '行政區'])
        
        self.community_data = self.community_data.dropna(subset=['社區名稱', '戶數'])
        self.community_data['戶數'] = pd.to_numeric(self.community_data['戶數'], errors='coerce')
        self.community_data = self.community_data.dropna(subset=['戶數'])
        
        self.calculate_sales_period()
        
        matched_count = self.check_data_matching()
        if matched_count == 0:
            print("❌ 資料預處理中止：無法找到匹配的編號")
            return None
        
        print("✅ 數據預處理完成")
        return self
    
    def calculate_sales_period(self):
        """計算銷售期間相關指標"""
        self.community_data['銷售開始日期'] = (
            self.community_data['銷售起始時間_date'].fillna(
                self.community_data['自售起始時間_date'].fillna(
                    self.community_data['代銷起始時間_date']
                )
            )
        )
        
        self.community_data['銷售天數'] = (
            self.current_date - self.community_data['銷售開始日期']
        ).dt.days
        
        self.community_data['銷售天數'] = self.community_data['銷售天數'].apply(
            lambda x: max(0, x) if pd.notna(x) else 0
        )
        
        self.community_data['銷售階段'] = pd.cut(
            self.community_data['銷售天數'],
            bins=[0, 90, 180, 365, 730, float('inf')],
            labels=['新推案(<3個月)', '初期銷售(3-6個月)', '穩定銷售(6-12個月)', 
                   '長期銷售(1-2年)', '長期銷售(>2年)'],
            include_lowest=True
        )
    
    def calculate_time_adjusted_absorption_rate(self):
        """計算考慮時間因素的去化率"""
        print("正在計算時間調整後的去化率...")
        
        if '備查編號' not in self.transaction_data.columns:
            print("❌ 交易資料中缺少 '備查編號' 欄位")
            return self
        
        if '編號' not in self.community_data.columns:
            print("❌ 社區資料中缺少 '編號' 欄位")
            return self
        
        print("正在進行編號比對...")
        
        transaction_with_id = self.transaction_data.dropna(subset=['備查編號'])
        community_with_id = self.community_data.dropna(subset=['編號'])
        
        transaction_ids = set(transaction_with_id['備查編號'].astype(str))
        community_ids = set(community_with_id['編號'].astype(str))
        matched_ids = transaction_ids.intersection(community_ids)
        
        print(f"交易資料中的備查編號數量: {len(transaction_ids)}")
        print(f"社區資料中的編號數量: {len(community_ids)}")
        print(f"成功比對的編號數量: {len(matched_ids)}")
        
        if len(matched_ids) == 0:
            print("❌ 沒有找到匹配的編號，請檢查編號格式是否一致")
            return self
        
        filtered_transaction = transaction_with_id[
            transaction_with_id['備查編號'].astype(str).isin(matched_ids)
        ].copy()
        
        filtered_community = community_with_id[
            community_with_id['編號'].astype(str).isin(matched_ids)
        ].copy()
        
        sold_units = filtered_transaction.groupby('備查編號').size().reset_index(name='已售戶數')
        sold_units['備查編號'] = sold_units['備查編號'].astype(str)
        
        community_for_merge = filtered_community.copy()
        community_for_merge['編號'] = community_for_merge['編號'].astype(str)
        
        merged_data = pd.merge(
            community_for_merge[['編號', '縣市', '行政區', '社區名稱', '戶數', '銷售開始日期', '銷售天數', '銷售階段']],
            sold_units,
            left_on='編號',
            right_on='備查編號',
            how='left'
        )
        
        merged_data['已售戶數'] = merged_data['已售戶數'].fillna(0)
        
        merged_data['去化率'] = (merged_data['已售戶數'] / merged_data['戶數']) * 100
        merged_data['去化率'] = merged_data['去化率'].round(2)
        
        merged_data['銷售月數'] = merged_data['銷售天數'] / 30.44
        merged_data['銷售月數'] = merged_data['銷售月數'].apply(lambda x: max(0.5, x))
        merged_data['月均去化率'] = merged_data['去化率'] / merged_data['銷售月數']
        merged_data['月均去化率'] = merged_data['月均去化率'].round(2)
        
        merged_data['預估完全去化月數'] = np.where(
            merged_data['月均去化率'] > 0,
            100 / merged_data['月均去化率'],
            float('inf')
        )
        
        def categorize_performance(row):
            if row['銷售天數'] < 90:
                if row['去化率'] > 20:
                    return '新案熱銷'
                elif row['去化率'] > 10:
                    return '新案穩健'
                else:
                    return '新案待觀察'
            elif row['銷售天數'] < 365:
                if row['去化率'] > 70:
                    return '銷售優異'
                elif row['去化率'] > 40:
                    return '銷售良好'
                elif row['去化率'] > 20:
                    return '銷售普通'
                else:
                    return '銷售緩慢'
            else:
                if row['去化率'] > 80:
                    return '長期穩健'
                elif row['去化率'] > 50:
                    return '持續銷售'
                elif row['去化率'] > 30:
                    return '銷售遲緩'
                else:
                    return '去化困難'
        
        merged_data['銷售表現'] = merged_data.apply(categorize_performance, axis=1)
        
        self.analysis_result = merged_data
        print(f"✅ 時間調整去化率計算完成，共分析 {len(merged_data)} 個有編號比對的社區")
        return self
    
    def generate_time_aware_report(self):
        """生成考慮時間因素的分析報告"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        print("\n" + "="*60)
        print("預售屋市場時間調整去化率分析報告")
        print("(基於備查編號精確比對)")
        print("="*60)
        
        total_communities = len(self.analysis_result)
        total_units = self.analysis_result['戶數'].sum()
        total_sold = self.analysis_result['已售戶數'].sum()
        overall_rate = (total_sold / total_units) * 100
        avg_monthly_rate = self.analysis_result['月均去化率'].mean()
        
        print(f"\n📊 整體市場概況")
        print(f"✅ 成功比對社區數: {total_communities:,} 個")
        print(f"📋 這些社區在兩個檔案中都有對應的備查編號/編號")
        print(f"🏠 總戶數: {total_units:,} 戶")
        print(f"💰 已售戶數: {total_sold:,} 戶") 
        print(f"📈 整體去化率: {overall_rate:.2f}%")
        print(f"⚡ 平均月去化率: {avg_monthly_rate:.2f}%")

    def generate_district_city_report(self):
        """生成縣市行政區分析報告"""
        if not hasattr(self, 'district_analysis') or not hasattr(self, 'city_analysis'):
            print("請先執行縣市和行政區分析")
            return
        
        print("\n" + "="*60)
        print("縣市與行政區去化率深度分析")
        print("="*60)
        
        print(f"\n🏙️ 縣市市場表現排名")
        print(f"{'排名':<4} {'縣市':<8} {'整體去化率':<10} {'總戶數':<8} {'已售戶數':<8} {'社區數':<6} {'行政區數':<6} {'市場熱度':<8}")
        print("-" * 70)
        
        for idx, row in self.city_analysis.head(15).iterrows():
            print(f"{idx+1:<4} {row['縣市']:<8} {row['整體去化率']:<10.1f}% "
                  f"{row['戶數']:<8,} {row['已售戶數']:<8,} {row['社區數量']:<6} "
                  f"{row['行政區數量']:<6} {row['市場熱度']:<8}")

    def create_time_aware_visualizations(self):
        """創建考慮時間因素的視覺化圖表"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        try:
            plt.style.use('seaborn-v0_8')
        except:
            pass
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('預售屋市場時間調整分析', fontsize=16, fontweight='bold')
        
        stage_dist = self.analysis_result['銷售階段'].value_counts()
        axes[0,0].pie(stage_dist.values, labels=stage_dist.index, autopct='%1.1f%%', startangle=90)
        axes[0,0].set_title('銷售階段分布')
        
        performance_dist = self.analysis_result['銷售表現'].value_counts()
        axes[0,1].bar(range(len(performance_dist)), performance_dist.values, color='lightblue')
        axes[0,1].set_xticks(range(len(performance_dist)))
        axes[0,1].set_xticklabels(performance_dist.index, rotation=45, ha='right')
        axes[0,1].set_title('銷售表現分布')
        axes[0,1].set_ylabel('社區數量')
        
        scatter = axes[1,0].scatter(self.analysis_result['銷售天數'], self.analysis_result['去化率'], 
                                   alpha=0.6, c=self.analysis_result['月均去化率'], cmap='viridis')
        axes[1,0].set_title('銷售天數 vs 去化率')
        axes[1,0].set_xlabel('銷售天數')
        axes[1,0].set_ylabel('去化率 (%)')
        
        axes[1,1].hist(self.analysis_result['月均去化率'], bins=30, color='lightgreen', alpha=0.7)
        axes[1,1].set_title('月均去化率分布')
        axes[1,1].set_xlabel('月均去化率 (%)')
        axes[1,1].set_ylabel('社區數量')
        
        plt.tight_layout()
        plt.show()

    def create_district_heatmap(self):
        """創建行政區去化率熱力圖"""
        if not hasattr(self, 'district_analysis'):
            print("請先執行行政區分析")
            return
        
        try:
            pivot_data = self.district_analysis.pivot(index='縣市', columns='行政區', values='整體去化率')
            pivot_data = pivot_data.dropna(how='all').fillna(0)
            
            if len(pivot_data) > 0:
                plt.figure(figsize=(16, 10))
                sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='RdYlGn', 
                           center=50, cbar_kws={'label': '去化率 (%)'})
                plt.title('縣市-行政區去化率熱力圖', fontsize=14, fontweight='bold')
                plt.xlabel('行政區')
                plt.ylabel('縣市')
                plt.xticks(rotation=45, ha='right')
                plt.yticks(rotation=0)
                plt.tight_layout()
                plt.show()
            else:
                print("無足夠數據創建熱力圖")
        except Exception as e:
            print(f"熱力圖創建失敗: {str(e)}")

    def analyze_market_efficiency(self):
        """分析市場效率"""
        print("\n" + "="*60)
        print("市場效率分析")
        print("="*60)
        
        efficient_communities = self.analysis_result[self.analysis_result['月均去化率'] > 3]
        slow_communities = self.analysis_result[
            (self.analysis_result['銷售天數'] > 365) & 
            (self.analysis_result['去化率'] < 50)
        ]
        
        print(f"\n📈 市場效率指標")
        print(f"高效銷售社區 (月均去化率>3%): {len(efficient_communities)} 個")
        print(f"銷售緩慢社區 (1年以上且去化率<50%): {len(slow_communities)} 個")
        print(f"市場效率比: {len(efficient_communities)/(len(slow_communities)+1):.2f}")

    def export_time_aware_results(self, filename="time_aware_presale_analysis", format_type="csv"):
        """匯出時間調整分析結果"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        export_columns = ['編號', '縣市', '行政區', '社區名稱', '戶數', '已售戶數', '去化率', 
                        '銷售天數', '月均去化率', '銷售階段', '銷售表現', '預估完全去化月數']
        main_data = self.analysis_result[export_columns].copy()
        
        main_filename = f"{filename}_主要分析.csv"
        main_data.to_csv(main_filename, index=False, encoding='utf-8-sig')
        
        print(f"時間調整分析結果已匯出為: {main_filename}")

# 季度

    def show_quarterly_performance_menu(self):
        """季度表現分析選單"""
        if self.analysis_result is None:
            print("請先執行基本分析")
            return
        
        while True:
            print("\n" + "="*60)
            print("📈 季度去化表現分析")
            print("="*60)
            print("1. 🏙️ 選擇縣市查看行政區季度表現")
            print("2. 📊 全市場季度趨勢總覽")
            print("3. 🏘️ 特定行政區季度詳細分析")
            print("4. 📋 季度表現排名變化")
            print("5. 📈 季度去化率趨勢圖表")
            print("0. ❌ 返回主選單")
            print("-" * 60)
            
            try:
                choice = input("請選擇功能 (0-5): ").strip()
                
                if choice == "0":
                    print("返回主選單...")
                    break
                elif choice == "1":
                    self.show_city_quarterly_districts()
                elif choice == "2":
                    self.show_market_quarterly_overview()
                elif choice == "3":
                    self.show_district_quarterly_detail()
                elif choice == "4":
                    self.show_quarterly_ranking_changes()
                elif choice == "5":
                    self.plot_quarterly_trends()
                else:
                    print("❌ 無效的選擇，請輸入 0-5 之間的數字")
                    
            except KeyboardInterrupt:
                print("\n程式中斷，返回主選單...")
                break
            except Exception as e:
                print(f"❌ 發生錯誤: {str(e)}")

    def show_city_quarterly_districts(self):
        """顯示特定縣市的行政區季度表現"""
        if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
            if self.quarterly_analysis is None:
                print("❌ 無法進行季度分析，可能缺少交易年季資料")
                return
        
        available_cities = sorted(self.quarterly_analysis['縣市'].unique())
        print("\n📍 可用縣市清單:")
        for i, city in enumerate(available_cities, 1):
            print(f"{i:2d}. {city}")
        
        try:
            choice = input(f"\n請選擇縣市 (1-{len(available_cities)}) 或輸入縣市名稱: ").strip()
            
            if choice.isdigit():
                city_index = int(choice) - 1
                if 0 <= city_index < len(available_cities):
                    selected_city = available_cities[city_index]
                else:
                    print("❌ 無效的選擇")
                    return
            else:
                if choice in available_cities:
                    selected_city = choice
                else:
                    print("❌ 找不到該縣市")
                    return
            
            self.display_city_quarterly_performance(selected_city)
            
        except Exception as e:
            print(f"❌ 錯誤: {str(e)}")

    def display_city_quarterly_performance(self, city_name):
        """顯示特定縣市的季度表現"""
        city_quarterly = self.quarterly_analysis[self.quarterly_analysis['縣市'] == city_name].copy()
        
        if len(city_quarterly) == 0:
            print(f"❌ {city_name} 沒有季度交易資料")
            return
        
        # 計算各季各行政區的綜合數據
        quarterly_district_stats = city_quarterly.groupby(['交易年季', '行政區']).agg({
            '累積去化率': 'mean',
            '季度銷售戶數': 'sum',
            '戶數': 'sum',
            '備查編號': 'nunique'  # 社區數
        }).reset_index()
        
        quarterly_district_stats.columns = ['交易年季', '行政區', '累積去化率', '季度銷售戶數', '總戶數', '社區數']
        
        # 取得所有季度並排序
        quarters = sorted(quarterly_district_stats['交易年季'].unique())
        all_districts = sorted(quarterly_district_stats['行政區'].unique())
        
        print(f"\n🏙️ {city_name} 各行政區季度表現詳細分析")
        print("="*120)
        
        # 1. 累積去化率表現
        print(f"\n📈 各行政區季度累積去化率 (%)")
        self._print_quarterly_table(quarterly_district_stats, all_districts, quarters, '累積去化率', 'rate')
        
        # 2. 季度銷售戶數
        print(f"\n🏠 各行政區季度銷售戶數")
        self._print_quarterly_table(quarterly_district_stats, all_districts, quarters, '季度銷售戶數', 'number')
        
        # 3. 參與社區數
        print(f"\n🏘️ 各行政區季度參與社區數")
        self._print_quarterly_table(quarterly_district_stats, all_districts, quarters, '社區數', 'number')
        
        # 4. 總戶數
        print(f"\n🏗️ 各行政區季度總戶數")
        self._print_quarterly_table(quarterly_district_stats, all_districts, quarters, '總戶數', 'number')
        
        # 季度統計摘要
        print(f"\n📊 {city_name} 季度統計摘要")
        print("="*100)
        print(f"{'季度':<10} {'總銷售戶數':<12} {'平均去化率':<12} {'參與行政區':<12} {'參與社區數':<12} {'總戶數':<12}")
        print("-" * 100)
        
        for quarter in quarters:
            quarter_data = quarterly_district_stats[quarterly_district_stats['交易年季'] == quarter]
            if len(quarter_data) > 0:
                total_sales = quarter_data['季度銷售戶數'].sum()
                avg_rate = quarter_data['累積去化率'].mean()
                districts_count = quarter_data['行政區'].nunique()
                communities_count = quarter_data['社區數'].sum()
                total_units = quarter_data['總戶數'].sum()
                
                print(f"{str(quarter):<10} {total_sales:>10,} {avg_rate:>8.1f}% "
                    f"{districts_count:>8} {communities_count:>10} {total_units:>10,}")

    def _print_quarterly_table(self, data, districts, quarters, column, data_type):
        """列印季度數據表格的輔助方法"""
        # 計算欄位寬度
        max_district_len = max(len(district) for district in districts) if districts else 8
        district_width = max(10, max_district_len + 2)
        
        # 表頭
        print(f"{'行政區':<{district_width}}", end="")
        for quarter in quarters:
            print(f"{str(quarter):<12}", end="")
        if data_type == 'rate':
            print("平均去化率")
        else:
            print("總計")
        print("-" * (district_width + 12 * len(quarters) + 12))
        
        # 數據行
        for district in districts:
            print(f"{district:<{district_width}}", end="")
            quarter_values = []
            
            for quarter in quarters:
                district_quarter_data = data[
                    (data['行政區'] == district) & (data['交易年季'] == quarter)
                ]
                
                if len(district_quarter_data) > 0:
                    value = district_quarter_data[column].iloc[0]
                    if data_type == 'rate':
                        print(f"{value:>8.1f}%   ", end="")
                    else:
                        print(f"{value:>8,}    ", end="")
                    quarter_values.append(value)
                else:
                    print(f"{'--':>8}    ", end="")
            
            # 計算總計或平均
            if quarter_values:
                if data_type == 'rate':
                    avg_value = sum(quarter_values) / len(quarter_values)
                    print(f"{avg_value:>8.1f}%")
                else:
                    total_value = sum(quarter_values)
                    print(f"{total_value:>8,}")
            else:
                print(f"{'--':>8}")
        print()  # 空行分隔

    def show_market_quarterly_overview(self):
        """顯示全市場季度趨勢總覽"""
        if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
            if self.quarterly_analysis is None:
                print("❌ 無法進行季度分析")
                return
        
        print("\n📊 全市場季度趨勢總覽")
        print("="*70)
        
        # 按季度統計
        quarterly_summary = self.quarterly_analysis.groupby('交易年季').agg({
            '季度銷售戶數': 'sum',
            '累積銷售戶數': 'sum',
            '累積去化率': 'mean',
            '縣市': 'nunique',
            '行政區': 'nunique',
            '備查編號': 'nunique'
        }).reset_index()
        
        quarterly_summary.columns = ['季度', '季度銷售戶數', '累積銷售戶數', '平均累積去化率', '縣市數', '行政區數', '社區數']
        
        print(f"{'季度':<12} {'季度銷售':<10} {'累積銷售':<10} {'平均去化率':<12} {'縣市數':<8} {'行政區數':<10} {'社區數':<8}")
        print("-" * 80)
        
        for _, row in quarterly_summary.iterrows():
            print(f"{str(row['季度']):<12} {row['季度銷售戶數']:>8,} {row['累積銷售戶數']:>8,} "
                f"{row['平均累積去化率']:>8.1f}% {row['縣市數']:>6} {row['行政區數']:>8} {row['社區數']:>6}")
        
        # 季度成長分析
        print(f"\n📈 季度成長分析")
        print("-" * 50)
        
        for i in range(1, len(quarterly_summary)):
            current = quarterly_summary.iloc[i]
            previous = quarterly_summary.iloc[i-1]
            
            sales_growth = ((current['季度銷售戶數'] - previous['季度銷售戶數']) / previous['季度銷售戶數'] * 100) if previous['季度銷售戶數'] > 0 else 0
            rate_change = current['平均累積去化率'] - previous['平均累積去化率']
            
            print(f"{current['季度']}: 銷售戶數 {sales_growth:+.1f}%, 去化率 {rate_change:+.1f}%")

    def show_district_quarterly_detail(self):
        """顯示特定行政區的季度詳細分析"""
        if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
            if self.quarterly_analysis is None:
                print("❌ 無法進行季度分析")
                return
        
        # 顯示可用的縣市-行政區組合
        district_combinations = self.quarterly_analysis.groupby(['縣市', '行政區']).size().reset_index(name='交易季數')
        district_combinations = district_combinations.sort_values(['縣市', '行政區'])
        
        print("\n📍 可用行政區清單 (交易季數≥2):")
        qualified_districts = district_combinations[district_combinations['交易季數'] >= 2]
        
        for i, (_, row) in enumerate(qualified_districts.iterrows(), 1):
            print(f"{i:2d}. {row['縣市']}-{row['行政區']} ({row['交易季數']}季有交易)")
        
        try:
            choice = input(f"\n請選擇行政區 (1-{len(qualified_districts)}): ").strip()
            
            if choice.isdigit():
                district_index = int(choice) - 1
                if 0 <= district_index < len(qualified_districts):
                    selected_row = qualified_districts.iloc[district_index]
                    self.display_district_quarterly_detail(selected_row['縣市'], selected_row['行政區'])
                else:
                    print("❌ 無效的選擇")
            else:
                print("❌ 請輸入數字")
                
        except Exception as e:
            print(f"❌ 錯誤: {str(e)}")

    def display_district_quarterly_detail(self, city_name, district_name):
        """顯示特定行政區的季度詳細資料"""
        district_data = self.quarterly_analysis[
            (self.quarterly_analysis['縣市'] == city_name) & 
            (self.quarterly_analysis['行政區'] == district_name)
        ].copy()
        
        print(f"\n🏘️ {city_name}-{district_name} 季度詳細分析")
        print("="*70)
        
        # 按季度和社區顯示
        quarterly_community = district_data.groupby(['交易年季', '社區名稱']).agg({
            '季度銷售戶數': 'sum',
            '累積銷售戶數': 'max',
            '累積去化率': 'max',
            '戶數': 'first'
        }).reset_index()
        
        for quarter in sorted(quarterly_community['交易年季'].unique()):
            quarter_data = quarterly_community[quarterly_community['交易年季'] == quarter]
            
            print(f"\n📅 {quarter}")
            print(f"{'社區名稱':<20} {'季度銷售':<10} {'累積銷售':<10} {'累積去化率':<12} {'總戶數':<8}")
            print("-" * 70)
            
            for _, row in quarter_data.iterrows():
                print(f"{row['社區名稱']:<20} {row['季度銷售戶數']:>8} {row['累積銷售戶數']:>8} "
                    f"{row['累積去化率']:>8.1f}% {row['戶數']:>6}")
            
            quarter_total_sales = quarter_data['季度銷售戶數'].sum()
            quarter_avg_rate = quarter_data['累積去化率'].mean()
            print(f"{'小計':<20} {quarter_total_sales:>8} {'':>8} {quarter_avg_rate:>8.1f}%")

    def show_quarterly_ranking_changes(self):
        """顯示季度排名變化"""
        if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
            if self.quarterly_analysis is None:
                print("❌ 無法進行季度分析")
                return
        
        print("\n🏆 行政區季度排名變化分析")
        print("="*70)
        
        # 計算各季度各行政區的平均去化率
        quarterly_district_avg = self.quarterly_analysis.groupby(['交易年季', '縣市', '行政區'])['累積去化率'].mean().reset_index()
        
        quarters = sorted(quarterly_district_avg['交易年季'].unique())
        
        if len(quarters) < 2:
            print("❌ 季度資料不足，無法進行排名比較")
            return
        
        for i, quarter in enumerate(quarters):
            quarter_data = quarterly_district_avg[quarterly_district_avg['交易年季'] == quarter].copy()
            quarter_data = quarter_data.sort_values('累積去化率', ascending=False)
            quarter_data['排名'] = range(1, len(quarter_data) + 1)
            
            print(f"\n📅 {quarter} 排名 (TOP 10)")
            print(f"{'排名':<4} {'縣市-行政區':<20} {'累積去化率':<12}")
            print("-" * 40)
            
            for _, row in quarter_data.head(10).iterrows():
                district_full_name = f"{row['縣市']}-{row['行政區']}"
                print(f"{row['排名']:<4} {district_full_name:<20} {row['累積去化率']:>8.1f}%")

    def plot_quarterly_trends(self):
        """繪製季度趨勢圖表"""
        if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
            print("正在進行季度分析...")
            self.analyze_quarterly_trends()
            if self.quarterly_analysis is None:
                print("❌ 無法進行季度分析")
                return
        
        try:
            import matplotlib.pyplot as plt
            
            # 全市場季度趨勢
            quarterly_summary = self.quarterly_analysis.groupby('交易年季').agg({
                '季度銷售戶數': 'sum',
                '累積去化率': 'mean'
            }).reset_index()
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # 季度銷售戶數趨勢
            quarters_str = [str(q) for q in quarterly_summary['交易年季']]
            ax1.plot(quarters_str, quarterly_summary['季度銷售戶數'], marker='o', linewidth=2, color='blue')
            ax1.set_title('季度銷售戶數趨勢', fontsize=14, fontweight='bold')
            ax1.set_ylabel('銷售戶數')
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)
            
            # 平均累積去化率趨勢
            ax2.plot(quarters_str, quarterly_summary['累積去化率'], marker='s', linewidth=2, color='red')
            ax2.set_title('平均累積去化率趨勢', fontsize=14, fontweight='bold')
            ax2.set_ylabel('累積去化率 (%)')
            ax2.set_xlabel('季度')
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            plt.show()
            
        except ImportError:
            print("❌ 無法載入 matplotlib，請安裝該套件以顯示圖表")
        except Exception as e:
            print(f"❌ 圖表生成失敗: {str(e)}")

    # def show_quarterly_performance_menu(self):
    #     """季度表現分析選單"""
    #     if self.analysis_result is None:
    #         print("請先執行基本分析")
    #         return
        
    #     while True:
    #         print("\n" + "="*60)
    #         print("📈 季度去化表現分析")
    #         print("="*60)
    #         print("1. 🏙️ 選擇縣市查看行政區季度表現")
    #         print("2. 📊 全市場季度趨勢總覽")
    #         print("3. 🏘️ 特定行政區季度詳細分析")
    #         print("4. 📋 季度表現排名變化")
    #         print("5. 📈 季度去化率趨勢圖表")
    #         print("0. ❌ 返回主選單")
    #         print("-" * 60)
            
    #         try:
    #             choice = input("請選擇功能 (0-5): ").strip()
                
    #             if choice == "0":
    #                 print("返回主選單...")
    #                 break
    #             elif choice == "1":
    #                 self.show_city_quarterly_districts()
    #             elif choice == "2":
    #                 self.show_market_quarterly_overview()
    #             elif choice == "3":
    #                 self.show_district_quarterly_detail()
    #             elif choice == "4":
    #                 self.show_quarterly_ranking_changes()
    #             elif choice == "5":
    #                 self.plot_quarterly_trends()
    #             else:
    #                 print("❌ 無效的選擇，請輸入 0-5 之間的數字")
                    
    #         except KeyboardInterrupt:
    #             print("\n程式中斷，返回主選單...")
    #             break
    #         except Exception as e:
    #             print(f"❌ 發生錯誤: {str(e)}")

    # def show_city_quarterly_districts(self):
    #     """顯示特定縣市的行政區季度表現"""
    #     if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
    #         print("正在進行季度分析...")
    #         self.analyze_quarterly_trends()
    #         if self.quarterly_analysis is None:
    #             print("❌ 無法進行季度分析，可能缺少交易年季資料")
    #             return
        
    #     available_cities = sorted(self.quarterly_analysis['縣市'].unique())
    #     print("\n📍 可用縣市清單:")
    #     for i, city in enumerate(available_cities, 1):
    #         print(f"{i:2d}. {city}")
        
    #     try:
    #         choice = input(f"\n請選擇縣市 (1-{len(available_cities)}) 或輸入縣市名稱: ").strip()
            
    #         if choice.isdigit():
    #             city_index = int(choice) - 1
    #             if 0 <= city_index < len(available_cities):
    #                 selected_city = available_cities[city_index]
    #             else:
    #                 print("❌ 無效的選擇")
    #                 return
    #         else:
    #             if choice in available_cities:
    #                 selected_city = choice
    #             else:
    #                 print("❌ 找不到該縣市")
    #                 return
            
    #         self.display_city_quarterly_performance(selected_city)
            
    #     except Exception as e:
    #         print(f"❌ 錯誤: {str(e)}")

    # def display_city_quarterly_performance(self, city_name):
    #     """顯示特定縣市的季度表現"""
    #     city_quarterly = self.quarterly_analysis[self.quarterly_analysis['縣市'] == city_name].copy()
        
    #     if len(city_quarterly) == 0:
    #         print(f"❌ {city_name} 沒有季度交易資料")
    #         return
        
    #     # 計算各季各行政區的累積去化率
    #     quarterly_district = city_quarterly.groupby(['交易年季', '行政區'])['累積去化率'].mean().reset_index()
    #     quarterly_pivot = quarterly_district.pivot(index='行政區', columns='交易年季', values='累積去化率')
        
    #     # 取得所有季度並排序
    #     quarters = sorted([col for col in quarterly_pivot.columns])
        
    #     print(f"\n🏙️ {city_name} 各行政區季度累積去化率表現")
    #     print("="*80)
        
    #     # 計算欄位寬度
    #     max_district_len = max(len(district) for district in quarterly_pivot.index)
    #     district_width = max(8, max_district_len + 2)
        
    #     # 表頭
    #     print(f"{'行政區':<{district_width}}", end="")
    #     for quarter in quarters:
    #         print(f"{str(quarter):<12}", end="")
    #     print("平均去化率")
    #     print("-" * (district_width + 12 * len(quarters) + 12))
        
    #     # 數據行
    #     for district in quarterly_pivot.index:
    #         print(f"{district:<{district_width}}", end="")
    #         quarter_rates = []
    #         for quarter in quarters:
    #             rate = quarterly_pivot.loc[district, quarter]
    #             if pd.notna(rate):
    #                 print(f"{rate:>8.1f}%   ", end="")
    #                 quarter_rates.append(rate)
    #             else:
    #                 print(f"{'--':>8}    ", end="")
            
    #         # 計算平均去化率
    #         if quarter_rates:
    #             avg_rate = sum(quarter_rates) / len(quarter_rates)
    #             print(f"{avg_rate:>8.1f}%")
    #         else:
    #             print(f"{'--':>8}")
        
    #     # 季度統計摘要
    #     print(f"\n📊 {city_name} 季度統計摘要")
    #     print("-" * 50)
        
    #     for quarter in quarters:
    #         quarter_data = city_quarterly[city_quarterly['交易年季'] == quarter]
    #         if len(quarter_data) > 0:
    #             total_sales = quarter_data['季度銷售戶數'].sum()
    #             avg_rate = quarter_data['累積去化率'].mean()
    #             districts_count = quarter_data['行政區'].nunique()
                
    #             print(f"{quarter}: 銷售{total_sales:,}戶, 平均累積去化率{avg_rate:.1f}%, {districts_count}個行政區有交易")

    # def show_market_quarterly_overview(self):
    #     """顯示全市場季度趨勢總覽"""
    #     if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
    #         print("正在進行季度分析...")
    #         self.analyze_quarterly_trends()
    #         if self.quarterly_analysis is None:
    #             print("❌ 無法進行季度分析")
    #             return
        
    #     print("\n📊 全市場季度趨勢總覽")
    #     print("="*70)
        
    #     # 按季度統計
    #     quarterly_summary = self.quarterly_analysis.groupby('交易年季').agg({
    #         '季度銷售戶數': 'sum',
    #         '累積銷售戶數': 'sum',
    #         '累積去化率': 'mean',
    #         '縣市': 'nunique',
    #         '行政區': 'nunique',
    #         '備查編號': 'nunique'
    #     }).reset_index()
        
    #     quarterly_summary.columns = ['季度', '季度銷售戶數', '累積銷售戶數', '平均累積去化率', '縣市數', '行政區數', '社區數']
        
    #     print(f"{'季度':<12} {'季度銷售':<10} {'累積銷售':<10} {'平均去化率':<12} {'縣市數':<8} {'行政區數':<10} {'社區數':<8}")
    #     print("-" * 80)
        
    #     for _, row in quarterly_summary.iterrows():
    #         print(f"{str(row['季度']):<12} {row['季度銷售戶數']:>8,} {row['累積銷售戶數']:>8,} "
    #               f"{row['平均累積去化率']:>8.1f}% {row['縣市數']:>6} {row['行政區數']:>8} {row['社區數']:>6}")
        
    #     # 季度成長分析
    #     print(f"\n📈 季度成長分析")
    #     print("-" * 50)
        
    #     for i in range(1, len(quarterly_summary)):
    #         current = quarterly_summary.iloc[i]
    #         previous = quarterly_summary.iloc[i-1]
            
    #         sales_growth = ((current['季度銷售戶數'] - previous['季度銷售戶數']) / previous['季度銷售戶數'] * 100) if previous['季度銷售戶數'] > 0 else 0
    #         rate_change = current['平均累積去化率'] - previous['平均累積去化率']
            
    #         print(f"{current['季度']}: 銷售戶數 {sales_growth:+.1f}%, 去化率 {rate_change:+.1f}%")

    # def show_district_quarterly_detail(self):
    #     """顯示特定行政區的季度詳細分析"""
    #     if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
    #         print("正在進行季度分析...")
    #         self.analyze_quarterly_trends()
    #         if self.quarterly_analysis is None:
    #             print("❌ 無法進行季度分析")
    #             return
        
    #     # 顯示可用的縣市-行政區組合
    #     district_combinations = self.quarterly_analysis.groupby(['縣市', '行政區']).size().reset_index(name='交易季數')
    #     district_combinations = district_combinations.sort_values(['縣市', '行政區'])
        
    #     print("\n📍 可用行政區清單 (交易季數≥2):")
    #     qualified_districts = district_combinations[district_combinations['交易季數'] >= 2]
        
    #     for i, (_, row) in enumerate(qualified_districts.iterrows(), 1):
    #         print(f"{i:2d}. {row['縣市']}-{row['行政區']} ({row['交易季數']}季有交易)")
        
    #     try:
    #         choice = input(f"\n請選擇行政區 (1-{len(qualified_districts)}): ").strip()
            
    #         if choice.isdigit():
    #             district_index = int(choice) - 1
    #             if 0 <= district_index < len(qualified_districts):
    #                 selected_row = qualified_districts.iloc[district_index]
    #                 self.display_district_quarterly_detail(selected_row['縣市'], selected_row['行政區'])
    #             else:
    #                 print("❌ 無效的選擇")
    #         else:
    #             print("❌ 請輸入數字")
                
    #     except Exception as e:
    #         print(f"❌ 錯誤: {str(e)}")

    # def display_district_quarterly_detail(self, city_name, district_name):
    #     """顯示特定行政區的季度詳細資料"""
    #     district_data = self.quarterly_analysis[
    #         (self.quarterly_analysis['縣市'] == city_name) & 
    #         (self.quarterly_analysis['行政區'] == district_name)
    #     ].copy()
        
    #     print(f"\n🏘️ {city_name}-{district_name} 季度詳細分析")
    #     print("="*70)
        
    #     # 按季度和社區顯示
    #     quarterly_community = district_data.groupby(['交易年季', '社區名稱']).agg({
    #         '季度銷售戶數': 'sum',
    #         '累積銷售戶數': 'max',
    #         '累積去化率': 'max',
    #         '戶數': 'first'
    #     }).reset_index()
        
    #     for quarter in sorted(quarterly_community['交易年季'].unique()):
    #         quarter_data = quarterly_community[quarterly_community['交易年季'] == quarter]
            
    #         print(f"\n📅 {quarter}")
    #         print(f"{'社區名稱':<20} {'季度銷售':<10} {'累積銷售':<10} {'累積去化率':<12} {'總戶數':<8}")
    #         print("-" * 70)
            
    #         for _, row in quarter_data.iterrows():
    #             print(f"{row['社區名稱']:<20} {row['季度銷售戶數']:>8} {row['累積銷售戶數']:>8} "
    #                   f"{row['累積去化率']:>8.1f}% {row['戶數']:>6}")
            
    #         quarter_total_sales = quarter_data['季度銷售戶數'].sum()
    #         quarter_avg_rate = quarter_data['累積去化率'].mean()
    #         print(f"{'小計':<20} {quarter_total_sales:>8} {'':>8} {quarter_avg_rate:>8.1f}%")

    # def show_quarterly_ranking_changes(self):
    #     """顯示季度排名變化"""
    #     if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
    #         print("正在進行季度分析...")
    #         self.analyze_quarterly_trends()
    #         if self.quarterly_analysis is None:
    #             print("❌ 無法進行季度分析")
    #             return
        
    #     print("\n🏆 行政區季度排名變化分析")
    #     print("="*70)
        
    #     # 計算各季度各行政區的平均去化率
    #     quarterly_district_avg = self.quarterly_analysis.groupby(['交易年季', '縣市', '行政區'])['累積去化率'].mean().reset_index()
        
    #     quarters = sorted(quarterly_district_avg['交易年季'].unique())
        
    #     if len(quarters) < 2:
    #         print("❌ 季度資料不足，無法進行排名比較")
    #         return
        
    #     for i, quarter in enumerate(quarters):
    #         quarter_data = quarterly_district_avg[quarterly_district_avg['交易年季'] == quarter].copy()
    #         quarter_data = quarter_data.sort_values('累積去化率', ascending=False)
    #         quarter_data['排名'] = range(1, len(quarter_data) + 1)
            
    #         print(f"\n📅 {quarter} 排名 (TOP 10)")
    #         print(f"{'排名':<4} {'縣市-行政區':<20} {'累積去化率':<12}")
    #         print("-" * 40)
            
    #         for _, row in quarter_data.head(10).iterrows():
    #             district_full_name = f"{row['縣市']}-{row['行政區']}"
    #             print(f"{row['排名']:<4} {district_full_name:<20} {row['累積去化率']:>8.1f}%")

    # def plot_quarterly_trends(self):
    #     """繪製季度趨勢圖表"""
    #     if not hasattr(self, 'quarterly_analysis') or self.quarterly_analysis is None:
    #         print("正在進行季度分析...")
    #         self.analyze_quarterly_trends()
    #         if self.quarterly_analysis is None:
    #             print("❌ 無法進行季度分析")
    #             return
        
    #     try:
    #         import matplotlib.pyplot as plt
            
    #         # 全市場季度趨勢
    #         quarterly_summary = self.quarterly_analysis.groupby('交易年季').agg({
    #             '季度銷售戶數': 'sum',
    #             '累積去化率': 'mean'
    #         }).reset_index()
            
    #         fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
    #         # 季度銷售戶數趨勢
    #         quarters_str = [str(q) for q in quarterly_summary['交易年季']]
    #         ax1.plot(quarters_str, quarterly_summary['季度銷售戶數'], marker='o', linewidth=2, color='blue')
    #         ax1.set_title('季度銷售戶數趨勢', fontsize=14, fontweight='bold')
    #         ax1.set_ylabel('銷售戶數')
    #         ax1.grid(True, alpha=0.3)
    #         ax1.tick_params(axis='x', rotation=45)
            
    #         # 平均累積去化率趨勢
    #         ax2.plot(quarters_str, quarterly_summary['累積去化率'], marker='s', linewidth=2, color='red')
    #         ax2.set_title('平均累積去化率趨勢', fontsize=14, fontweight='bold')
    #         ax2.set_ylabel('累積去化率 (%)')
    #         ax2.set_xlabel('季度')
    #         ax2.grid(True, alpha=0.3)
    #         ax2.tick_params(axis='x', rotation=45)
            
    #         plt.tight_layout()
    #         plt.show()
            
    #     except ImportError:
    #         print("❌ 無法載入 matplotlib，請安裝該套件以顯示圖表")
    #     except Exception as e:
    #         print(f"❌ 圖表生成失敗: {str(e)}")


    # ====== 互動式方法 ======
    def show_available_cities(self):
        """顯示可用的縣市清單"""
        if self.analysis_result is None:
            print("請先執行基本分析")
            return []
        
        available_cities = sorted(self.analysis_result['縣市'].unique())
        print("\n" + "="*50)
        print("📍 可用縣市清單")
        print("="*50)
        
        for i, city in enumerate(available_cities, 1):
            community_count = len(self.analysis_result[self.analysis_result['縣市'] == city])
            units_count = self.analysis_result[self.analysis_result['縣市'] == city]['戶數'].sum()
            print(f"{i:2d}. {city:<8} (社區: {community_count:3d}個, 戶數: {units_count:,}戶)")
        
        return available_cities

    def interactive_city_analysis(self):
        """互動式縣市分析選單"""
        if self.analysis_result is None:
            print("請先執行基本分析")
            return
        
        while True:
            print("\n" + "="*60)
            print("🎯 縣市分析選單")
            print("="*60)
            print("1. 📊 檢視所有縣市概況")
            print("2. 🏙️ 選擇特定縣市詳細分析")
            print("3. 📋 顯示可用縣市清單")
            print("4. 🔍 批次分析多個縣市")
            print("5. 📈 縣市排名比較")
            print("6. 📊 季度去化表現分析")
            print("0. ❌ 返回主選單")
            print("-" * 60)
            
            try:
                choice = input("請選擇功能 (0-5): ").strip()
                
                if choice == "0":
                    print("返回主選單...")
                    break
                elif choice == "1":
                    self.show_all_cities_overview()
                elif choice == "2":
                    self.select_single_city_analysis()
                elif choice == "3":
                    self.show_available_cities()
                elif choice == "4":
                    self.batch_city_analysis()
                elif choice == "5":
                    self.show_city_ranking_comparison()
                elif choice == "6":
                    self.show_quarterly_performance_menu()
                else:
                    print("❌ 無效的選擇，請輸入 0-5 之間的數字")
                    
            except KeyboardInterrupt:
                print("\n程式中斷，返回主選單...")
                break
            except Exception as e:
                print(f"❌ 發生錯誤: {str(e)}")

    def show_all_cities_overview(self):
        """顯示所有縣市概況"""
        if not hasattr(self, 'city_analysis'):
            print("正在進行縣市分析...")
            self.analyze_city_performance()
        
        print("\n" + "="*70)
        print("🏙️ 全縣市市場概況")
        print("="*70)
        
        print(f"{'排名':<4} {'縣市':<8} {'去化率':<8} {'社區數':<6} {'戶數':<10} {'熱度':<8}")
        print("-" * 70)
        
        for idx, row in self.city_analysis.iterrows():
            rank = idx + 1
            print(f"{rank:<4} {row['縣市']:<8} {row['整體去化率']:<8.1f}% "
                f"{row['社區數量']:<6} {row['戶數']:<10,} {row['市場熱度']:<8}")

    def select_single_city_analysis(self):
        """選擇單一縣市進行詳細分析"""
        available_cities = self.show_available_cities()
        
        if not available_cities:
            return
        
        print(f"\n請選擇要分析的縣市:")
        print("輸入縣市編號 (1-{}) 或直接輸入縣市名稱".format(len(available_cities)))
        print("輸入 'q' 或 '0' 返回上層選單")
        
        while True:
            try:
                user_input = input("\n您的選擇: ").strip()
                
                if user_input.lower() in ['q', '0', 'quit', 'exit']:
                    break
                
                if user_input.isdigit():
                    city_index = int(user_input) - 1
                    if 0 <= city_index < len(available_cities):
                        selected_city = available_cities[city_index]
                    else:
                        print(f"❌ 請輸入 1-{len(available_cities)} 之間的數字")
                        continue
                else:
                    if user_input in available_cities:
                        selected_city = user_input
                    else:
                        matches = [city for city in available_cities if user_input in city]
                        if len(matches) == 1:
                            selected_city = matches[0]
                            print(f"找到匹配縣市: {selected_city}")
                        elif len(matches) > 1:
                            print(f"找到多個匹配縣市: {', '.join(matches)}")
                            print("請輸入更精確的縣市名稱")
                            continue
                        else:
                            print(f"❌ 找不到縣市 '{user_input}'")
                            print("請檢查輸入的縣市名稱是否正確")
                            continue
                
                print(f"\n🎯 開始分析 {selected_city}...")
                self.analyze_single_city(selected_city)
                
                while True:
                    continue_choice = input(f"\n是否要分析其他縣市? (y/n): ").strip().lower()
                    if continue_choice in ['y', 'yes', '是', '1']:
                        break
                    elif continue_choice in ['n', 'no', '否', '0']:
                        return
                    else:
                        print("請輸入 y 或 n")
                        
            except KeyboardInterrupt:
                print("\n分析中斷")
                break
            except Exception as e:
                print(f"❌ 分析過程中發生錯誤: {str(e)}")

    def batch_city_analysis(self):
        """批次分析多個縣市"""
        available_cities = self.show_available_cities()
        
        if not available_cities:
            return
        
        print(f"\n📝 批次分析模式")
        print("請輸入要分析的縣市編號，用逗號分隔 (例如: 1,3,5)")
        print("或輸入縣市名稱，用逗號分隔 (例如: 台北市,新北市,桃園市)")
        print("輸入 'all' 分析所有縣市")
        print("輸入 'q' 返回上層選單")
        
        try:
            user_input = input("\n您的選擇: ").strip()
            
            if user_input.lower() in ['q', 'quit', 'exit']:
                return
            
            selected_cities = []
            
            if user_input.lower() == 'all':
                selected_cities = available_cities
                print(f"將分析所有 {len(selected_cities)} 個縣市")
            else:
                inputs = [item.strip() for item in user_input.split(',')]
                
                for inp in inputs:
                    if inp.isdigit():
                        city_index = int(inp) - 1
                        if 0 <= city_index < len(available_cities):
                            selected_cities.append(available_cities[city_index])
                        else:
                            print(f"⚠️ 編號 {inp} 超出範圍，已跳過")
                    else:
                        if inp in available_cities:
                            selected_cities.append(inp)
                        else:
                            print(f"⚠️ 找不到縣市 '{inp}'，已跳過")
            
            if not selected_cities:
                print("❌ 沒有選擇有效的縣市")
                return
            
            print(f"\n確認要分析以下 {len(selected_cities)} 個縣市:")
            for i, city in enumerate(selected_cities, 1):
                print(f"  {i}. {city}")
            
            confirm = input(f"\n確認開始批次分析? (y/n): ").strip().lower()
            if confirm not in ['y', 'yes', '是', '1']:
                print("已取消批次分析")
                return
            
            print(f"\n🚀 開始批次分析 {len(selected_cities)} 個縣市...")
            
            for i, city in enumerate(selected_cities, 1):
                print(f"\n{'='*60}")
                print(f"📊 [{i}/{len(selected_cities)}] 分析 {city}")
                print(f"{'='*60}")
                
                try:
                    self.analyze_single_city(city)
                except Exception as e:
                    print(f"❌ {city} 分析失敗: {str(e)}")
                    continue
            
            print(f"\n✅ 批次分析完成！共分析了 {len(selected_cities)} 個縣市")
            
        except KeyboardInterrupt:
            print("\n批次分析中斷")
        except Exception as e:
            print(f"❌ 批次分析過程中發生錯誤: {str(e)}")

    def show_city_ranking_comparison(self):
        """顯示縣市排名比較"""
        if not hasattr(self, 'city_analysis'):
            print("正在進行縣市分析...")
            self.analyze_city_performance()
        
        print("\n" + "="*80)
        print("🏆 縣市排名比較分析")
        print("="*80)
        
        rankings = {
            '去化率': self.city_analysis.sort_values('整體去化率', ascending=False),
            '總戶數': self.city_analysis.sort_values('戶數', ascending=False),
            '社區數': self.city_analysis.sort_values('社區數量', ascending=False),
            '月均去化率': self.city_analysis.sort_values('月均去化率', ascending=False)
        }
        
        print(f"\n{'排名':<4} ", end="")
        for metric in rankings.keys():
            print(f"{metric:<12}", end="")
        print()
        print("-" * 80)
        
        for rank in range(min(10, len(self.city_analysis))):
            print(f"{rank+1:<4} ", end="")
            for metric, data in rankings.items():
                city = data.iloc[rank]['縣市']
                print(f"{city:<12}", end="")
            print()

# ====== 主程式 (在類別外部) ======
def main():
    """主要執行函數"""
    try:
        print("正在初始化分析程式...")
        analyzer = PresaleMarketAnalysis()

        input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\pre_sale_data\processed"
        fn = "預售屋買賣主檔_11006_11406.csv"
        pre_sale_input_path = os.path.join(input_dir, fn)

        input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\sale_data\processed"
        fn = "預售屋備查.csv"
        sale_data_input_path = os.path.join(input_dir, fn)
        
        analyzer.load_data(pre_sale_input_path, sale_data_input_path)
        
        result = analyzer.preprocess_data()
        if result is None:
            print("❌ 由於編號比對失敗，程式終止執行")
            return
        
        print("\n" + "="*60)
        print("執行基本去化率分析...")
        print("="*60)
        analyzer.calculate_time_adjusted_absorption_rate()
        
        analyzer.analyze_city_performance()
        analyzer.analyze_district_performance()
        
        while True:
            print("\n" + "="*70)
            print("🏠 預售屋市場去化率分析系統")
            print("="*70)
            print("1. 📊 查看整體市場報告")
            print("2. 🏙️ 縣市分析選單")
            print("3. 🏘️ 行政區分析報告")
            print("4. 📈 市場效率分析")
            print("5. 📋 生成視覺化圖表")
            print("6. 💾 匯出分析結果")
            print("7. 🔄 執行完整分析 (所有縣市)")
            print("0. 🚪 結束程式")
            print("-" * 70)
            
            try:
                main_choice = input("請選擇功能 (0-7): ").strip()
                
                if main_choice == "0":
                    print("👋 感謝使用預售屋市場分析系統！")
                    break
                elif main_choice == "1":
                    analyzer.generate_time_aware_report()
                elif main_choice == "2":
                    analyzer.interactive_city_analysis()
                elif main_choice == "3":
                    analyzer.generate_district_city_report()
                elif main_choice == "4":
                    analyzer.analyze_market_efficiency()
                elif main_choice == "5":
                    print("\n正在生成視覺化圖表...")
                    try:
                        analyzer.create_time_aware_visualizations()
                        analyzer.create_district_heatmap()
                    except Exception as e:
                        print(f"⚠️ 圖表生成失敗: {str(e)}")
                elif main_choice == "6":
                    analyzer.export_time_aware_results("presale_analysis_results", "csv")
                elif main_choice == "7":
                    print("\n🚀 執行完整分析...")
                    analyzer.analyze_city_detailed()
                    analyzer.generate_city_detailed_report()
                    
                    print("\n正在生成視覺化圖表...")
                    try:
                        analyzer.create_time_aware_visualizations()
                        analyzer.create_district_heatmap()
                        
                        if hasattr(analyzer, 'city_analysis'):
                            top_cities = analyzer.city_analysis.head(3)['縣市'].tolist()
                            for city in top_cities:
                                print(f"正在生成 {city} 詳細視覺化圖表...")
                                analyzer.create_city_visualizations(city)
                    except Exception as e:
                        print(f"⚠️ 圖表生成失敗: {str(e)}")
                    
                    analyzer.export_time_aware_results("presale_analysis_results", "csv")
                    
                    print("\n✅ 完整分析執行完成！")
                else:
                    print("❌ 無效的選擇，請輸入 0-7 之間的數字")
                    
            except KeyboardInterrupt:
                print("\n程式中斷，正在退出...")
                break
            except Exception as e:
                print(f"❌ 程式執行錯誤: {str(e)}")
                print("請檢查資料格式或聯繫開發者")
                import traceback
                traceback.print_exc()
        
    except FileNotFoundError as e:
        print(f"❌ 檔案載入錯誤: {str(e)}")
        print("請確認 CSV 檔案存在於正確路徑")
    except Exception as e:
        print(f"❌ 程式初始化錯誤: {str(e)}")
        print("請檢查資料格式或聯繫開發者")
        import traceback
        traceback.print_exc()

# if __name__ == "__main__":
#     main()

if __name__ == "__main__":
    # 測試方法是否存在
    test_analyzer = PresaleMarketAnalysis()
    print("🔍 檢查方法存在性:")
    print(f"interactive_city_analysis: {hasattr(test_analyzer, 'interactive_city_analysis')}")
    print(f"show_available_cities: {hasattr(test_analyzer, 'show_available_cities')}")
    
    if hasattr(test_analyzer, 'interactive_city_analysis'):
        print("✅ 方法已正確添加，啟動主程式...")
        main()
    else:
        print("❌ 方法尚未正確添加到類別中")
        print("請檢查縮排是否正確（應該是4個空格）")