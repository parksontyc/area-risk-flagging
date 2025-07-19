"""
預售屋市場去化率分析程式 (含縣市行政區深度分析)
考慮銷售時間因素的進階分析

功能特色：
✅ 基於備查編號精確比對
✅ 時間調整去化率計算  
✅ 縣市市場熱度分析
✅ 行政區表現等級評估
✅ 詳細縣市分析報告
✅ 行政區季度排名趨勢
✅ 各行政區社區去化高低排名
✅ 多維度視覺化圖表
✅ 行政區去化率熱力圖
✅ 縣市專屬視覺化圖表

分析層級：
📊 市場總覽 → 縣市分析 → 行政區分析 → 社區排名
🔍 包含季度趨勢、表現排名、投資建議

必要套件：
pip install pandas numpy matplotlib seaborn

可選套件（匯出Excel格式）：
pip install openpyxl

使用方法：
python presale_analysis.py
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
project_root = Path.cwd().parent  # 找出根目錄：Path.cwd()找出現在所在目錄(/run).parent(上一層是notebook).parent(再上層一層business_district_discovery)
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
        # 允許使用者指定分析基準日期，預設為 2024年底
        if analysis_date:
            self.current_date = analysis_date
        else:
            self.current_date = datetime(2024, 12, 31)  # 使用固定日期避免時間變動影響
        print(f"分析基準日期設定為: {self.current_date.strftime('%Y-%m-%d')}")
        
    def load_data(self, transaction_file, community_file):
        """載入交易資料和社區資料"""
        print("正在載入資料...")
        print("📋 分析策略：僅分析在兩個檔案中都有對應編號的社區")
        print("   • 交易檔案的 '備查編號' 欄位")
        print("   • 社區檔案的 '編號' 欄位")
        print("   • 確保資料完整性和分析準確性")
        
        # 載入預售屋交易資料
        self.transaction_data = pd.read_csv(transaction_file)
        print(f"✅ 交易資料載入完成: {len(self.transaction_data)} 筆記錄")
        
        # 載入社區預售備查資料
        self.community_data = pd.read_csv(community_file)
        print(f"✅ 社區資料載入完成: {len(self.community_data)} 個社區")
        
        return self
    
    def parse_date_columns(self):
        """解析日期相關欄位"""
        print("正在解析日期資料...")
        
        # 處理社區資料的日期欄位
        date_columns = ['銷售起始時間', '自售起始時間', '代銷起始時間', '備查完成日期', '建照核發日']
        
        for col in date_columns:
            if col in self.community_data.columns:
                # 將數值型日期轉換為datetime (假設是民國年格式)
                self.community_data[col] = pd.to_numeric(self.community_data[col], errors='coerce')
                # 處理民國年轉西元年 (例如: 1120101 -> 2023-01-01)
                self.community_data[f'{col}_date'] = self.community_data[col].apply(self.convert_taiwan_date)
        
        # 處理交易資料的日期
        if '交易年月' in self.transaction_data.columns:
            self.transaction_data['交易年月'] = pd.to_numeric(self.transaction_data['交易年月'], errors='coerce')
            self.transaction_data['交易日期_parsed'] = self.transaction_data['交易年月'].apply(self.convert_taiwan_yearmonth)
        
        print("日期解析完成")
        return self
    # 請將以下方法添加到您的 PresaleMarketAnalysis 類別中

    def analyze_quarterly_trends(self):
        """分析季度趨勢"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return None
        
        print("正在分析季度趨勢...")
        
        # 處理交易年季資料
        if '交易年季' not in self.transaction_data.columns:
            print("⚠️ 交易資料中缺少 '交易年季' 欄位，無法進行季度分析")
            return None
        
        # 清理交易年季資料
        transaction_quarterly = self.transaction_data.dropna(subset=['交易年季', '備查編號'])
        
        # 計算每季每個社區的銷售戶數
        quarterly_sales = transaction_quarterly.groupby(['備查編號', '交易年季']).size().reset_index(name='季度銷售戶數')
        
        # 與社區基本資料合併
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
        
        # 計算累積銷售和去化率
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
            
            # 篩選該縣市的資料
            city_data = self.analysis_result[self.analysis_result['縣市'] == city].copy()
            city_quarterly = None
            if hasattr(self, 'quarterly_analysis') and self.quarterly_analysis is not None:
                city_quarterly = self.quarterly_analysis[self.quarterly_analysis['縣市'] == city].copy()
            
            # 行政區層級分析
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
            
            # 各行政區的季度排名（如果有季度資料）
            quarterly_district_ranking = None
            if city_quarterly is not None and len(city_quarterly) > 0:
                # 計算各季各行政區的平均去化率
                quarterly_district = city_quarterly.groupby(['交易年季', '行政區'])['累積去化率'].mean().reset_index()
                quarterly_district_ranking = quarterly_district.pivot(index='行政區', columns='交易年季', values='累積去化率')
                
                # 為每季添加排名
                for quarter in quarterly_district_ranking.columns:
                    quarterly_district_ranking[f'{quarter}_排名'] = quarterly_district_ranking[quarter].rank(ascending=False)
            
            # 各行政區內社區去化率排名
            district_community_ranking = {}
            for district in city_data['行政區'].unique():
                if pd.isna(district):
                    continue
                    
                district_communities = city_data[city_data['行政區'] == district].copy()
                district_communities = district_communities.sort_values('去化率', ascending=False)
                
                # 去化高的社區（前5名或去化率>60%）
                high_performing = district_communities[
                    (district_communities['去化率'] > 60) | 
                    (district_communities.index.isin(district_communities.head(5).index))
                ]
                
                # 去化低的社區（後5名或去化率<30%）
                low_performing = district_communities[
                    (district_communities['去化率'] < 30) | 
                    (district_communities.index.isin(district_communities.tail(5).index))
                ].sort_values('去化率')
                
                district_community_ranking[district] = {
                    'high_performing': high_performing,
                    'low_performing': low_performing,
                    'total_communities': len(district_communities)
                }
            
            # 儲存該縣市的分析結果
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
            
            # 基本概況
            stats = analysis['basic_stats']
            print(f"\n📊 {city} 市場基本概況")
            print(f"行政區數量: {stats['districts_count']} 個")
            print(f"社區總數: {stats['total_communities']} 個")
            print(f"總戶數: {stats['total_units']:,} 戶")
            print(f"已售戶數: {stats['sold_units']:,} 戶")
            print(f"整體去化率: {stats['overall_absorption_rate']:.2f}%")
            print(f"平均月去化率: {stats['avg_monthly_rate']:.2f}%")
            
            # 行政區排名
            district_analysis = analysis['district_analysis']
            print(f"\n🏘️ {city} 行政區去化率排名")
            print(f"{'排名':<4} {'行政區':<12} {'整體去化率':<10} {'社區數':<6} {'總戶數':<8} {'已售戶數':<8} {'月均去化率':<10}")
            print("-" * 75)
            
            for idx, row in district_analysis.iterrows():
                rank = district_analysis.index.get_loc(idx) + 1
                print(f"{rank:<4} {row['行政區']:<12} {row['整體去化率']:<10.1f}% "
                    f"{row['社區數量']:<6} {row['戶數']:<8,} {row['已售戶數']:<8,} "
                    f"{row['月均去化率']:<10.2f}%")
            
            # 季度行政區排名趨勢（如果有資料）
            if analysis['quarterly_district_ranking'] is not None and not analysis['quarterly_district_ranking'].empty:
                print(f"\n📈 {city} 行政區季度去化率趨勢")
                quarterly_data = analysis['quarterly_district_ranking']
                
                # 顯示最近幾季的資料
                quarters = [col for col in quarterly_data.columns if not col.endswith('_排名')]
                quarters = sorted(quarters)[-4:]  # 最近4季
                
                print(f"{'行政區':<12}", end="")
                for quarter in quarters:
                    print(f"{quarter:<12}", end="")
                print()
                print("-" * (12 + 12 * len(quarters)))
                
                for district in quarterly_data.index:
                    print(f"{district:<12}", end="")
                    for quarter in quarters:
                        rate = quarterly_data.loc[district, quarter]
                        if pd.notna(rate):
                            print(f"{rate:<12.1f}%", end="")
                        else:
                            print(f"{'--':<12}", end="")
                    print()
            
            # 各行政區社區排名
            print(f"\n🎯 {city} 各行政區社區去化表現")
            community_ranking = analysis['district_community_ranking']
            
            for district, data in community_ranking.items():
                print(f"\n📍 {district} (共{data['total_communities']}個社區)")
                
                # 高表現社區
                if len(data['high_performing']) > 0:
                    print(f"   🔥 去化表現優異社區:")
                    for _, community in data['high_performing'].head(5).iterrows():
                        print(f"      [{community['編號']}] {community['社區名稱']}: "
                            f"{community['去化率']:.1f}% (月均{community['月均去化率']:.2f}%, "
                            f"{community['已售戶數']}/{community['戶數']}戶)")
                
                # 低表現社區
                if len(data['low_performing']) > 0:
                    print(f"   ⚠️ 需要關注社區:")
                    for _, community in data['low_performing'].head(5).iterrows():
                        print(f"      [{community['編號']}] {community['社區名稱']}: "
                            f"{community['去化率']:.1f}% (月均{community['月均去化率']:.2f}%, "
                            f"{community['已售戶數']}/{community['戶數']}戶, "
                            f"銷售{community['銷售天數']}天)")
            
            # 該縣市的市場建議
            print(f"\n💡 {city} 市場分析建議")
            
            # 找出表現最好和最差的行政區
            best_district = district_analysis.iloc[0]['行政區']
            worst_district = district_analysis.iloc[-1]['行政區']
            best_rate = district_analysis.iloc[0]['整體去化率']
            worst_rate = district_analysis.iloc[-1]['整體去化率']
            
            print(f"🌟 表現最佳行政區: {best_district} ({best_rate:.1f}%)")
            print(f"⚡ 需要加強行政區: {worst_district} ({worst_rate:.1f}%)")
            
            # 根據整體表現給建議
            overall_rate = stats['overall_absorption_rate']
            if overall_rate > 70:
                print(f"✅ {city}市場表現優異，建議：")
                print(f"   • 可考慮在表現優異的行政區增加推案")
                print(f"   • 關注高去化社區的成功經驗並複製")
            elif overall_rate > 50:
                print(f"🔔 {city}市場表現穩健，建議：")
                print(f"   • 加強去化緩慢社區的銷售策略")
                print(f"   • 參考表現優異區域的銷售模式")
            else:
                print(f"⚠️ {city}市場需要重點關注，建議：")
                print(f"   • 重新評估定價策略和產品定位")
                print(f"   • 考慮調整銷售通路或行銷策略")
                print(f"   • 密切監控市場變化和競爭動態")

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
            
            # 1. 行政區去化率排名
            axes[0,0].barh(district_analysis['行政區'], district_analysis['整體去化率'], 
                        color='skyblue')
            axes[0,0].set_title(f'{city_name} 行政區去化率排名')
            axes[0,0].set_xlabel('整體去化率 (%)')
            
            # 添加數值標籤
            for i, v in enumerate(district_analysis['整體去化率']):
                axes[0,0].text(v + 1, i, f'{v:.1f}%', va='center')
            
            # 2. 行政區社區數量分布
            axes[0,1].pie(district_analysis['社區數量'], labels=district_analysis['行政區'], 
                        autopct='%1.1f%%', startangle=90)
            axes[0,1].set_title(f'{city_name} 各行政區社區數量分布')
            
            # 3. 行政區戶數vs去化率散點圖
            scatter = axes[1,0].scatter(district_analysis['戶數'], district_analysis['整體去化率'], 
                                    s=district_analysis['社區數量']*20, alpha=0.6, 
                                    c=district_analysis['月均去化率'], cmap='viridis')
            axes[1,0].set_title(f'{city_name} 行政區戶數 vs 去化率')
            axes[1,0].set_xlabel('總戶數')
            axes[1,0].set_ylabel('整體去化率 (%)')
            
            # 添加行政區標籤
            for i, row in district_analysis.iterrows():
                axes[1,0].annotate(row['行政區'], (row['戶數'], row['整體去化率']), 
                                xytext=(5, 5), textcoords='offset points', fontsize=8)
            
            try:
                plt.colorbar(scatter, ax=axes[1,0], label='月均去化率 (%)')
            except:
                pass
            
            # 4. 季度趨勢圖（如果有資料）
            if analysis['quarterly_district_ranking'] is not None and not analysis['quarterly_district_ranking'].empty:
                quarterly_data = analysis['quarterly_district_ranking']
                quarters = [col for col in quarterly_data.columns if not col.endswith('_排名')]
                quarters = sorted(quarters)
                
                for district in quarterly_data.index[:5]:  # 只顯示前5個行政區
                    values = [quarterly_data.loc[district, q] if pd.notna(quarterly_data.loc[district, q]) else 0 
                            for q in quarters]
                    axes[1,1].plot(quarters, values, marker='o', label=district, linewidth=2)
                
                axes[1,1].set_title(f'{city_name} 行政區季度去化率趨勢')
                axes[1,1].set_xlabel('季度')
                axes[1,1].set_ylabel('累積去化率 (%)')
                axes[1,1].legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                axes[1,1].tick_params(axis='x', rotation=45)
            else:
                axes[1,1].text(0.5, 0.5, '無季度資料', ha='center', va='center', 
                            transform=axes[1,1].transAxes, fontsize=14)
                axes[1,1].set_title(f'{city_name} 季度趨勢（無資料）')
            
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"⚠️ {city_name} 視覺化圖表生成失敗: {str(e)}")

    def export_city_detailed_results(self, filename="city_detailed_analysis", format_type="csv"):
        """匯出詳細縣市分析結果"""
        if not hasattr(self, 'city_detailed_analysis'):
            print("請先執行詳細縣市分析")
            return
        
        if format_type.lower() == "excel":
            try:
                filename_excel = f"{filename}.xlsx"
                with pd.ExcelWriter(filename_excel, engine='openpyxl') as writer:
                    
                    # 匯出每個縣市的行政區分析
                    for city, analysis in self.city_detailed_analysis.items():
                        # 行政區分析
                        district_df = analysis['district_analysis'].copy()
                        district_df['縣市'] = city
                        district_df = district_df[['縣市'] + [col for col in district_df.columns if col != '縣市']]
                        district_df.to_excel(writer, sheet_name=f'{city}_行政區分析', index=False)
                        
                        # 季度趨勢（如果有）
                        if analysis['quarterly_district_ranking'] is not None:
                            quarterly_df = analysis['quarterly_district_ranking'].copy()
                            quarterly_df.index.name = '行政區'
                            quarterly_df.to_excel(writer, sheet_name=f'{city}_季度趨勢')
                        
                        # 社區排名匯總
                        community_summary = []
                        for district, data in analysis['district_community_ranking'].items():
                            # 高表現社區
                            for _, community in data['high_performing'].iterrows():
                                community_summary.append({
                                    '縣市': city,
                                    '行政區': district,
                                    '編號': community['編號'],
                                    '社區名稱': community['社區名稱'],
                                    '去化率': community['去化率'],
                                    '月均去化率': community['月均去化率'],
                                    '已售戶數': community['已售戶數'],
                                    '總戶數': community['戶數'],
                                    '表現類別': '優異'
                                })
                            
                            # 低表現社區
                            for _, community in data['low_performing'].iterrows():
                                community_summary.append({
                                    '縣市': city,
                                    '行政區': district,
                                    '編號': community['編號'],
                                    '社區名稱': community['社區名稱'],
                                    '去化率': community['去化率'],
                                    '月均去化率': community['月均去化率'],
                                    '已售戶數': community['已售戶數'],
                                    '總戶數': community['戶數'],
                                    '表現類別': '需關注'
                                })
                        
                        if community_summary:
                            community_df = pd.DataFrame(community_summary)
                            community_df.to_excel(writer, sheet_name=f'{city}_社區排名', index=False)
                
                print(f"詳細縣市分析結果已匯出至: {filename_excel}")
                
            except ImportError:
                print("❌ 缺少 openpyxl 套件，無法匯出 Excel 格式")
                print("請執行: pip install openpyxl")
                print("改為匯出 CSV 格式...")
                format_type = "csv"
        
        if format_type.lower() == "csv":
            # 匯出為多個 CSV 檔案
            files_exported = []
            
            for city, analysis in self.city_detailed_analysis.items():
                # 行政區分析
                district_df = analysis['district_analysis'].copy()
                district_df['縣市'] = city
                district_filename = f"{filename}_{city}_行政區分析.csv"
                district_df.to_csv(district_filename, index=False, encoding='utf-8-sig')
                files_exported.append(district_filename)
                
                # 季度趨勢
                if analysis['quarterly_district_ranking'] is not None:
                    quarterly_filename = f"{filename}_{city}_季度趨勢.csv"
                    analysis['quarterly_district_ranking'].to_csv(quarterly_filename, encoding='utf-8-sig')
                    files_exported.append(quarterly_filename)
                
                # 社區排名
                community_summary = []
                for district, data in analysis['district_community_ranking'].items():
                    for _, community in data['high_performing'].iterrows():
                        community_summary.append({
                            '縣市': city, '行政區': district, '編號': community['編號'],
                            '社區名稱': community['社區名稱'], '去化率': community['去化率'],
                            '月均去化率': community['月均去化率'], '已售戶數': community['已售戶數'],
                            '總戶數': community['戶數'], '表現類別': '優異'
                        })
                    
                    for _, community in data['low_performing'].iterrows():
                        community_summary.append({
                            '縣市': city, '行政區': district, '編號': community['編號'],
                            '社區名稱': community['社區名稱'], '去化率': community['去化率'],
                            '月均去化率': community['月均去化率'], '已售戶數': community['已售戶數'],
                            '總戶數': community['戶數'], '表現類別': '需關注'
                        })
                
                if community_summary:
                    community_df = pd.DataFrame(community_summary)
                    community_filename = f"{filename}_{city}_社區排名.csv"
                    community_df.to_csv(community_filename, index=False, encoding='utf-8-sig')
                    files_exported.append(community_filename)
            
            print(f"詳細縣市分析結果已匯出為 CSV 格式:")
            for file in files_exported:
                print(f"  - {file}")

    def analyze_single_city(self, city_name):
        """分析單一縣市的詳細報告"""
        if self.analysis_result is None:
            print("請先執行基本去化率計算")
            return
        
        # 檢查縣市是否存在
        available_cities = self.analysis_result['縣市'].unique()
        if city_name not in available_cities:
            print(f"❌ 找不到縣市 '{city_name}'")
            print(f"可用的縣市: {', '.join(available_cities)}")
            return
        
        print(f"🎯 開始分析 {city_name}...")
        
        # 執行季度分析（如果還沒做過）
        if not hasattr(self, 'quarterly_analysis'):
            self.analyze_quarterly_trends()
        
        # 執行該縣市的詳細分析
        self.analyze_city_detailed(city_name)
        
        # 生成詳細報告
        self.generate_city_detailed_report(city_name)
        
        # 創建視覺化
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
        
        # 分析編號比對情況
        transaction_with_id = self.transaction_data.dropna(subset=['備查編號'])
        community_with_id = self.community_data.dropna(subset=['編號'])
        
        transaction_ids = set(transaction_with_id['備查編號'].astype(str))
        community_ids = set(community_with_id['編號'].astype(str))
        matched_ids = transaction_ids.intersection(community_ids)
        
        # 只在交易資料中有的編號
        only_in_transaction = transaction_ids - community_ids
        # 只在社區資料中有的編號
        only_in_community = community_ids - transaction_ids
        
        print(f"📊 編號比對統計：")
        print(f"交易資料編號總數: {len(transaction_ids)}")
        print(f"社區資料編號總數: {len(community_ids)}")
        print(f"✅ 成功比對編號數: {len(matched_ids)}")
        print(f"⚠️ 僅存在於交易資料: {len(only_in_transaction)}")
        print(f"⚠️ 僅存在於社區資料: {len(only_in_community)}")
        print(f"📈 比對成功率: {len(matched_ids)/max(len(transaction_ids), len(community_ids))*100:.1f}%")
        
        if len(matched_ids) == 0:
            print("\n❌ 沒有找到任何匹配的編號!")
            print("建議檢查：")
            print("1. 編號格式是否一致")
            print("2. 是否有前後空格或特殊字符")
            print("3. 數據類型是否相同")
            
            # 顯示範例編號供檢查
            print(f"\n範例交易資料編號: {list(transaction_ids)[:5]}")
            print(f"範例社區資料編號: {list(community_ids)[:5]}")
        
    def analyze_district_performance(self):
        """分析行政區去化表現"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return None
        
        print("正在進行行政區分析...")
        
        # 行政區層級統計
        district_stats = self.analysis_result.groupby(['縣市', '行政區']).agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean',
            '銷售天數': 'mean'
        }).reset_index()
        
        # 計算行政區整體去化率
        district_stats['整體去化率'] = (district_stats['已售戶數'] / district_stats['戶數']) * 100
        district_stats['整體去化率'] = district_stats['整體去化率'].round(2)
        
        # 計算社區數量
        district_community_count = self.analysis_result.groupby(['縣市', '行政區']).size().reset_index(name='社區數量')
        district_stats = pd.merge(district_stats, district_community_count, on=['縣市', '行政區'])
        
        # 計算行政區的去化表現分級
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
        
        # 排序
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
        
        # 縣市層級統計
        city_stats = self.analysis_result.groupby('縣市').agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean',
            '銷售天數': 'mean'
        }).reset_index()
        
        # 計算縣市整體去化率
        city_stats['整體去化率'] = (city_stats['已售戶數'] / city_stats['戶數']) * 100
        city_stats['整體去化率'] = city_stats['整體去化率'].round(2)
        
        # 計算行政區數量和社區數量
        city_district_count = self.analysis_result.groupby('縣市')['行政區'].nunique().reset_index()
        city_district_count.columns = ['縣市', '行政區數量']
        
        city_community_count = self.analysis_result.groupby('縣市').size().reset_index(name='社區數量')
        
        city_stats = pd.merge(city_stats, city_district_count, on='縣市')
        city_stats = pd.merge(city_stats, city_community_count, on='縣市')
        
        # 計算縣市的去化表現分級
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
        
        # 排序
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
            if len(date_str) == 7:  # 1120101 格式
                year = int(date_str[:3]) + 1911  # 民國年轉西元年
                month = int(date_str[3:5])
                day = int(date_str[5:7])
                return datetime(year, month, day)
            elif len(date_str) == 6:  # 112101 格式
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
        
        # 解析日期欄位
        self.parse_date_columns()
        
        # 處理交易資料
        self.transaction_data = self.transaction_data.dropna(subset=['社區名稱', '縣市', '行政區'])
        
        # 處理社區資料
        self.community_data = self.community_data.dropna(subset=['社區名稱', '戶數'])
        self.community_data['戶數'] = pd.to_numeric(self.community_data['戶數'], errors='coerce')
        self.community_data = self.community_data.dropna(subset=['戶數'])
        
        # 計算銷售期間
        self.calculate_sales_period()
        
        # 檢查編號比對情況
        matched_count = self.check_data_matching()
        if matched_count == 0:
            print("❌ 資料預處理中止：無法找到匹配的編號")
            return None
        
        print("✅ 數據預處理完成")
        return self
    
    def calculate_sales_period(self):
        """計算銷售期間相關指標"""
        # 確定銷售起始日期 (優先順序：銷售起始時間 > 自售起始時間 > 代銷起始時間)
        self.community_data['銷售開始日期'] = (
            self.community_data['銷售起始時間_date'].fillna(
                self.community_data['自售起始時間_date'].fillna(
                    self.community_data['代銷起始時間_date']
                )
            )
        )
        
        # 計算截至分析日期的銷售天數
        self.community_data['銷售天數'] = (
            self.current_date - self.community_data['銷售開始日期']
        ).dt.days
        
        # 處理負值或異常值
        self.community_data['銷售天數'] = self.community_data['銷售天數'].apply(
            lambda x: max(0, x) if pd.notna(x) else 0
        )
        
        # 銷售期間分類
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
        
        # 首先確保編號欄位存在
        if '備查編號' not in self.transaction_data.columns:
            print("❌ 交易資料中缺少 '備查編號' 欄位")
            return self
        
        if '編號' not in self.community_data.columns:
            print("❌ 社區資料中缺少 '編號' 欄位")
            return self
        
        # 先用編號比對，確保只分析有對應編號的社區
        print("正在進行編號比對...")
        
        # 清理編號資料（移除空值）
        transaction_with_id = self.transaction_data.dropna(subset=['備查編號'])
        community_with_id = self.community_data.dropna(subset=['編號'])
        
        # 取得有對應編號的社區清單
        transaction_ids = set(transaction_with_id['備查編號'].astype(str))
        community_ids = set(community_with_id['編號'].astype(str))
        matched_ids = transaction_ids.intersection(community_ids)
        
        print(f"交易資料中的備查編號數量: {len(transaction_ids)}")
        print(f"社區資料中的編號數量: {len(community_ids)}")
        print(f"成功比對的編號數量: {len(matched_ids)}")
        
        if len(matched_ids) == 0:
            print("❌ 沒有找到匹配的編號，請檢查編號格式是否一致")
            return self
        
        # 篩選出有匹配編號的資料
        filtered_transaction = transaction_with_id[
            transaction_with_id['備查編號'].astype(str).isin(matched_ids)
        ].copy()
        
        filtered_community = community_with_id[
            community_with_id['編號'].astype(str).isin(matched_ids)
        ].copy()
        
        # 計算每個編號的銷售戶數
        sold_units = filtered_transaction.groupby('備查編號').size().reset_index(name='已售戶數')
        sold_units['備查編號'] = sold_units['備查編號'].astype(str)
        
        # 準備社區資料進行合併
        community_for_merge = filtered_community.copy()
        community_for_merge['編號'] = community_for_merge['編號'].astype(str)
        
        # 使用編號進行精確合併
        merged_data = pd.merge(
            community_for_merge[['編號', '縣市', '行政區', '社區名稱', '戶數', '銷售開始日期', '銷售天數', '銷售階段']],
            sold_units,
            left_on='編號',
            right_on='備查編號',
            how='left'
        )
        # 填補未售出的社區
        merged_data['已售戶數'] = merged_data['已售戶數'].fillna(0)
        
        # 計算基本去化率
        merged_data['去化率'] = (merged_data['已售戶數'] / merged_data['戶數']) * 100
        merged_data['去化率'] = merged_data['去化率'].round(2)
        
        # 計算去化速度 (每月去化率)
        merged_data['銷售月數'] = merged_data['銷售天數'] / 30.44  # 平均每月天數
        merged_data['銷售月數'] = merged_data['銷售月數'].apply(lambda x: max(0.5, x))  # 最少0.5個月
        merged_data['月均去化率'] = merged_data['去化率'] / merged_data['銷售月數']
        merged_data['月均去化率'] = merged_data['月均去化率'].round(2)
        
        # 預估完全去化所需時間
        merged_data['預估完全去化月數'] = np.where(
            merged_data['月均去化率'] > 0,
            100 / merged_data['月均去化率'],
            float('inf')
        )
        
        # 分類去化表現 (考慮銷售期間)
        def categorize_performance(row):
            if row['銷售天數'] < 90:  # 新推案
                if row['去化率'] > 20:
                    return '新案熱銷'
                elif row['去化率'] > 10:
                    return '新案穩健'
                else:
                    return '新案待觀察'
            elif row['銷售天數'] < 365:  # 一年內
                if row['去化率'] > 70:
                    return '銷售優異'
                elif row['去化率'] > 40:
                    return '銷售良好'
                elif row['去化率'] > 20:
                    return '銷售普通'
                else:
                    return '銷售緩慢'
            else:  # 超過一年
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
        
        # 整體概況
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
        
        # 資料匹配狀況說明
        print(f"\n🔗 資料比對說明")
        print(f"本分析僅包含在以下兩個檔案中都有對應編號的社區：")
        print(f"  • 交易資料 (lvr_pre_sale_test.csv) - 備查編號欄位")
        print(f"  • 社區資料 (lvr_sale_data_test.csv) - 編號欄位")
        print(f"這確保了分析的準確性和資料完整性")
        
        # 銷售階段分析
        print(f"\n⏰ 不同銷售階段表現")
        stage_analysis = self.analysis_result.groupby('銷售階段').agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean'
        }).reset_index()
        stage_analysis['整體去化率'] = (stage_analysis['已售戶數'] / stage_analysis['戶數']) * 100
        
        for _, row in stage_analysis.iterrows():
            community_count = len(self.analysis_result[self.analysis_result['銷售階段']==row['銷售階段']])
            print(f"{row['銷售階段']}: 去化率 {row['整體去化率']:.1f}%, "
                  f"月均去化率 {row['月均去化率']:.2f}%, 社區數 {community_count} 個")
        
        # 銷售表現分布
        print(f"\n🎯 銷售表現分布")
        performance_dist = self.analysis_result['銷售表現'].value_counts()
        for performance, count in performance_dist.items():
            percentage = (count / total_communities) * 100
            print(f"{performance}: {count} 個社區 ({percentage:.1f}%)")
        
        # 高效去化社區 (月均去化率高)
        print(f"\n🚀 高效去化社區 (月均去化率 > 5%)")
        high_efficiency = self.analysis_result[self.analysis_result['月均去化率'] > 5].sort_values('月均去化率', ascending=False)
        if len(high_efficiency) > 0:
            for _, row in high_efficiency.head(10).iterrows():
                print(f"[{row['編號']}] {row['縣市']}-{row['行政區']}-{row['社區名稱']}: "
                      f"月均 {row['月均去化率']:.2f}%, 累計 {row['去化率']:.1f}%, "
                      f"銷售 {row['銷售天數']} 天")
        else:
            print("目前無月均去化率超過5%的社區")
        
        # 去化困難社區 (銷售時間長但去化率低)
        print(f"\n⚠️ 需關注社區 (銷售超過6個月且去化率<30%)")
        concern_communities = self.analysis_result[
            (self.analysis_result['銷售天數'] > 180) & 
            (self.analysis_result['去化率'] < 30)
        ].sort_values('銷售天數', ascending=False)
        
        if len(concern_communities) > 0:
            for _, row in concern_communities.head(10).iterrows():
                print(f"[{row['編號']}] {row['縣市']}-{row['行政區']}-{row['社區名稱']}: "
                      f"去化率 {row['去化率']:.1f}%, 銷售 {row['銷售天數']} 天, "
                      f"戶數 {row['戶數']} 戶")
        else:
            print("目前無需特別關注的社區")
    
    def generate_district_city_report(self):
        """生成縣市行政區分析報告"""
        if not hasattr(self, 'district_analysis') or not hasattr(self, 'city_analysis'):
            print("請先執行縣市和行政區分析")
            return
        
        print("\n" + "="*60)
        print("縣市與行政區去化率深度分析")
        print("="*60)
        
        # 縣市表現分析
        print(f"\n🏙️ 縣市市場表現排名")
        print(f"{'排名':<4} {'縣市':<8} {'整體去化率':<10} {'總戶數':<8} {'已售戶數':<8} {'社區數':<6} {'行政區數':<6} {'市場熱度':<8}")
        print("-" * 70)
        
        for idx, row in self.city_analysis.head(15).iterrows():
            print(f"{idx+1:<4} {row['縣市']:<8} {row['整體去化率']:<10.1f}% "
                  f"{row['戶數']:<8,} {row['已售戶數']:<8,} {row['社區數量']:<6} "
                  f"{row['行政區數量']:<6} {row['市場熱度']:<8}")
        
        # 市場熱度分布
        print(f"\n🌡️ 市場熱度分布")
        heat_dist = self.city_analysis['市場熱度'].value_counts()
        total_cities = len(self.city_analysis)
        for heat, count in heat_dist.items():
            percentage = (count / total_cities) * 100
            print(f"{heat}: {count} 個縣市 ({percentage:.1f}%)")
        
        # 行政區表現分析 - 只顯示有足夠樣本的區域
        print(f"\n🏘️ 行政區表現排名 (社區數≥3)")
        qualified_districts = self.district_analysis[self.district_analysis['社區數量'] >= 3]
        
        print(f"{'排名':<4} {'縣市-行政區':<20} {'整體去化率':<10} {'社區數':<6} {'總戶數':<8} {'表現等級':<8}")
        print("-" * 70)
        
        for idx, row in qualified_districts.head(20).iterrows():
            district_name = f"{row['縣市']}-{row['行政區']}"
            print(f"{idx+1:<4} {district_name:<20} {row['整體去化率']:<10.1f}% "
                  f"{row['社區數量']:<6} {row['戶數']:<8,} {row['表現等級']:<8}")
        
        # 表現等級分布
        print(f"\n📊 行政區表現等級分布")
        grade_dist = self.district_analysis['表現等級'].value_counts()
        total_districts = len(self.district_analysis)
        for grade, count in grade_dist.items():
            percentage = (count / total_districts) * 100
            print(f"{grade}: {count} 個行政區 ({percentage:.1f}%)")
        
        # 特別關注區域
        print(f"\n⭐ 表現優異行政區 (去化率≥80%)")
        excellent_districts = self.district_analysis[self.district_analysis['整體去化率'] >= 80]
        if len(excellent_districts) > 0:
            for _, row in excellent_districts.iterrows():
                avg_monthly = row['月均去化率']
                print(f"{row['縣市']}-{row['行政區']}: {row['整體去化率']:.1f}% "
                      f"(月均{avg_monthly:.2f}%, {row['社區數量']}個社區)")
        else:
            print("目前無去化率超過80%的行政區")
        
        print(f"\n⚠️ 需要關注行政區 (去化率<30%且社區數≥2)")
        concern_districts = self.district_analysis[
            (self.district_analysis['整體去化率'] < 30) & 
            (self.district_analysis['社區數量'] >= 2)
        ].sort_values('整體去化率')
        
        if len(concern_districts) > 0:
            for _, row in concern_districts.iterrows():
                print(f"{row['縣市']}-{row['行政區']}: {row['整體去化率']:.1f}% "
                      f"({row['社區數量']}個社區, {row['戶數']}戶)")
        else:
            print("目前無需特別關注的行政區")
        
        # 區域發展建議
        print(f"\n💡 區域發展建議")
        hot_cities = self.city_analysis[self.city_analysis['市場熱度'] == '熱門']
        cold_districts = self.district_analysis[self.district_analysis['表現等級'] == '困難']
        
        if len(hot_cities) > 0:
            print(f"🔥 推薦投資縣市: {', '.join(hot_cities['縣市'].tolist())}")
        
        if len(cold_districts) > 0:
            print(f"⚡ 需要策略調整的行政區: {len(cold_districts)} 個")
            print(f"   建議重新評估定價策略、銷售方式或產品定位")
    
    def create_time_aware_visualizations(self):
        """創建考慮時間因素的視覺化圖表"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        try:
            plt.style.use('seaborn-v0_8')
        except:
            # 如果 seaborn 樣式不可用，使用預設樣式
            pass
        
        fig, axes = plt.subplots(3, 3, figsize=(24, 18))
        fig.suptitle('預售屋市場時間調整分析 (含縣市行政區)', fontsize=16, fontweight='bold')
        
        # 1. 銷售階段分布
        stage_dist = self.analysis_result['銷售階段'].value_counts()
        axes[0,0].pie(stage_dist.values, labels=stage_dist.index, autopct='%1.1f%%', startangle=90)
        axes[0,0].set_title('銷售階段分布')
        
        # 2. 銷售表現分布
        performance_dist = self.analysis_result['銷售表現'].value_counts()
        axes[0,1].bar(range(len(performance_dist)), performance_dist.values, color='lightblue')
        axes[0,1].set_xticks(range(len(performance_dist)))
        axes[0,1].set_xticklabels(performance_dist.index, rotation=45, ha='right')
        axes[0,1].set_title('銷售表現分布')
        axes[0,1].set_ylabel('社區數量')
        
        # 3. 銷售天數 vs 去化率散點圖
        scatter = axes[0,2].scatter(self.analysis_result['銷售天數'], self.analysis_result['去化率'], 
                                   alpha=0.6, c=self.analysis_result['月均去化率'], cmap='viridis')
        axes[0,2].set_title('銷售天數 vs 去化率')
        axes[0,2].set_xlabel('銷售天數')
        axes[0,2].set_ylabel('去化率 (%)')
        try:
            plt.colorbar(scatter, ax=axes[0,2], label='月均去化率 (%)')
        except:
            pass
        
        # 4. 縣市去化率排名 (TOP 10)
        if hasattr(self, 'city_analysis'):
            top_cities = self.city_analysis.head(10)
            bars = axes[1,0].barh(top_cities['縣市'], top_cities['整體去化率'], color='lightcoral')
            axes[1,0].set_title('縣市去化率排名 (TOP 10)')
            axes[1,0].set_xlabel('整體去化率 (%)')
            
            # 添加數值標籤
            for bar in bars:
                width = bar.get_width()
                axes[1,0].text(width + 1, bar.get_y() + bar.get_height()/2, 
                              f'{width:.1f}%', ha='left', va='center')
        
        # 5. 市場熱度分布圓餅圖
        if hasattr(self, 'city_analysis'):
            heat_dist = self.city_analysis['市場熱度'].value_counts()
            colors = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4']
            axes[1,1].pie(heat_dist.values, labels=heat_dist.index, autopct='%1.1f%%', 
                         colors=colors[:len(heat_dist)], startangle=90)
            axes[1,1].set_title('縣市市場熱度分布')
        
        # 6. 行政區表現等級分布
        if hasattr(self, 'district_analysis'):
            grade_dist = self.district_analysis['表現等級'].value_counts()
            axes[1,2].bar(grade_dist.index, grade_dist.values, color='lightgreen')
            axes[1,2].set_title('行政區表現等級分布')
            axes[1,2].set_ylabel('行政區數量')
            axes[1,2].tick_params(axis='x', rotation=45)
        
        # 7. 月均去化率分布
        axes[2,0].hist(self.analysis_result['月均去化率'], bins=30, color='lightgreen', alpha=0.7)
        axes[2,0].set_title('月均去化率分布')
        axes[2,0].set_xlabel('月均去化率 (%)')
        axes[2,0].set_ylabel('社區數量')
        axes[2,0].axvline(self.analysis_result['月均去化率'].mean(), color='red', 
                         linestyle='--', label=f'平均: {self.analysis_result["月均去化率"].mean():.2f}%')
        axes[2,0].legend()
        
        # 8. 行政區社區數 vs 平均去化率散點圖
        if hasattr(self, 'district_analysis'):
            scatter2 = axes[2,1].scatter(self.district_analysis['社區數量'], 
                                        self.district_analysis['整體去化率'], 
                                        alpha=0.7, c=self.district_analysis['戶數'], 
                                        cmap='plasma', s=50)
            axes[2,1].set_title('行政區社區數 vs 去化率')
            axes[2,1].set_xlabel('社區數量')
            axes[2,1].set_ylabel('整體去化率 (%)')
            try:
                plt.colorbar(scatter2, ax=axes[2,1], label='總戶數')
            except:
                pass
        
        # 9. 戶數規模 vs 月均去化率
        axes[2,2].scatter(self.analysis_result['戶數'], self.analysis_result['月均去化率'], alpha=0.6)
        axes[2,2].set_title('社區戶數 vs 月均去化率')
        axes[2,2].set_xlabel('總戶數')
        axes[2,2].set_ylabel('月均去化率 (%)')
        axes[2,2].set_xscale('log')
        
        plt.tight_layout()
        plt.show()
    
    def create_district_heatmap(self):
        """創建行政區去化率熱力圖"""
        if not hasattr(self, 'district_analysis'):
            print("請先執行行政區分析")
            return
        
        try:
            # 創建樞紐表用於熱力圖
            pivot_data = self.district_analysis.pivot(index='縣市', columns='行政區', values='整體去化率')
            
            # 只顯示有足夠數據的縣市
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
            print("可能是由於數據結構不適合或缺少 seaborn 套件")
    
    def analyze_market_efficiency(self):
        """分析市場效率"""
        print("\n" + "="*60)
        print("市場效率分析")
        print("="*60)
        
        # 效率指標計算
        efficient_communities = self.analysis_result[self.analysis_result['月均去化率'] > 3]
        slow_communities = self.analysis_result[
            (self.analysis_result['銷售天數'] > 365) & 
            (self.analysis_result['去化率'] < 50)
        ]
        
        print(f"\n📈 市場效率指標")
        print(f"高效銷售社區 (月均去化率>3%): {len(efficient_communities)} 個")
        print(f"銷售緩慢社區 (1年以上且去化率<50%): {len(slow_communities)} 個")
        print(f"市場效率比: {len(efficient_communities)/(len(slow_communities)+1):.2f}")
        
        # 預測分析
        print(f"\n🔮 去化時間預測")
        fast_communities = self.analysis_result[
            (self.analysis_result['月均去化率'] > 2) & 
            (self.analysis_result['去化率'] < 80)
        ]
        
        for _, row in fast_communities.head(5).iterrows():
            remaining_rate = 100 - row['去化率']
            estimated_months = remaining_rate / row['月均去化率'] if row['月均去化率'] > 0 else float('inf')
            print(f"{row['社區名稱']}: 預估 {estimated_months:.1f} 個月完全去化")
    
    def export_time_aware_results(self, filename="time_aware_presale_analysis", format_type="csv"):
        """匯出時間調整分析結果"""
        if self.analysis_result is None:
            print("請先執行去化率計算")
            return
        
        # 準備匯出資料
        export_columns = ['編號', '縣市', '行政區', '社區名稱', '戶數', '已售戶數', '去化率', 
                        '銷售天數', '月均去化率', '銷售階段', '銷售表現', '預估完全去化月數']
        main_data = self.analysis_result[export_columns].copy()
        
        # 銷售階段匯總
        stage_summary = self.analysis_result.groupby('銷售階段').agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean'
        }).reset_index()
        stage_summary['整體去化率'] = (stage_summary['已售戶數'] / stage_summary['戶數']) * 100
        
        # 銷售表現匯總
        performance_summary = self.analysis_result.groupby('銷售表現').agg({
            '戶數': 'sum',
            '已售戶數': 'sum',
            '去化率': 'mean',
            '月均去化率': 'mean'
        }).reset_index()
        performance_summary['社區數量'] = self.analysis_result.groupby('銷售表現').size().values
        
        # 縣市匯總（如果有的話）
        city_summary = None
        if hasattr(self, 'city_analysis'):
            city_summary = self.city_analysis.copy()
        
        # 行政區匯總（如果有的話）
        district_summary = None
        if hasattr(self, 'district_analysis'):
            district_summary = self.district_analysis.copy()
        
        if format_type.lower() == "excel":
            try:
                filename_excel = f"{filename}.xlsx"
                with pd.ExcelWriter(filename_excel, engine='openpyxl') as writer:
                    main_data.to_excel(writer, sheet_name='時間調整去化分析', index=False)
                    stage_summary.to_excel(writer, sheet_name='銷售階段匯總', index=False)
                    performance_summary.to_excel(writer, sheet_name='銷售表現匯總', index=False)
                    
                    if city_summary is not None:
                        city_summary.to_excel(writer, sheet_name='縣市分析', index=False)
                    
                    if district_summary is not None:
                        district_summary.to_excel(writer, sheet_name='行政區分析', index=False)
                
                print(f"時間調整分析結果已匯出至: {filename_excel}")
            except ImportError:
                print("❌ 缺少 openpyxl 套件，無法匯出 Excel 格式")
                print("請執行: pip install openpyxl")
                print("改為匯出 CSV 格式...")
                format_type = "csv"
        
        if format_type.lower() == "csv":
            # 匯出為多個 CSV 檔案
            main_filename = f"{filename}_主要分析.csv"
            stage_filename = f"{filename}_銷售階段匯總.csv"
            performance_filename = f"{filename}_銷售表現匯總.csv"
            
            main_data.to_csv(main_filename, index=False, encoding='utf-8-sig')
            stage_summary.to_csv(stage_filename, index=False, encoding='utf-8-sig')
            performance_summary.to_csv(performance_filename, index=False, encoding='utf-8-sig')
            
            files_exported = [main_filename, stage_filename, performance_filename]
            
            if city_summary is not None:
                city_filename = f"{filename}_縣市分析.csv"
                city_summary.to_csv(city_filename, index=False, encoding='utf-8-sig')
                files_exported.append(city_filename)
            
            if district_summary is not None:
                district_filename = f"{filename}_行政區分析.csv"
                district_summary.to_csv(district_filename, index=False, encoding='utf-8-sig')
                files_exported.append(district_filename)
            
            print(f"時間調整分析結果已匯出為 CSV 格式:")
            for file in files_exported:
                print(f"  - {file}")

# 請將以下方法添加到您的 PresaleMarketAnalysis 類別中，並替換 main() 函數

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
                
                # 檢查是否為數字
                if user_input.isdigit():
                    city_index = int(user_input) - 1
                    if 0 <= city_index < len(available_cities):
                        selected_city = available_cities[city_index]
                    else:
                        print(f"❌ 請輸入 1-{len(available_cities)} 之間的數字")
                        continue
                else:
                    # 直接輸入縣市名稱
                    if user_input in available_cities:
                        selected_city = user_input
                    else:
                        # 模糊匹配
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
                
                # 執行選定縣市的分析
                print(f"\n🎯 開始分析 {selected_city}...")
                self.analyze_single_city(selected_city)
                
                # 詢問是否繼續分析其他縣市
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
                # 解析用戶輸入
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
            
            # 確認分析
            print(f"\n確認要分析以下 {len(selected_cities)} 個縣市:")
            for i, city in enumerate(selected_cities, 1):
                print(f"  {i}. {city}")
            
            confirm = input(f"\n確認開始批次分析? (y/n): ").strip().lower()
            if confirm not in ['y', 'yes', '是', '1']:
                print("已取消批次分析")
                return
            
            # 執行批次分析
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
        
        # 不同維度的排名
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
        
        for rank in range(min(10, len(self.city_analysis))):  # 顯示前10名
            print(f"{rank+1:<4} ", end="")
            for metric, data in rankings.items():
                city = data.iloc[rank]['縣市']
                print(f"{city:<12}", end="")
            print()
        
        # 市場熱度分析
        print(f"\n🌡️ 市場熱度分布:")
        heat_distribution = self.city_analysis['市場熱度'].value_counts()
        for heat, count in heat_distribution.items():
            cities = self.city_analysis[self.city_analysis['市場熱度'] == heat]['縣市'].tolist()
            print(f"  {heat}: {count}個縣市 - {', '.join(cities)}")

  
        """顯示縣市排名比較"""
        if not hasattr(self, 'city_analysis'):
            print("正在進行縣市分析...")
            self.analyze_city_performance()
        
        print("\n" + "="*80)
        print("🏆 縣市排名比較分析")
        print("="*80)
        
        # 不同維度的排名
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
        
        for rank in range(min(10, len(self.city_analysis))):  # 顯示前10名
            print(f"{rank+1:<4} ", end="")
            for metric, data in rankings.items():
                city = data.iloc[rank]['縣市']
                print(f"{city:<12}", end="")
            print()
        
        # 市場熱度分析
        print(f"\n🌡️ 市場熱度分布:")
        heat_distribution = self.city_analysis['市場熱度'].value_counts()
        for heat, count in heat_distribution.items():
            cities = self.city_analysis[self.city_analysis['市場熱度'] == heat]['縣市'].tolist()
            print(f"  {heat}: {count}個縣市 - {', '.join(cities)}")


        """顯示縣市排名比較"""
        if not hasattr(self, 'city_analysis'):
            print("正在進行縣市分析...")
            self.analyze_city_performance()
        
        print("\n" + "="*80)
        print("🏆 縣市排名比較分析")
        print("="*80)
        
        # 不同維度的排名
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
        
        for rank in range(min(10, len(self.city_analysis))):  # 顯示前10名
            print(f"{rank+1:<4} ", end="")
            for metric, data in rankings.items():
                city = data.iloc[rank]['縣市']
                print(f"{city:<12}", end="")
            print()
        
        # 市場熱度分析
        print(f"\n🌡️ 市場熱度分布:")
        heat_distribution = self.city_analysis['市場熱度'].value_counts()
        for heat, count in heat_distribution.items():
            cities = self.city_analysis[self.city_analysis['市場熱度'] == heat]['縣市'].tolist()
            print(f"  {heat}: {count}個縣市 - {', '.join(cities)}")
        
def main():
    """主要執行函數"""
    try:
        # 創建分析實例
        print("正在初始化分析程式...")
        analyzer = PresaleMarketAnalysis()

        input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\pre_sale_data\processed"
        fn = "預售屋買賣主檔_11006_11406.csv"
        pre_sale_input_path = os.path.join(input_dir, fn)

        input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\sale_data\processed"
        fn = "預售屋備查.csv"
        sale_data_input_path = os.path.join(input_dir, fn)
        
        # 載入並分析數據
        analyzer.load_data(pre_sale_input_path, sale_data_input_path)
        
        # 預處理數據（包含編號比對檢查）
        result = analyzer.preprocess_data()
        if result is None:
            print("❌ 由於編號比對失敗，程式終止執行")
            return
        
        # 執行基本分析
        print("\n" + "="*60)
        print("執行基本去化率分析...")
        print("="*60)
        analyzer.calculate_time_adjusted_absorption_rate()
        
        # 執行縣市和行政區分析
        analyzer.analyze_city_performance()
        analyzer.analyze_district_performance()
        
        # 主選單迴圈
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
                    export_choice = input("選擇匯出格式 (csv/excel): ").strip().lower()
                    if export_choice in ['excel', 'xlsx']:
                        analyzer.export_time_aware_results("presale_analysis_results", "excel")
                    else:
                        analyzer.export_time_aware_results("presale_analysis_results", "csv")
                elif main_choice == "7":
                    print("\n🚀 執行完整分析...")
                    # 執行完整分析
                    analyzer.analyze_city_detailed()
                    analyzer.generate_city_detailed_report()
                    
                    print("\n正在生成視覺化圖表...")
                    try:
                        analyzer.create_time_aware_visualizations()
                        analyzer.create_district_heatmap()
                        
                        # 為主要縣市創建詳細視覺化
                        if hasattr(analyzer, 'city_analysis'):
                            top_cities = analyzer.city_analysis.head(3)['縣市'].tolist()
                            for city in top_cities:
                                print(f"正在生成 {city} 詳細視覺化圖表...")
                                analyzer.create_city_visualizations(city)
                    except Exception as e:
                        print(f"⚠️ 圖表生成失敗: {str(e)}")
                    
                    # 匯出結果
                    analyzer.export_time_aware_results("presale_analysis_results", "csv")
                    analyzer.export_city_detailed_results("city_detailed_analysis", "csv")
                    
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

if __name__ == "__main__":
    main()




