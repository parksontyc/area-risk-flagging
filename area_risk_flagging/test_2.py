#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
立即可用的預售屋風險分析系統
===========================

修正了參數錯誤，確保可以立即運行
包含相對風險分級功能
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import re

warnings.filterwarnings('ignore')

class WorkingPreSaleRiskAnalyzer:
    """可正常運行的預售屋風險分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.sale_data = None
        self.transaction_data = None
        
    def load_data(self, sale_file: str, transaction_file: str) -> bool:
        """載入預售屋數據"""
        try:
            self.sale_data = pd.read_csv(sale_file, encoding='utf-8')
            print(f"✅ 成功載入銷售資料: {len(self.sale_data)} 筆")
            
            self.transaction_data = pd.read_csv(transaction_file, encoding='utf-8')
            print(f"✅ 成功載入成交資料: {len(self.transaction_data)} 筆")
            
            self._preprocess_data()
            return True
            
        except Exception as e:
            print(f"❌ 數據載入失敗: {str(e)}")
            return False
    
    def _preprocess_data(self):
        """資料預處理"""
        print("🔄 開始資料預處理...")
        
        # 處理銷售資料
        if self.sale_data is not None:
            self.sale_data['銷售起始年季_clean'] = self.sale_data['銷售起始年季'].fillna('未知')
            self.sale_data['地區'] = self.sale_data['縣市'] + '-' + self.sale_data['行政區']
            self.sale_data['戶數'] = pd.to_numeric(self.sale_data['戶數'], errors='coerce').fillna(0)
            self._process_sales_time_data()
        
        # 處理成交資料
        if self.transaction_data is not None:
            self.transaction_data['交易年月'] = pd.to_numeric(self.transaction_data['交易年月'], errors='coerce')
            self.transaction_data['地區'] = self.transaction_data['縣市'] + '-' + self.transaction_data['行政區']
            self.transaction_data['交易總價'] = pd.to_numeric(self.transaction_data['交易總價'], errors='coerce')
            self.transaction_data['建物單價'] = pd.to_numeric(self.transaction_data['建物單價'], errors='coerce')
            self._process_transaction_time_data()
        
        print("✅ 資料預處理完成")
    
    def _process_sales_time_data(self):
        """處理銷售時間相關數據"""
        print("⏰ 處理銷售時間數據...")
        
        self.sale_data['銷售起始日期'] = self.sale_data['銷售起始時間'].apply(self._convert_roc_date)
        self.sale_data['銷售結束日期'] = self.sale_data['銷售期間'].apply(self._parse_sales_period)
        
        current_date = datetime.now()
        self.sale_data['銷售天數'] = self.sale_data.apply(
            lambda row: self._calculate_sales_days(row['銷售起始日期'], row['銷售結束日期'], current_date), 
            axis=1
        )
        
        self.sale_data['銷售月數'] = self.sale_data['銷售天數'] / 30.44
        self.sale_data['銷售階段'] = self.sale_data['銷售月數'].apply(self._classify_sales_stage)
        
        print(f"✅ 銷售時間處理完成，有效日期數: {self.sale_data['銷售起始日期'].notna().sum()}")
    
    def _process_transaction_time_data(self):
        """處理成交時間相關數據"""
        print("⏰ 處理成交時間數據...")
        
        self.transaction_data['交易日期_parsed'] = self.transaction_data['交易年月'].apply(
            lambda x: self._convert_year_month_to_date(x) if pd.notna(x) else None
        )
        
        print(f"✅ 成交時間處理完成，有效日期數: {self.transaction_data['交易日期_parsed'].notna().sum()}")
    
    def _convert_roc_date(self, roc_date):
        """轉換民國年日期為標準日期格式"""
        if pd.isna(roc_date) or roc_date == 'null':
            return None
        
        try:
            date_str = str(int(roc_date))
            
            if len(date_str) == 7:
                year = int(date_str[:3]) + 1911
                month = int(date_str[3:5])
                day = int(date_str[5:7])
                return datetime(year, month, day)
            elif len(date_str) == 6:
                year = int(date_str[:3]) + 1911
                month = int(date_str[3:5])
                day = int(date_str[5:6]) if len(date_str[5:]) == 1 else int(date_str[5:])
                return datetime(year, month, day)
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    def _convert_year_month_to_date(self, year_month):
        """轉換YYYAA格式的年月為日期"""
        if pd.isna(year_month):
            return None
        
        try:
            year_month_str = str(int(year_month))
            if len(year_month_str) >= 5:
                year = int(year_month_str[:3]) + 1911
                month = int(year_month_str[3:5])
                return datetime(year, month, 1)
            return None
        except (ValueError, TypeError):
            return None
    
    def _parse_sales_period(self, period_str):
        """解析銷售期間字串"""
        if pd.isna(period_str) or period_str in ['無', 'null', '']:
            return None
        
        try:
            if '~' in str(period_str):
                parts = str(period_str).split('~')
                if len(parts) >= 2:
                    end_part = parts[1].strip()
                    
                    if re.match(r'\d{7}', end_part):
                        return self._convert_roc_date(end_part)
                    elif '完售' in end_part or '售完' in end_part:
                        return None
            
            if '完銷' in str(period_str) or '售完' in str(period_str):
                return None
                
        except Exception:
            pass
        
        return None
    
    def _calculate_sales_days(self, start_date, end_date, current_date):
        """計算銷售天數"""
        if pd.isna(start_date):
            return 0
        
        if pd.isna(end_date):
            end_date = current_date
        
        return (end_date - start_date).days
    
    def _classify_sales_stage(self, sales_months):
        """分類銷售階段"""
        if pd.isna(sales_months) or sales_months <= 0:
            return '未知'
        
        if sales_months < 6:
            return '新開案'
        elif sales_months < 24:
            return '積極銷售'
        elif sales_months < 48:
            return '尾盤銷售'
        else:
            return '長期銷售'
    
    def calculate_time_adjusted_absorption_rate(self, by_quarter: bool = True, min_sales_months: float = 1.0) -> pd.DataFrame:
        """計算時間調整的預售屋去化率"""
        if self.sale_data is None or self.transaction_data is None:
            raise ValueError("請先載入數據")
        
        print("📊 計算時間調整預售屋去化率...")
        
        # 篩選有效的銷售數據
        valid_sales = self.sale_data[
            (self.sale_data['銷售月數'] >= min_sales_months) & 
            (self.sale_data['戶數'] > 0)
        ].copy()
        
        print(f"有效銷售項目數: {len(valid_sales)} (原始: {len(self.sale_data)})")
        
        results = []
        
        # 按地區和時間分組計算
        group_cols = ['地區']
        if by_quarter:
            group_cols.append('交易年季')
        
        # 計算各地區的供給數據
        region_supply = valid_sales.groupby('地區').agg({
            '戶數': 'sum',
            '銷售月數': 'mean',
            '編號': 'count'
        }).reset_index()
        region_supply.columns = ['地區', '總戶數', '平均銷售月數', '項目數']
        
        # 計算各地區各季的成交筆數
        demand_stats = self.transaction_data.groupby(group_cols).agg({
            '備查編號': 'count',
            '交易總價': ['sum', 'mean'],
            '建物單價': 'mean'
        }).reset_index()
        
        if by_quarter:
            demand_stats.columns = ['地區', '交易年季', '成交筆數', '總交易金額', '平均交易價', '平均建物單價']
        else:
            demand_stats.columns = ['地區', '成交筆數', '總交易金額', '平均交易價', '平均建物單價']
        
        # 合併供需數據
        if by_quarter:
            for quarter in demand_stats['交易年季'].unique():
                quarter_data = demand_stats[demand_stats['交易年季'] == quarter]
                merged = quarter_data.merge(region_supply, on='地區', how='left')
                
                # 計算時間調整去化率
                merged['標準去化率'] = merged['成交筆數'] / merged['總戶數']
                merged['標準去化率'] = merged['標準去化率'].fillna(0).clip(0, 1)
                
                # 計算月去化率
                merged['月去化率'] = merged['標準去化率'] / merged['平均銷售月數']
                merged['月去化率'] = merged['月去化率'].fillna(0)
                
                # 計算時間調整去化率
                merged['時間調整去化率'] = merged.apply(
                    lambda row: self._calculate_time_weighted_absorption(
                        row['標準去化率'], row['平均銷售月數']
                    ), axis=1
                )
                
                # 計算銷售效率分數
                merged['銷售效率分數'] = merged.apply(
                    lambda row: self._calculate_sales_efficiency_score(
                        row['成交筆數'], row['平均銷售月數'], row['項目數']
                    ), axis=1
                )
                
                merged['分析時間'] = quarter
                results.append(merged)
        else:
            merged = demand_stats.merge(region_supply, on='地區', how='left')
            
            # 計算各項指標
            merged['標準去化率'] = merged['成交筆數'] / merged['總戶數']
            merged['標準去化率'] = merged['標準去化率'].fillna(0).clip(0, 1)
            
            merged['月去化率'] = merged['標準去化率'] / merged['平均銷售月數']
            merged['月去化率'] = merged['月去化率'].fillna(0)
            
            merged['時間調整去化率'] = merged.apply(
                lambda row: self._calculate_time_weighted_absorption(
                    row['標準去化率'], row['平均銷售月數']
                ), axis=1
            )
            
            merged['銷售效率分數'] = merged.apply(
                lambda row: self._calculate_sales_efficiency_score(
                    row['成交筆數'], row['平均銷售月數'], row['項目數']
                ), axis=1
            )
            
            results.append(merged)
        
        result_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        
        print(f"✅ 完成 {len(result_df)} 筆時間調整去化率計算")
        return result_df
    
    def _calculate_time_weighted_absorption(self, standard_rate: float, sales_months: float) -> float:
        """計算時間加權去化率"""
        if pd.isna(standard_rate) or pd.isna(sales_months) or sales_months <= 0:
            return 0
        
        baseline_months = 12
        
        if sales_months < baseline_months:
            time_factor = min(baseline_months / sales_months, 2.0)
        else:
            time_factor = max(baseline_months / sales_months, 0.5)
        
        adjusted_rate = standard_rate * time_factor
        return min(adjusted_rate, 1.0)
    
    def _calculate_sales_efficiency_score(self, transactions: int, sales_months: float, projects: int) -> float:
        """計算銷售效率分數"""
        if pd.isna(sales_months) or sales_months <= 0 or projects <= 0:
            return 0
        
        monthly_transactions = transactions / sales_months
        efficiency_per_project = monthly_transactions / projects
        
        return efficiency_per_project
    
    def classify_risk_with_relative_option(self, indicators: pd.DataFrame, use_relative: bool = True) -> pd.DataFrame:
        """風險分級（含相對分級選項）"""
        print("🎯 進行時間調整風險等級分類...")
        
        result = indicators.copy()
        
        # 首先進行絕對分級
        result = self._absolute_risk_classification(result)
        
        # 如果使用相對分級
        if use_relative:
            result = self._relative_risk_classification(result)
        
        print("✅ 風險分級完成")
        return result
    
    def _absolute_risk_classification(self, data: pd.DataFrame) -> pd.DataFrame:
        """絕對標準風險分級"""
        result = data.copy()
        
        # 初始化風險等級
        result['絕對風險等級'] = '🟢 低風險'
        result['風險分數'] = 0
        result['主要風險因子'] = ''
        
        for idx, row in result.iterrows():
            risk_factors = []
            risk_score = 0
            
            # 時間調整去化率風險
            time_adj_rate = row.get('時間調整去化率', 0)
            if time_adj_rate < 0.25:
                risk_factors.append('時間調整去化率偏低')
                risk_score += 3
            elif time_adj_rate > 0.65:
                risk_factors.append('可能過度銷售')
                risk_score += 1
            
            # 月去化率風險
            monthly_rate = row.get('月去化率', 0)
            if monthly_rate < 0.05:
                risk_factors.append('銷售效率低落')
                risk_score += 2
            
            # 成交量風險
            transactions = row.get('成交筆數', 0)
            if transactions == 0:
                risk_factors.append('完全無成交')
                risk_score += 4
            elif transactions < 5:
                risk_factors.append('成交量極低')
                risk_score += 2
            
            # 銷售期間風險
            avg_sales_months = row.get('平均銷售月數', 0)
            if avg_sales_months > 36:
                risk_factors.append('銷售期間過長')
                risk_score += 1
            
            # 綜合風險等級判定
            if risk_score >= 5:
                result.loc[idx, '絕對風險等級'] = '🔴 高風險'
            elif risk_score >= 2.5:
                result.loc[idx, '絕對風險等級'] = '🟡 中風險'
            
            result.loc[idx, '風險分數'] = risk_score
            result.loc[idx, '主要風險因子'] = '; '.join(risk_factors) if risk_factors else '無明顯風險'
        
        # 設定預設風險等級
        result['風險等級'] = result['絕對風險等級']
        
        return result
    
    def _relative_risk_classification(self, data: pd.DataFrame) -> pd.DataFrame:
        """相對標準風險分級（同縣市內比較）"""
        print("   📊 執行同縣市內相對風險分級...")
        
        result = data.copy()
        
        # 提取縣市資訊
        result['縣市'] = result['地區'].str.split('-').str[0]
        
        # 初始化相對風險欄位
        result['相對風險等級'] = '🟢 低風險'
        result['縣市內排名'] = 0
        result['縣市內百分位'] = 0.0
        result['相對風險原因'] = ''
        
        # 相對分級閾值
        high_risk_threshold = 25
        medium_risk_threshold = 50
        
        # 絕對最低標準
        absolute_minimums = {
            'min_transactions': 1,
            'min_time_adj_rate': 0.005,
            'max_sales_months': 60
        }
        
        # 按縣市分別進行相對分級
        for city in result['縣市'].unique():
            city_mask = result['縣市'] == city
            city_data = result[city_mask].copy()
            
            if len(city_data) < 3:
                print(f"      ⚠️ {city} 行政區數量過少({len(city_data)})，保持絕對分級")
                result.loc[city_mask, '相對風險等級'] = result.loc[city_mask, '絕對風險等級']
                result.loc[city_mask, '相對風險原因'] = '行政區數量不足,使用絕對標準'
                continue
            
            # 計算縣市內綜合排名
            time_adj_rank = city_data['時間調整去化率'].rank(method='min', ascending=False)
            monthly_rank = city_data['月去化率'].rank(method='min', ascending=False)
            transaction_rank = city_data['成交筆數'].rank(method='min', ascending=False)
            
            # 綜合排名（權重分配）
            composite_rank = (time_adj_rank * 0.4 + monthly_rank * 0.3 + transaction_rank * 0.3)
            
            # 計算百分位數
            percentiles = composite_rank.rank(pct=True) * 100
            
            # 相對風險分級
            relative_risks = []
            relative_reasons = []
            
            for i, (orig_idx, row) in enumerate(city_data.iterrows()):
                percentile = percentiles.iloc[i]
                
                # 檢查絕對最低標準
                absolute_issues = []
                if row.get('成交筆數', 0) < absolute_minimums['min_transactions']:
                    absolute_issues.append('成交筆數過低')
                if row.get('時間調整去化率', 0) < absolute_minimums['min_time_adj_rate']:
                    absolute_issues.append('去化率極低')
                if row.get('平均銷售月數', 0) > absolute_minimums['max_sales_months']:
                    absolute_issues.append('銷售期間過長')
                
                if absolute_issues:
                    risk_level = '🔴 高風險'
                    reason = f"絕對標準問題: {'; '.join(absolute_issues)}"
                else:
                    # 使用相對標準
                    if percentile <= high_risk_threshold:
                        risk_level = '🔴 高風險'
                        reason = f"縣市內排名後{high_risk_threshold}% (第{composite_rank.iloc[i]:.0f}名)"
                    elif percentile <= medium_risk_threshold:
                        risk_level = '🟡 中風險'
                        reason = f"縣市內中等表現 (第{composite_rank.iloc[i]:.0f}名)"
                    else:
                        risk_level = '🟢 低風險'
                        reason = f"縣市內前{100-medium_risk_threshold}% (第{composite_rank.iloc[i]:.0f}名)"
                
                relative_risks.append(risk_level)
                relative_reasons.append(reason)
            
            # 更新結果
            result.loc[city_mask, '相對風險等級'] = relative_risks
            result.loc[city_mask, '縣市內排名'] = composite_rank.values
            result.loc[city_mask, '縣市內百分位'] = percentiles.values
            result.loc[city_mask, '相對風險原因'] = relative_reasons
            
            # 顯示該縣市的分級結果
            city_risk_counts = pd.Series(relative_risks).value_counts()
            print(f"      🏙️ {city} 相對分級結果:")
            for level, count in city_risk_counts.items():
                percentage = count / len(city_data) * 100
                print(f"         {level}: {count} 個 ({percentage:.1f}%)")
        
        # 使用相對風險等級作為主要風險等級
        result['風險等級'] = result['相對風險等級']
        
        return result
    
    def run_interactive_analysis(self):
        """執行互動式分析界面"""
        print("🖥️ 啟動時間調整互動式分析界面...")
        
        if self.sale_data is None or self.transaction_data is None:
            print("❌ 請先載入數據")
            return
        
        while True:
            print("\n" + "="*55)
            print("🏠 預售屋風險檢視分析系統 (修正版)")
            print("="*55)
            print("1. 📊 時間調整整體市場風險分析")
            print("2. 🏙️ 指定縣市時間調整分析")
            print("3. 🎯 絕對 vs 相對風險分級比較")
            print("4. 📋 生成分析報告")
            print("0. ❌ 退出系統")
            print("="*55)
            
            choice = input("請選擇功能 (0-4): ").strip()
            
            if choice == '0':
                print("👋 感謝使用，再見！")
                break
            elif choice == '1':
                self._show_overall_analysis()
            elif choice == '2':
                self._show_city_analysis()
            elif choice == '3':
                self._show_risk_comparison()
            elif choice == '4':
                self._generate_report()
            else:
                print("⚠️ 無效選擇，請重新輸入")
    
    def _show_overall_analysis(self):
        """顯示整體分析"""
        print("\n📊 時間調整整體市場風險分析")
        print("-" * 35)
        
        print("請選擇風險分級方式:")
        print("1. 相對分級 (同縣市內比較，推薦)")
        print("2. 絕對分級 (固定閾值)")
        
        choice = input("請選擇 (1-2): ").strip()
        use_relative = choice == '1'
        
        # 計算時間調整去化率
        absorption_data = self.calculate_time_adjusted_absorption_rate(by_quarter=True)
        
        # 進行風險分級
        risk_analysis = self.classify_risk_with_relative_option(absorption_data, use_relative)
        
        # 統計結果
        classification_method = '相對分級' if use_relative else '絕對分級'
        print(f"\n🎯 時間調整分析摘要 ({classification_method}):")
        
        total_regions = len(risk_analysis['地區'].unique())
        high_risk = len(risk_analysis[risk_analysis['風險等級'] == '🔴 高風險'])
        medium_risk = len(risk_analysis[risk_analysis['風險等級'] == '🟡 中風險'])
        low_risk = len(risk_analysis[risk_analysis['風險等級'] == '🟢 低風險'])
        
        print(f"   總分析地區數: {total_regions}")
        print(f"   🔴 高風險地區: {high_risk} 個 ({high_risk/total_regions*100:.1f}%)")
        print(f"   🟡 中風險地區: {medium_risk} 個 ({medium_risk/total_regions*100:.1f}%)")
        print(f"   🟢 低風險地區: {low_risk} 個 ({low_risk/total_regions*100:.1f}%)")
        print(f"   📈 平均時間調整去化率: {risk_analysis['時間調整去化率'].mean():.2%}")
        print(f"   ⏰ 平均月去化率: {risk_analysis['月去化率'].mean():.3%}")
        
        # 顯示高風險地區
        high_risk_areas = risk_analysis[risk_analysis['風險等級'] == '🔴 高風險']['地區'].unique()
        if len(high_risk_areas) > 0:
            print(f"\n⚠️ 高風險地區清單 (前10個):")
            for area in high_risk_areas[:10]:
                print(f"   • {area}")
        else:
            print(f"\n✅ 目前無高風險地區")
        
        # 如果是相對分級，顯示各縣市改善情況
        if use_relative and '縣市' in risk_analysis.columns:
            print(f"\n🏙️ 各縣市風險分布:")
            city_stats = risk_analysis.groupby('縣市').agg({
                '風險等級': lambda x: (x == '🔴 高風險').sum(),
                '地區': 'count'
            }).reset_index()
            city_stats.columns = ['縣市', '高風險地區數', '總地區數']
            
            for _, row in city_stats.iterrows():
                risk_rate = row['高風險地區數'] / row['總地區數'] * 100
                print(f"   {row['縣市']}: {row['高風險地區數']}/{row['總地區數']} 高風險 ({risk_rate:.1f}%)")
    
    def _show_city_analysis(self):
        """顯示縣市分析"""
        print("\n🏙️ 指定縣市時間調整分析")
        print("-" * 35)
        
        available_cities = sorted(self.sale_data['縣市'].unique())
        print("可用縣市:")
        for i, city in enumerate(available_cities, 1):
            print(f"{i:2d}. {city}")
        
        try:
            choice = int(input("\n請選擇縣市編號: ")) - 1
            if 0 <= choice < len(available_cities):
                selected_city = available_cities[choice]
                
                # 計算該縣市的分析
                absorption_data = self.calculate_time_adjusted_absorption_rate(by_quarter=True)
                
                # 篩選該縣市數據
                city_data = absorption_data[absorption_data['地區'].str.startswith(f"{selected_city}-")]
                
                if len(city_data) > 0:
                    # 進行相對風險分級
                    risk_analysis = self.classify_risk_with_relative_option(city_data, use_relative=True)
                    
                    print(f"\n📊 {selected_city} 時間調整分析結果:")
                    
                    total_districts = len(risk_analysis['地區'].unique())
                    high_risk = len(risk_analysis[risk_analysis['風險等級'] == '🔴 高風險'])
                    medium_risk = len(risk_analysis[risk_analysis['風險等級'] == '🟡 中風險'])
                    low_risk = len(risk_analysis[risk_analysis['風險等級'] == '🟢 低風險'])
                    
                    print(f"   行政區數量: {total_districts}")
                    print(f"   🔴 高風險: {high_risk} 個")
                    print(f"   🟡 中風險: {medium_risk} 個")
                    print(f"   🟢 低風險: {low_risk} 個")
                    print(f"   平均時間調整去化率: {risk_analysis['時間調整去化率'].mean():.2%}")
                    
                    # 顯示各行政區排名
                    print(f"\n📋 {selected_city} 各行政區風險排名:")
                    
                    district_summary = risk_analysis.groupby('地區').agg({
                        '時間調整去化率': 'mean',
                        '月去化率': 'mean',
                        '風險等級': lambda x: x.mode().iloc[0] if len(x) > 0 else '🟢 低風險',
                        '相對風險原因': 'first'
                    }).reset_index()
                    
                    # 按時間調整去化率排序
                    district_summary = district_summary.sort_values('時間調整去化率', ascending=False)
                    
                    for _, row in district_summary.iterrows():
                        district_name = row['地區'].split('-')[1]
                        reason = row.get('相對風險原因', '')
                        print(f"   {row['風險等級']} {district_name}: 時間調整去化率 {row['時間調整去化率']:.2%} ({reason})")
                else:
                    print(f"⚠️ 未找到 {selected_city} 的數據")
            else:
                print("⚠️ 無效的縣市編號")
        except (ValueError, IndexError):
            print("⚠️ 請輸入有效的數字")
    
    def _show_risk_comparison(self):
        """顯示絕對vs相對風險分級比較"""
        print("\n🎯 絕對 vs 相對風險分級比較")
        print("-" * 45)
        
        # 計算絕對分級
        absorption_data_abs = self.calculate_time_adjusted_absorption_rate(by_quarter=True)
        absolute_result = self.classify_risk_with_relative_option(absorption_data_abs, use_relative=False)
        
        # 計算相對分級
        absorption_data_rel = self.calculate_time_adjusted_absorption_rate(by_quarter=True)
        relative_result = self.classify_risk_with_relative_option(absorption_data_rel, use_relative=True)
        
        # 統計比較
        print(f"\n📊 分級結果統計比較:")
        
        # 絕對分級統計
        abs_counts = absolute_result['風險等級'].value_counts()
        print(f"\n🔸 絕對分級結果:")
        for level, count in abs_counts.items():
            percentage = count / len(absolute_result) * 100
            print(f"   {level}: {count} 個地區 ({percentage:.1f}%)")
        
        # 相對分級統計
        rel_counts = relative_result['風險等級'].value_counts()
        print(f"\n🔸 相對分級結果:")
        for level, count in rel_counts.items():
            percentage = count / len(relative_result) * 100
            print(f"   {level}: {count} 個地區 ({percentage:.1f}%)")
        
        # 改善統計
        abs_high = len(absolute_result[absolute_result['風險等級'] == '🔴 高風險'])
        rel_high = len(relative_result[relative_result['風險等級'] == '🔴 高風險'])
        improvement = abs_high - rel_high
        
        print(f"\n💡 相對分級改善效果:")
        print(f"   絕對分級高風險地區: {abs_high} 個")
        print(f"   相對分級高風險地區: {rel_high} 個")
        print(f"   改善地區數: {improvement} 個")
        print(f"   改善比例: {improvement/len(relative_result)*100:.1f}%")
    
    def _generate_report(self):
        """生成分析報告"""
        print("\n📋 生成分析報告")
        print("-" * 25)
        
        # 計算相對分級結果
        absorption_data = self.calculate_time_adjusted_absorption_rate(by_quarter=True)
        risk_analysis = self.classify_risk_with_relative_option(absorption_data, use_relative=True)
        
        # 儲存結果
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'預售屋風險分析報告_{timestamp}.csv'
        
        risk_analysis.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ 分析報告已儲存: {output_file}")
        print(f"   📊 分析地區數: {len(risk_analysis['地區'].unique())}")
        print(f"   📅 分析時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """主程式入口"""
    print("🏠 預售屋風險檢視分析系統 (修正版)")
    print("=" * 50)
    
    # 初始化分析器
    analyzer = WorkingPreSaleRiskAnalyzer()
    
    # 載入數據
    input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\pre_sale_data\processed"
    fn = "預售屋買賣主檔_11006_11406.csv"
    transaction_file = os.path.join(input_dir, fn)

    input_dir = r"C:\pylabs\area-risk-flagging\data\lvr_moi\sale_data\processed"
    fn = "預售屋備查.csv"
    sale_file = os.path.join(input_dir, fn)
    
    # 載入數據
    if analyzer.load_data(sale_file, transaction_file):
        print("✅ 數據載入成功，啟動互動式分析...")
        analyzer.run_interactive_analysis()
    else:
        print("❌ 數據載入失敗，請檢查檔案路徑")

if __name__ == "__main__":
    main()


