import os
import pandas as pd
from datetime import datetime
import re

# 配置信息
CONFIG = {
    'wechat_columns': ['交易时间', '交易类型', '交易对方', '商品', '收/支', '金额', '支付方式', '当前状态', '交易单号', '商户单号', '备注'],
    'alipay_columns': ['交易时间', '交易类型', '交易对方', '对方账户', '商品名称', '收/支', '金额', '支付方式', '交易状态', '交易订单号', '商家订单号', '备注'],
    'merged_columns': ['交易时间', '交易类型', '交易对方', '商品/商品名称', '收/支', '金额', '收支金额', '支付方式', '交易状态'],
    'hidden_columns': ['交易单号', '商户单号/商家订单号', '备注']
}

# 对应关系映射
COLUMN_MAPPING = {
    'wechat': {
        '交易时间': '交易时间',
        '交易类型': '交易类型',
        '交易对方': '交易对方',
        '商品': '商品/商品名称',
        '收/支': '收/支',
        '金额': '金额',
        '支付方式': '支付方式',
        '当前状态': '交易状态',
        '交易单号': '交易单号',
        '商户单号': '商户单号/商家订单号',
        '备注': '备注'
    },
    'alipay': {
        '交易时间': '交易时间',
        '交易类型': '交易类型',
        '交易对方': '交易对方',
        '商品名称': '商品/商品名称',
        '收/支': '收/支',
        '金额': '金额',
        '支付方式': '支付方式',
        '交易状态': '交易状态',
        '交易订单号': '交易单号',
        '商家订单号': '商户单号/商家订单号',
        '备注': '备注'
    }
}

# 交易状态标准化映射
STATUS_MAPPING = {
    '支付成功': ['支付成功', '对方已收钱', '已转账', '交易成功', '交易已完成', '支付成功'],
    '已存入零钱': ['已存入零钱', '存入零钱', '转入零钱']
}

def find_bill_files(directory):
    """查找目录中的微信和支付宝账单文件"""
    wechat_files = []
    alipay_files = []
    
    for file in os.listdir(directory):
        if file.endswith('.xlsx') and '微信' in file:
            wechat_files.append(os.path.join(directory, file))
        elif file.endswith('.csv') and '支付宝' in file:
            alipay_files.append(os.path.join(directory, file))
    
    return wechat_files, alipay_files

def read_wechat_bill(file_path):
    """读取微信账单并处理数据"""
    print(f"读取微信账单: {os.path.basename(file_path)}")
    
    try:
        # 读取微信账单，跳过前16行（表头在第16行，数据从第17行开始）
        df = pd.read_excel(file_path, skiprows=16)
        
        print(f"微信账单原始数据行数: {len(df)}")
        print(f"微信账单原始列数: {len(df.columns)}")
        print(f"微信账单原始前3行数据:")
        print(df.head(3))
        
        # 设置正确的列名
        if len(df.columns) >= len(CONFIG['wechat_columns']):
            df.columns = CONFIG['wechat_columns']
        else:
            print(f"警告：微信账单列数不足，期望{len(CONFIG['wechat_columns'])}列，实际{len(df.columns)}列")
        
        # 数据验证
        print("\n微信账单数据验证:")
        
        # 验证交易时间
        try:
            df['交易时间'] = pd.to_datetime(df['交易时间'], errors='coerce')
            valid_dates = df['交易时间'].count()
            print(f"交易时间有效记录: {valid_dates}/{len(df)}")
        except:
            print("交易时间验证失败")
        
        # 处理金额字段
        print(f"原始金额列前5个值: {df['金额'].head().tolist()}")
        df['金额'] = df['金额'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        df['金额'] = pd.to_numeric(df['金额'], errors='coerce').fillna(0.0)
        
        # 金额统计
        valid_amounts = (df['金额'] != 0).sum()
        total_amount = df['金额'].sum()
        print(f"金额有效记录(非0): {valid_amounts}/{len(df)}")
        print(f"金额总和: {total_amount:.2f}")
        print(f"金额为0的记录: {(df['金额'] == 0).sum()}")
        
        # 交易状态标准化
        if '当前状态' in df.columns:
            def standardize_status(status):
                status = str(status).strip()
                if status == '退款':
                    return '退款'
                for standard, variations in STATUS_MAPPING.items():
                    if status in variations:
                        return standard
                return status
            
            df['当前状态'] = df['当前状态'].apply(standardize_status)
        
        # 映射到合并后的列名
        mapped_df = pd.DataFrame(columns=CONFIG['merged_columns'] + CONFIG['hidden_columns'])
        for wechat_col, merged_col in COLUMN_MAPPING['wechat'].items():
            if wechat_col in df.columns:
                mapped_df[merged_col] = df[wechat_col]
        
        # 添加来源标识
        mapped_df['来源'] = '微信'
        mapped_df['支付方式'] = '微信支付'  # 确保支付方式正确
        
        print(f"成功处理微信账单，有效记录数: {len(mapped_df)}")
        return mapped_df
    except Exception as e:
        print(f"读取微信账单出错: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    return mapped_df

def read_alipay_bill(file_path):
    """读取支付宝账单并处理数据"""
    print(f"读取支付宝账单: {os.path.basename(file_path)}")
    
    # 手动解析支付宝账单
    try:
        # 逐行读取文件
        with open(file_path, 'r', encoding='gbk') as f:
            lines = f.readlines()
        
        print(f"文件总行数: {len(lines)}")
        
        # 查找包含'交易时间'的行作为表头
        header_index = -1
        for i, line in enumerate(lines):
            if '交易时间' in line:
                header_index = i
                print(f"找到表头行: 第{header_index + 1}行")
                print(f"表头内容: {line.strip()}")
                break
        
        if header_index == -1:
            print("未找到支付宝账单表头")
            return None
        
        # 使用csv模块正确解析
        import csv
        from io import StringIO
        
        # 提取数据行
        data_rows = []
        valid_count = 0
        error_count = 0
        
        for i in range(header_index + 1, len(lines)):
            line = lines[i].strip()
            if line and not line.startswith('----') and not line.startswith('"----'):
                try:
                    # 使用csv模块正确解析行
                    reader = csv.reader(StringIO(line))
                    row = next(reader)
                    data_rows.append(row)
                    valid_count += 1
                except Exception as parse_error:
                    print(f"解析行{i+1}时出错: {parse_error}")
                    print(f"行内容: {line}")
                    error_count += 1
        
        print(f"解析结果: 有效行{valid_count}，错误行{error_count}")
        
        if data_rows:
            print(f"第一行数据(前6列): {data_rows[0][:6]}")
            if len(data_rows) > 1:
                print(f"第二行数据(前6列): {data_rows[1][:6]}")
        
        # 创建映射后的DataFrame
        mapped_df = pd.DataFrame(columns=CONFIG['merged_columns'] + CONFIG['hidden_columns'])
        mapped_df['来源'] = '支付宝'
        mapped_df['支付方式'] = '支付宝'  # 确保支付方式正确
        
        # 提取需要的字段
        processed_count = 0
        zero_amount_count = 0
        
        for row in data_rows:
            if len(row) >= 12:
                try:
                    # 处理交易时间
                    trade_time = pd.to_datetime(row[0].strip(), errors='coerce')
                    
                    # 处理金额
                    amount_str = row[6].strip()
                    amount_str = amount_str.replace('¥', '').replace(',', '')
                    amount = pd.to_numeric(amount_str, errors='coerce')
                    
                    if pd.isna(amount) or amount == 0:
                        zero_amount_count += 1
                    
                    # 添加记录
                    new_row = {
                        '交易时间': trade_time,
                        '交易类型': row[1].strip(),
                        '交易对方': row[2].strip(),
                        '商品/商品名称': row[4].strip() if len(row) > 4 else '',
                        '收/支': row[5].strip() if len(row) > 5 else '',
                        '金额': amount if not pd.isna(amount) else 0.0,
                        '支付方式': '支付宝',
                        '交易状态': row[8].strip() if len(row) > 8 else '',
                        '交易单号': row[9].strip() if len(row) > 9 else '',
                        '商户单号/商家订单号': row[10].strip() if len(row) > 10 else '',
                        '备注': row[11].strip() if len(row) > 11 else '',
                        '来源': '支付宝'
                    }
                    
                    # 添加到DataFrame
                    mapped_df = pd.concat([mapped_df, pd.DataFrame([new_row])], ignore_index=True)
                    processed_count += 1
                    
                except Exception as row_error:
                    print(f"处理记录时出错: {row_error}")
                    print(f"问题记录: {row[:6]}")
        
        # 交易状态标准化
        if '交易状态' in mapped_df.columns:
            def standardize_status(status):
                status = str(status).strip()
                if status == '退款':
                    return '退款'
                for standard, variations in STATUS_MAPPING.items():
                    if status in variations:
                        return standard
                return status
            
            mapped_df['交易状态'] = mapped_df['交易状态'].apply(standardize_status)
        
        # 数据质量统计
        valid_dates = mapped_df['交易时间'].count()
        valid_amounts = (mapped_df['金额'] != 0).sum()
        total_amount = mapped_df['金额'].sum()
        
        print(f"\n支付宝账单处理结果:")
        print(f"总处理记录: {processed_count}")
        print(f"交易时间有效: {valid_dates}")
        print(f"金额有效记录(非0): {valid_amounts}/{processed_count}")
        print(f"金额总和: {total_amount:.2f}")
        print(f"金额为0的记录: {zero_amount_count}")
        
        # 过滤掉完全空的记录
        mapped_df = mapped_df.dropna(subset=['交易时间', '交易类型', '交易对方'], how='all')
        
        print(f"成功读取支付宝账单，最终有效记录数: {len(mapped_df)}")
        return mapped_df
    except Exception as e:
        print(f"读取支付宝账单出错: {e}")
        import traceback
        traceback.print_exc()
        return None

def extract_month(date_str):
    """从日期字符串中提取月份"""
    if pd.isna(date_str):
        return None
    
    try:
        # 尝试直接解析datetime对象
        if isinstance(date_str, datetime):
            return date_str.strftime('%Y-%m')
        
        # 尝试解析字符串
        date = pd.to_datetime(date_str)
        return date.strftime('%Y-%m')
    except:
        # 使用正则表达式提取
        match = re.search(r'\d{4}[-/]?(1[0-2]|0?[1-9])', str(date_str))
        if match:
            year = match.group(0)[:4]
            month = match.group(1).zfill(2)
            return f"{year}-{month}"
        return None

def merge_bills(wechat_df, alipay_df):
    """合并微信和支付宝账单"""
    print("\n=== 开始合并账单 ===")
    
    # 数据质量检查和清洗
    def clean_and_validate(df, source_name):
        if df is None or df.empty:
            return None
        
        print(f"\n{source_name}账单处理:")
        
        # 确保所有必需列存在
        required_columns = CONFIG['merged_columns'] + CONFIG['hidden_columns']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # 统计
        valid_records = len(df)
        valid_dates = df['交易时间'].count()
        valid_amounts = (df['金额'] != 0).sum()
        total_amount = df['金额'].sum()
        
        print(f"  有效记录数: {valid_records}")
        print(f"  交易时间有效: {valid_dates}")
        print(f"  金额有效(非0): {valid_amounts}")
        print(f"  金额总和: {total_amount:.2f}")
        
        return df
    
    # 清洗两部分数据
    wechat_df = clean_and_validate(wechat_df, "微信")
    alipay_df = clean_and_validate(alipay_df, "支付宝")
    
    # 合并数据
    if wechat_df is not None and alipay_df is not None:
        merged_df = pd.concat([wechat_df, alipay_df], ignore_index=True)
        print(f"\n合并微信({len(wechat_df)})和支付宝({len(alipay_df)})账单")
    elif wechat_df is not None:
        merged_df = wechat_df
        print(f"\n仅合并微信账单({len(wechat_df)})")
    elif alipay_df is not None:
        merged_df = alipay_df
        print(f"\n仅合并支付宝账单({len(alipay_df)})")
    else:
        print("\n没有可合并的数据")
        return None
    
    # 按交易时间排序（从月初到月末）
    merged_df = merged_df.sort_values('交易时间')
    
    # 重置索引
    merged_df = merged_df.reset_index(drop=True)
    
    # 计算收支金额（支出为负值，收入为正值）用于Python中的验证和统计
    def calculate_income_expense(row):
        if row['收/支'] == '支出':
            return -row['金额']
        else:
            return row['金额']
    
    merged_df['收支金额'] = merged_df.apply(calculate_income_expense, axis=1)
    
    # 提取月份
    merged_df['月份'] = merged_df['交易时间'].apply(extract_month)
    
    # 最终数据质量报告
    print("\n=== 合并后数据质量报告 ===")
    print(f"总记录数: {len(merged_df)}")
    print(f"交易时间有效: {merged_df['交易时间'].count()}/{len(merged_df)}")
    print(f"金额有效(非0): {(merged_df['金额'] != 0).sum()}/{len(merged_df)}")
    print(f"金额总和: {merged_df['金额'].sum():.2f}")
    print(f"收支金额总和: {merged_df['收支金额'].sum():.2f}")
    print(f"微信记录: {(merged_df['来源'] == '微信').sum()}")
    print(f"支付宝记录: {(merged_df['来源'] == '支付宝').sum()}")
    
    # 检查关键字段缺失
    critical_fields = ['交易时间', '交易类型', '交易对方', '收/支']
    print("\n关键字段缺失情况:")
    for field in critical_fields:
        missing_count = merged_df[field].isnull().sum() + (merged_df[field] == '').sum()
        if missing_count > 0:
            missing_pct = (missing_count / len(merged_df)) * 100
            print(f"  {field}: {missing_count} ({missing_pct:.1f}%)")
    
    return merged_df

def save_single_file(merged_df, output_dir):
    """将所有月份的数据保存到单个Excel文件"""
    if merged_df is None:
        return
    
    # 移除临时列和隐藏列
    output_df = merged_df.drop(['月份', '来源'] + CONFIG['hidden_columns'], axis=1, errors='ignore')
    
    # 确保列的顺序正确
    output_columns = CONFIG['merged_columns']
    output_df = output_df[output_columns]
    
    # 生成文件名 - 合并导出时使用"总账单.xlsx"
    filename = "总账单.xlsx"
    
    output_file = os.path.join(output_dir, filename)
    
    # 保存为Excel文件，使用xlsxwriter进行高级格式化
    try:
        import xlsxwriter
        
        # 创建Excel writer
        writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
        
        # 写入数据（不包含索引）
        output_df.to_excel(writer, index=False, sheet_name='合并账单')
        
        # 获取workbook和worksheet对象
        workbook = writer.book
        worksheet = writer.sheets['合并账单']
        
        # 设置日期格式
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        
        # 设置会计专用格式（带人民币符号）
        accounting_format = workbook.add_format({'num_format': '_([$¥-804]* #,##0.00_);_([$¥-804]* -#,##0.00_);_([$¥-804]* "-"??_);_(@_)'})
        
        # 应用日期格式到第一列
        worksheet.set_column(0, 0, 20, date_format)
        
        # 获取列索引
        amount_col = output_df.columns.get_loc('金额')
        income_expense_col = output_df.columns.get_loc('收支金额')
        income_expense_col_letter = xlsxwriter.utility.xl_col_to_name(income_expense_col)
        amount_col_letter = xlsxwriter.utility.xl_col_to_name(amount_col)
        type_col = output_df.columns.get_loc('收/支')
        type_col_letter = xlsxwriter.utility.xl_col_to_name(type_col)
        
        # 获取数据行数
        num_rows = len(output_df)
        num_cols = len(output_df.columns)
        
        # 设置列宽和会计专用格式
        worksheet.set_column(amount_col, amount_col, 15, accounting_format)  # 金额列使用会计专用格式
        worksheet.set_column(income_expense_col, income_expense_col, 15, accounting_format)  # 收支金额列使用会计专用格式
        
        # 为每一行的收支金额设置Excel公式：=IF(收/支="支出", -金额, 金额)
        # 注意：这里会覆盖DataFrame中的数值，使用Excel公式
        for row_num in range(1, num_rows + 1):  # 从第2行开始（Excel索引从1开始）
            formula = f'=IF({type_col_letter}{row_num+1}="支出", -{amount_col_letter}{row_num+1}, {amount_col_letter}{row_num+1})'
            worksheet.write_formula(row_num, income_expense_col, formula, accounting_format)
        
        # 冻结首行
        worksheet.freeze_panes(1, 0)
        
        # 添加筛选功能
        worksheet.autofilter(0, 0, num_rows, num_cols - 1)
        
        # 添加SUBTOTAL公式计算收支金额总和
        subtotal_row = num_rows + 1
        # 第一列保持空白，不写"合计"文字
        subtotal_formula = f'=SUBTOTAL(9,{income_expense_col_letter}2:{income_expense_col_letter}{num_rows + 1})'
        worksheet.write(subtotal_row, income_expense_col, subtotal_formula, accounting_format)  # 使用会计专用格式
        
        # 保存文件
        writer.close()
        
        print(f"\n已保存到单个文件: {output_file}")
        print(f"  记录数: {len(output_df)}")
        print(f"  金额统计: 总计{output_df['金额'].sum():.2f}元")
        print(f"  收支金额总计: {output_df['收支金额'].sum():.2f}元")
        print(f"  微信记录: {(merged_df['来源'] == '微信').sum()}")
        print(f"  支付宝记录: {(merged_df['来源'] == '支付宝').sum()}")
        print(f"  首行已冻结，筛选功能已开启")
        print(f"  日期格式已设置，金额列已应用会计专用格式")
        print(f"  收支金额列已添加，SUBTOTAL公式已计算")
        
    except Exception as e:
        print(f"保存文件出错 {output_file}: {e}")
        import traceback
        traceback.print_exc()


def save_by_month(merged_df, output_dir):
    """按月份保存合并后的账单"""
    if merged_df is None:
        return
    
    # 数据最终验证
    print("\n=== 保存前最终数据验证 ===")
    print(f"数据类型:")
    print(merged_df.dtypes)
    
    # 创建输出目录（如果不存在）
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 按月份分组并保存
    months = merged_df['月份'].unique()
    print(f"\n保存月份: {sorted([m for m in months if m is not None])}")
    
    for month in months:
        if month is not None:
            month_df = merged_df[merged_df['月份'] == month]
            
            # 移除临时列和隐藏列
            output_df = month_df.drop(['月份', '来源'] + CONFIG['hidden_columns'], axis=1, errors='ignore')
            
            # 确保列的顺序正确
            output_columns = CONFIG['merged_columns']
            output_df = output_df[output_columns]
            
            # 生成文件名
            month_name = month_str_to_chinese(month)
            output_file = os.path.join(output_dir, f"{month_name}账单.xlsx")
            
            # 保存为Excel文件，使用xlsxwriter进行高级格式化
            try:
                import xlsxwriter
                
                # 创建Excel writer
                writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
                
                # 写入数据（不包含索引）
                output_df.to_excel(writer, sheet_name='账单明细', index=False)
                
                # 获取工作簿和工作表
                workbook = writer.book
                worksheet = writer.sheets['账单明细']
                
                # 获取数据范围
                num_rows = len(output_df)
                num_cols = len(output_df.columns)
                
                # 设置列宽
                column_widths = {
                    '交易时间': 20,
                    '交易类型': 15,
                    '交易对方': 25,
                    '商品/商品名称': 30,
                    '收/支': 8,
                    '金额': 15,
                    '收支金额': 15,
                    '支付方式': 12,
                    '交易状态': 12
                }
                
                for col_idx, col_name in enumerate(output_df.columns):
                    if col_name in column_widths:
                        worksheet.set_column(col_idx, col_idx, column_widths[col_name])
                    else:
                        worksheet.set_column(col_idx, col_idx, 15)
                
                # 创建格式
                date_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm'})
                # 添加会计专用格式，确保Excel识别为会计专用格式
                # 修改会计专用格式，让负数使用负号而不是括号
                accounting_format = workbook.add_format({'num_format': '_([$¥-804]* #,##0.00_);_([$¥-804]* -#,##0.00_);_([$¥-804]* "-"??_);_(@_)'})
                
                # 应用日期格式到第一列
                worksheet.set_column(0, 0, 20, date_format)
                
                # 获取列索引
                amount_col = output_df.columns.get_loc('金额')
                income_expense_col = output_df.columns.get_loc('收支金额')
                income_expense_col_letter = xlsxwriter.utility.xl_col_to_name(income_expense_col)
                amount_col_letter = xlsxwriter.utility.xl_col_to_name(amount_col)
                type_col = output_df.columns.get_loc('收/支')
                type_col_letter = xlsxwriter.utility.xl_col_to_name(type_col)
                
                # 设置列宽和会计专用格式
                worksheet.set_column(amount_col, amount_col, 15, accounting_format)  # 金额列使用会计专用格式
                worksheet.set_column(income_expense_col, income_expense_col, 15, accounting_format)  # 收支金额列使用会计专用格式
                
                # 为每一行的收支金额设置Excel公式：=IF(收/支="支出", -金额, 金额)
                # 注意：这里会覆盖DataFrame中的数值，使用Excel公式
                for row_num in range(1, num_rows + 1):  # 从第2行开始（Excel索引从1开始）
                    formula = f'=IF({type_col_letter}{row_num+1}="支出", -{amount_col_letter}{row_num+1}, {amount_col_letter}{row_num+1})'
                    worksheet.write_formula(row_num, income_expense_col, formula, accounting_format)
                
                # 冻结首行
                worksheet.freeze_panes(1, 0)
                
                # 添加筛选功能
                worksheet.autofilter(0, 0, num_rows, num_cols - 1)
                
                # 添加SUBTOTAL公式计算收支金额总和
                subtotal_row = num_rows + 1
                # 第一列保持空白，不写"合计"文字
                subtotal_formula = f'=SUBTOTAL(9,{income_expense_col_letter}2:{income_expense_col_letter}{num_rows + 1})'
                worksheet.write(subtotal_row, income_expense_col, subtotal_formula, accounting_format)  # 使用会计专用格式
                
                # 保存文件
                writer.close()
                
                print(f"\n已保存: {output_file}")
                print(f"  记录数: {len(output_df)}")
                print(f"  金额统计: 总计{output_df['金额'].sum():.2f}元")
                print(f"  收支金额总计: {output_df['收支金额'].sum():.2f}元")
                print(f"  微信记录: {(month_df['来源'] == '微信').sum()}")
                print(f"  支付宝记录: {(month_df['来源'] == '支付宝').sum()}")
                print(f"  首行已冻结，筛选功能已开启")
                print(f"  日期格式已设置，金额列已应用会计专用格式")
                print(f"  收支金额列已添加，SUBTOTAL公式已计算")
                
            except Exception as e:
                print(f"保存文件出错 {output_file}: {e}")
                import traceback
                traceback.print_exc()

def month_str_to_chinese(month_str):
    """将月份字符串转换为中文格式"""
    try:
        date = datetime.strptime(month_str, '%Y-%m')
        return date.strftime('%Y年%m月')
    except:
        return month_str

def validate_merge_integrity(wechat_df, alipay_df, merged_df):
    """验证合并前后的数据一致性"""
    print("\n=== 合并完整性验证 ===")
    
    # 验证记录数
    expected_records = 0
    if wechat_df is not None:
        expected_records += len(wechat_df)
    if alipay_df is not None:
        expected_records += len(alipay_df)
    
    actual_records = len(merged_df)
    print(f"预期记录数: {expected_records}")
    print(f"实际记录数: {actual_records}")
    
    if expected_records == actual_records:
        print("✓ 记录数完全匹配")
    else:
        print(f"✗ 记录数不匹配，差异: {abs(expected_records - actual_records)}")
    
    # 验证金额总和
    expected_amount = 0
    if wechat_df is not None:
        expected_amount += wechat_df['金额'].sum()
    if alipay_df is not None:
        expected_amount += alipay_df['金额'].sum()
    
    actual_amount = merged_df['金额'].sum()
    print(f"\n预期总金额: {expected_amount:.2f}")
    print(f"实际总金额: {actual_amount:.2f}")
    
    if abs(expected_amount - actual_amount) < 0.01:
        print("✓ 总金额完全匹配")
    else:
        print(f"✗ 总金额不匹配，差异: {abs(expected_amount - actual_amount):.2f}")
    
    # 验证收支金额总和（使用与实际计算相同的逻辑）
    def calculate_expected_income_expense(df):
        if df is None:
            return 0
        
        # 与实际计算相同的逻辑：支出为负，其他为正
        expected_income_expense = 0
        for _, row in df.iterrows():
            if row['收/支'] == '支出':
                expected_income_expense -= row['金额']
            else:
                expected_income_expense += row['金额']
        return expected_income_expense
    
    expected_income_expense = 0
    if wechat_df is not None:
        expected_income_expense += calculate_expected_income_expense(wechat_df)
    if alipay_df is not None:
        expected_income_expense += calculate_expected_income_expense(alipay_df)
    
    actual_income_expense = merged_df['收支金额'].sum()
    print(f"\n预期收支金额: {expected_income_expense:.2f}")
    print(f"实际收支金额: {actual_income_expense:.2f}")
    
    if abs(expected_income_expense - actual_income_expense) < 0.01:
        print("✓ 收支金额完全匹配")
    else:
        print(f"✗ 收支金额不匹配，差异: {abs(expected_income_expense - actual_income_expense):.2f}")
    
    # 验证来源分布
    if wechat_df is not None and alipay_df is not None:
        expected_wechat = len(wechat_df)
        expected_alipay = len(alipay_df)
        actual_wechat = (merged_df['来源'] == '微信').sum()
        actual_alipay = (merged_df['来源'] == '支付宝').sum()
        
        print(f"\n预期微信记录: {expected_wechat}")
        print(f"实际微信记录: {actual_wechat}")
        print(f"预期支付宝记录: {expected_alipay}")
        print(f"实际支付宝记录: {actual_alipay}")
        
        if expected_wechat == actual_wechat and expected_alipay == actual_alipay:
            print("✓ 来源分布完全匹配")
        else:
            print("✗ 来源分布不匹配")
    
    return expected_records == actual_records and abs(expected_amount - actual_amount) < 0.01

def main():
    """主函数"""
    # 获取当前目录
    current_dir = os.getcwd()
    print(f"当前工作目录: {current_dir}")
    
    # 查找账单文件
    wechat_files, alipay_files = find_bill_files(current_dir)
    
    print(f"\n找到的账单文件:")
    print(f"微信账单: {len(wechat_files)}个")
    for file in wechat_files:
        print(f"  - {os.path.basename(file)}")
    print(f"支付宝账单: {len(alipay_files)}个")
    for file in alipay_files:
        print(f"  - {os.path.basename(file)}")
    
    if not wechat_files and not alipay_files:
        print("\n未找到任何账单文件！")
        return
    
    # 账单导出方式选择
    print("\n账单导出方式：")
    print("1  按月份分开导出（直接按回车键） / 2  所有月份合并导出（输入'2'后按回车键）")
    print("")
    user_choice = input("请选择导出方式（直接回车选1 / 输入'2'选2）: ")
    
    # 处理用户选择
    if user_choice.strip() == '2':
        pass  # 选择合并导出
    else:
        user_choice = ''  # 默认为按月份导出（直接回车）
    
    # 读取微信账单
    wechat_df_list = []
    for file in wechat_files:
        df = read_wechat_bill(file)
        if df is not None:
            wechat_df_list.append(df)
    
    if wechat_df_list:
        wechat_df = pd.concat(wechat_df_list, ignore_index=True)
        print(f"\n微信账单汇总: {len(wechat_df)}条记录")
        print(f"微信账单总金额: {wechat_df['金额'].sum():.2f}元")
    else:
        wechat_df = None
        print("\n未读取到微信账单数据")
    
    # 读取支付宝账单
    alipay_df_list = []
    for file in alipay_files:
        df = read_alipay_bill(file)
        if df is not None:
            alipay_df_list.append(df)
    
    if alipay_df_list:
        alipay_df = pd.concat(alipay_df_list, ignore_index=True)
        print(f"支付宝账单汇总: {len(alipay_df)}条记录")
        print(f"支付宝账单总金额: {alipay_df['金额'].sum():.2f}元")
    else:
        alipay_df = None
        print("未读取到支付宝账单数据")
    
    # 合并账单
    merged_df = merge_bills(wechat_df, alipay_df)
    
    if merged_df is not None:
        print(f"\n合并后总记录数: {len(merged_df)}")
        print(f"涉及月份: {sorted(merged_df['月份'].unique())}")
        
        # 验证合并完整性
        is_valid = validate_merge_integrity(wechat_df, alipay_df, merged_df)
        
        # 根据用户选择导出方式
        export_choice = user_choice  # 使用之前的选择
        
        if not is_valid:
            print("\n⚠️  合并数据存在不一致，请检查！")
            confirm_continue = input("是否继续保存文件？(y/n，直接回车确认): ")
            if confirm_continue.strip().lower() != '' and confirm_continue.lower() != 'y':
                print("保存操作已取消")
                return
            else:
                print("\n⚠️  虽然数据不一致，但您选择继续保存文件")
                # 根据用户选择保存文件
                if export_choice.strip() == '':
                    save_by_month(merged_df, current_dir)
                else:
                    save_single_file(merged_df, current_dir)
                print("\n✅ 账单合并处理完成（但数据验证存在问题）！")
                print("� 合并结果验证发现问题，请仔细检查数据")
                print("💾 账单文件已保存到当前目录")
                print("🎨 已应用首行冻结、筛选功能、日期格式和会计专用格式")
                print("💰 收支金额列已添加，SUBTOTAL公式已计算")
                print("📋 交易状态已标准化，时间已按月份排序")
                print("\n请在Excel中打开文件并仔细检查数据！")
                return
        
        # 根据用户选择保存文件
        if export_choice.strip() == '':
            save_by_month(merged_df, current_dir)
        else:
            save_single_file(merged_df, current_dir)
        
        if not is_valid:
            print("\n⚠️  合并数据存在不一致，请检查！")
            confirm_continue = input("是否继续保存文件？(y/n，直接回车确认): ")
            if confirm_continue.strip().lower() != '' and confirm_continue.lower() != 'y':
                print("保存操作已取消")
                return
            else:
                print("\n⚠️  虽然数据不一致，但您选择继续保存文件")
                # 根据用户选择保存文件
                if export_choice.strip() == '':
                    save_by_month(merged_df, current_dir)
                else:
                    save_single_file(merged_df, current_dir)
                print("\n✅ 账单合并处理完成（但数据验证存在问题）！")
                print("📊 合并结果验证发现问题，请仔细检查数据")
                print("💾 账单文件已保存到当前目录")
                print("🎨 已应用首行冻结、筛选功能、日期格式和会计专用格式")
                print("💰 收支金额列已添加，SUBTOTAL公式已计算")
                print("📋 交易状态已标准化，时间已按月份排序")
                print("\n请在Excel中打开文件并仔细检查数据！")
                return
        
        # 根据用户选择保存文件
        if export_choice.strip() == '':
            save_by_month(merged_df, current_dir)
        else:
            save_single_file(merged_df, current_dir)
        
        # 只有验证通过才显示成功消息
        print("\n✅ 账单合并处理完成！")
        print("📊 合并结果已验证，数据完全一致")
        print("💾 账单文件已保存到当前目录")
        print("🎨 已应用首行冻结、筛选功能、日期格式和会计专用格式")
        print("💰 收支金额列已添加，SUBTOTAL公式已计算")
        print("📋 交易状态已标准化，时间已按月份排序")
        print("\n请在Excel中打开文件查看详细内容。")
    else:
        print("\n没有可合并的数据")


if __name__ == "__main__":
    main()