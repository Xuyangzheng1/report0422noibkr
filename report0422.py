import pandas as pd
import numpy as np
import datetime as dt
import yfinance as yf
import time
import logging
import os
import json
import random
import traceback
import requests
from prettytable import PrettyTable
from ib_insync import *

# 设置日志
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("earnings_reversal.log"), 
                              logging.StreamHandler()])
logger = logging.getLogger("EarningsReversal")

def log_info(message):
    logger.info(message)

def log_error(message):
    logger.error(message)

def log_debug(message):
    logger.debug(message)

def log_warning(message):
    logger.warning(message)

def log_trade(message):
    logger.info(f"TRADE: {message}")

# 配置 requests 添加自定义用户代理，解决 429 错误
original_get = requests.Session.get

def get_with_user_agent(self, *args, **kwargs):
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
        
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ]
    kwargs['headers']['User-Agent'] = random.choice(user_agents)
    
    # 添加短暂随机延迟，避免请求过于频繁
    delay = random.uniform(0.5, 1.0)  # 短暂延迟，避免API超时
    time.sleep(delay)
    
    # 调用原始方法
    return original_get(self, *args, **kwargs)

# 替换 Session.get 方法
requests.Session.get = get_with_user_agent

class EarningsDataCollector:
    """收益公告数据收集器 - 使用yfinance直接获取股票收益数据，增强错误处理"""
    
    def __init__(self, min_price=5.0, min_volume=100000, exclude_otc=True):
        """
        初始化数据收集器
        
        参数:
            min_price: 最低股票价格筛选，默认5美元
            min_volume: 最低日均交易量筛选，默认10万股
            exclude_otc: 是否排除场外交易股票，默认True
        """
        # 创建数据目录
        os.makedirs('data', exist_ok=True)
        self.min_price = min_price
        self.min_volume = min_volume
        self.exclude_otc = exclude_otc
        
        # 创建缓存目录
        cache_dir = os.path.join('data', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_file = os.path.join(cache_dir, 'market_cap_cache.json')
        self.market_cap_cache = self._load_market_cap_cache()
        
        # 加载预定义股票列表
        self.stock_universe = self._load_stock_universe()
        
    def _load_market_cap_cache(self):
        """加载市值缓存"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            log_error(f"加载市值缓存出错: {e}")
            return {}
            
    def _save_market_cap_cache(self):
        """保存市值缓存"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.market_cap_cache, f)
        except Exception as e:
            log_error(f"保存市值缓存出错: {e}")
    
    def _load_stock_universe(self):
        """加载预定义股票列表"""
        try:
            # 尝试加载标普500成分股
            try:
                # 尝试使用pandas_datareader获取标普500成分股
                import pandas_datareader.data as web
                try:
                    # 尝试获取标普500指数数据
                    sp500 = web.get_data_yahoo('^GSPC')
                    log_info(f"成功获取标普500指数数据")
                    
                    # 由于无法直接获取成分股，使用预定义列表
                    raise NotImplementedError("需要使用预定义的股票列表")
                except Exception as e:
                    log_warning(f"使用pandas_datareader获取标普500成分股出错: {e}")
                    raise
            except:
                # 使用预定义的主要股票列表
                sp500_tickers = [
                     "A", "AAL", "AAP", "AAPL", "ABBV", "ABC", "ABMD", "ABT", "ACN", "ADBE", 
            "ADI", "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIZ", 
            "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "AMAT", "AMCR", "AMD", 
            "AME", "AMGN", "AMP", "AMT", "AMZN", "ANSS", "ANTM", "AON", "AOS", "APA", 
            "APD", "APH", "APTV", "ARE", "ATO", "ATVI", "AVB", "AVGO", "AVY", "AWK", 
            "AXP", "AZO", "BA", "BAC", "BAX", "BBY", "BDX", "BEN", "BF.B", "BIIB", 
            "BIO", "BK", "BKNG", "BKR", "BLK", "BLL", "BMY", "BR", "BRK.B", "BSX", 
            "BWA", "BXP", "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE", "CBRE", 
            "CCI", "CDNS", "CDW", "CE", "CERN", "CF", "CFG", "CHD", "CHRW", "CHTR", 
            "CI", "CINF", "CL", "CLX", "CMA", "CMCSA", "CME", "CMG", "CMI", "CMS", 
            "CNC", "CNP", "COF", "COG", "COO", "COP", "COST", "CPB", "CPRT", "CRM", 
            "CSCO", "CSX", "CTAS", "CTLT", "CTSH", "CTVA", "CTXS", "CVS", "CVX", "CZR", 
            "D", "DAL", "DD", "DE", "DFS", "DG", "DGX", "DHI", "DHR", "DIS", 
            "DISCA", "DISCK", "DISH", "DLR", "DLTR", "DOV", "DOW", "DPZ", "DRE", "DRI", 
            "DTE", "DUK", "DVA", "DVN", "DXC", "DXCM", "EA", "EBAY", "ECL", "ED", 
            "EFX", "EIX", "EL", "EMN", "EMR", "ENPH", "EOG", "EQIX", "EQR", "ES", 
            "ESS", "ETN", "ETR", "ETSY", "EVRG", "EW", "EXC", "EXPD", "EXPE", "EXR", 
            "F", "FANG", "FAST", "FB", "FBHS", "FCX", "FDX", "FE", "FFIV", "FIS", 
            "FISV", "FITB", "FLT", "FMC", "FOX", "FOXA", "FRC", "FRT", "FTNT", "FTV", 
            "GD", "GE", "GILD", "GIS", "GL", "GLW", "GM", "GNRC", "GOOG", "GOOGL", 
            "GPC", "GPN", "GPS", "GRMN", "GS", "GWW", "HAL", "HAS", "HBAN", "HBI", 
            "HCA", "HD", "HES", "HIG", "HII", "HLT", "HOLX", "HON", "HPE", "HPQ", 
            "HRL", "HSIC", "HST", "HSY", "HUM", "HWM", "IBM", "ICE", "IDXX", "IEX", 
            "IFF", "ILMN", "INCY", "INFO", "INTC", "INTU", "IP", "IPG", "IPGP", "IQV", 
            "IR", "IRM", "ISRG", "IT", "ITW", "IVZ", "J", "JBHT", "JCI", "JKHY", 
            "JNJ", "JNPR", "JPM", "K", "KEY", "KEYS", "KHC", "KIM", "KLAC", "KMB", 
            "KMI", "KMX", "KO", "KR", "KSU", "L", "LB", "LDOS", "LEG", "LEN", 
            "LH", "LHX", "LIN", "LKQ", "LLY", "LMT", "LNC", "LNT", "LOW", "LRCX", 
            "LUMN", "LUV", "LVS", "LW", "LYB", "LYV", "MA", "MAA", "MAR", "MAS", 
            "MCD", "MCHP", "MCK", "MCO", "MDLZ", "MDT", "MET", "MGM", "MHK", "MKC", 
            "MKTX", "MLM", "MMC", "MMM", "MNST", "MO", "MOS", "MPC", "MPWR", "MRK", 
            "MRO", "MS", "MSCI", "MSFT", "MSI", "MTB", "MTD", "MU", "NCLH", "NDAQ", 
            "NEE", "NEM", "NFLX", "NI", "NKE", "NLOK", "NLSN", "NOC", "NOV", "NOW", 
            "NRG", "NSC", "NTAP", "NTRS", "NUE", "NVDA", "NVR", "NWL", "NWS", "NWSA", 
            "NXPI", "O", "ODFL", "OGN", "OKE", "OMC", "ORCL", "ORLY", "OTIS", "OXY", 
            "PAYC", "PAYX", "PBCT", "PCAR", "PEAK", "PEG", "PENN", "PEP", "PFE", "PFG", 
            "PG", "PGR", "PH", "PHM", "PKG", "PKI", "PLD", "PM", "PNC", "PNR", 
            "PNW", "POOL", "PPG", "PPL", "PRGO", "PRU", "PSA", "PSX", "PTC", "PVH", 
            "PWR", "PXD", "PYPL", "QCOM", "QRVO", "RCL", "RE", "REG", "REGN", "RF", 
            "RHI", "RJF", "RL", "RMD", "ROK", "ROL", "ROP", "ROST", "RSG", "RTX", 
            "SBAC", "SBUX", "SCHW", "SEE", "SHW", "SIVB", "SJM", "SLB", "SNA", "SNPS", 
            "SO", "SPG", "SPGI", "SRE", "STE", "STT", "STX", "STZ", "SWK", "SWKS", 
            "SYF", "SYK", "SYY", "T", "TAP", "TDG", "TDY", "TECH", "TEL", "TER", 
            "TFC", "TFX", "TGT", "TJX", "TMO", "TMUS", "TPR", "TRMB", "TROW", "TRV", 
            "TSCO", "TSLA", "TSN", "TT", "TTWO", "TWTR", "TXN", "TXT", "TYL", "UA", 
            "UAA", "UAL", "UDR", "UHS", "ULTA", "UNH", "UNM", "UNP", "UPS", "URI", 
            "USB", "V", "VFC", "VIAC", "VLO", "VMC", "VNO", "VRSK", "VRSN", "VRTX", 
            "VTR", "VTRS", "VZ", "WAB", "WAT", "WBA", "WDC", "WEC", "WELL", "WFC", 
            "WHR", "WLTW", "WM", "WMB", "WMT", "WRB", "WRK", "WST", "WU", "WY", 
            "WYNN", "XEL", "XLNX", "XOM", "XRAY", "XYL", "YUM", "ZBH", "ZBRA", "ZION", 
            "ZTS"
                ]
                log_info(f"使用预定义股票列表，包含{len(sp500_tickers)}只股票")
            
            # 返回股票池
            return sp500_tickers
            
        except Exception as e:
            log_error(f"加载股票池出错: {e}")
            log_error(traceback.format_exc())
            # 返回一些知名股票作为备选
            return ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "JPM", "JNJ", "V", "PG"]

    def get_upcoming_earnings(self, days_ahead=30):
        """
        获取即将到来的收益公告
        
        参数:
            days_ahead: 未来几天的收益公告
            
        返回:
            DataFrame: 收益公告数据
        """
        try:
            log_info(f"获取未来{days_ahead}天的收益公告数据...")
            
            # 获取当前日期
            today = dt.datetime.now().date()
            end_date = today + dt.timedelta(days=days_ahead)
            
            # 准备收益数据列表
            earnings_data = []
            
            # 处理每只股票
            for i, ticker in enumerate(self.stock_universe):
                # 输出进度
                if (i+1) % 10 == 0 or i == 0:
                    log_info(f"正在处理第 {i+1}/{len(self.stock_universe)} 只股票")
                    
                try:
                    # 获取股票对象
                    stock = yf.Ticker(ticker)
                    
                    # 获取收益信息
                    try:
                        # 尝试获取下一个收益日期
                        info = stock.info
                        earnings_date = None
                        
                        # 尝试各种可能的键名
                        if 'earningsDate' in info:
                            # 获取earnings_date值
                            earnings_date_value = info['earningsDate']
                            
                            # 判断类型并适当处理
                            if isinstance(earnings_date_value, list) and len(earnings_date_value) > 0:
                                # 如果是列表，取第一个元素
                                earnings_date = earnings_date_value[0]
                            elif hasattr(earnings_date_value, '__len__') and len(earnings_date_value) > 0:
                                # 如果是numpy数组或类似的序列类型，取第一个元素 (通过索引访问)
                                earnings_date = earnings_date_value[0]
                            else:
                                # 其他情况直接使用
                                earnings_date = earnings_date_value
                        elif 'nextEarningsDate' in info:
                            earnings_date = info['nextEarningsDate']
                        else:
                            # 尝试获取公司日历
                            try:
                                calendar = stock.calendar
                                
                                # 安全处理不同类型的calendar对象
                                if calendar is not None:
                                    # 如果是字典
                                    if isinstance(calendar, dict):
                                        if 'Earnings Date' in calendar:
                                            calendar_value = calendar['Earnings Date']
                                            # 检查是否是numpy数组
                                            if hasattr(calendar_value, '__len__') and not isinstance(calendar_value, (str, dict)):
                                                earnings_date = calendar_value[0] if len(calendar_value) > 0 else None
                                            else:
                                                earnings_date = calendar_value
                                        elif 'earningsDate' in calendar:
                                            calendar_value = calendar['earningsDate']
                                            # 检查是否是numpy数组
                                            if hasattr(calendar_value, '__len__') and not isinstance(calendar_value, (str, dict)):
                                                earnings_date = calendar_value[0] if len(calendar_value) > 0 else None
                                            else:
                                                earnings_date = calendar_value
                                    # 如果是DataFrame
                                    elif isinstance(calendar, pd.DataFrame):
                                        if 'Earnings Date' in calendar.index:
                                            earnings_date = calendar.loc['Earnings Date'].iloc[0]
                                        elif 'earningsDate' in calendar.index:
                                            earnings_date = calendar.loc['earningsDate'].iloc[0]
                                    # 如果是Series
                                    elif isinstance(calendar, pd.Series):
                                        if 'Earnings Date' in calendar.index:
                                            earnings_date = calendar['Earnings Date']
                                        elif 'earningsDate' in calendar.index:
                                            earnings_date = calendar['earningsDate']
                            except Exception as cal_err:
                                log_debug(f"获取{ticker}日历出错: {cal_err}")
                        
                        # 如果成功获取到收益日期
                        if earnings_date is not None:
                            # 转换为datetime
                            try:
                                earnings_date = pd.to_datetime(earnings_date)
                            except Exception as date_err:
                                log_debug(f"{ticker}收益日期格式转换失败: {earnings_date}, 错误: {date_err}")
                                continue
                            
                            # 检查日期是否在范围内
                            if today <= earnings_date.date() <= end_date:
                                # 获取股票信息
                                company_name = info.get('shortName', info.get('longName', ticker))
                                market_cap = info.get('marketCap', None)
                                price = info.get('regularMarketPrice', info.get('currentPrice', None))
                                volume = info.get('averageDailyVolume10Day', info.get('volume', None))
                                
                                # 添加到结果列表
                                earnings_data.append({
                                    'symbol': ticker,
                                    'company_name': company_name,
                                    'earnings_date': earnings_date,
                                    'estimated_eps': info.get('trailingEps', None),
                                    'market_cap': market_cap,
                                    'price': price,
                                    'volume': volume,
                                    'time_of_day': "After Market Close" if earnings_date.hour >= 12 else "Before Market Open"
                                })
                                
                                log_info(f"找到{ticker}的收益公告: {earnings_date.strftime('%Y-%m-%d')}")
                    
                    except Exception as e:
                        log_error(f"处理{ticker}收益数据时出错: {str(e)}")
                        continue
                        
                    # 添加随机延迟，避免请求过度
                    time.sleep(random.uniform(2.0, 4.0))
                    
                except Exception as e:
                    log_error(f"处理{ticker}时出错: {str(e)}")
                    continue
                    
            # 转换为DataFrame
            df = pd.DataFrame(earnings_data)
            
            if df.empty:
                log_info("未找到任何即将到来的收益公告")
                return df
            
            # 保存原始数据
            output_file = os.path.join('data', 'earnings_calendar_raw.csv')
            df.to_csv(output_file, index=False)
            log_info(f"成功获取并保存 {len(df)} 条收益公告数据到 {output_file}")
            
            return df
                
        except Exception as e:
            log_error(f"获取收益公告数据出错: {str(e)}")
            log_error(traceback.format_exc())
            return pd.DataFrame()
    
    def get_earnings_data(self, days_ahead=30, force_update=False):
        """
        主方法，获取收益公告数据 - 移除模拟数据，只使用真实数据
        
        参数:
            days_ahead: 获取未来多少天的数据
            force_update: 是否强制更新数据，即使本地已有今天的数据
            
        返回:
            DataFrame: 收益公告数据
        """
        log_info("开始获取收益公告数据...")
        
        # 首先尝试从本地文件加载，如果存在且在有效期内并且不强制更新
        filtered_file = os.path.join('data', 'filtered_earnings_calendar.csv')
        if os.path.exists(filtered_file) and not force_update:
            try:
                # 检查文件修改时间，如果是今天创建的，则直接使用
                file_mtime = dt.datetime.fromtimestamp(os.path.getmtime(filtered_file)).date()
                if file_mtime == dt.datetime.now().date():
                    log_info(f"使用今天的本地缓存数据: {filtered_file}")
                    df = pd.read_csv(filtered_file)
                    if 'earnings_date' in df.columns:
                        df['earnings_date'] = pd.to_datetime(df['earnings_date'])
                    
                    if not df.empty:
                        log_info(f"从本地加载了{len(df)}条收益公告数据")
                        return df
            except Exception as e:
                log_warning(f"加载本地缓存数据出错: {e}")
        
        # 如果强制更新或本地没有今天的数据，从Yahoo Finance获取数据
        if force_update:
            log_info("强制更新收益公告数据...")
        else:
            log_info("本地数据不存在或非今日数据，开始获取新数据...")
        
        # 从Yahoo Finance获取数据
        df = self.get_upcoming_earnings(days_ahead)
        
        if df.empty:
            log_warning("未获取到任何收益公告数据，尝试使用备份数据")
            # 尝试使用备份数据
            backup_file = os.path.join('data', 'earnings_backup.csv')
            if os.path.exists(backup_file):
                try:
                    backup_df = pd.read_csv(backup_file)
                    if 'earnings_date' in backup_df.columns:
                        backup_df['earnings_date'] = pd.to_datetime(backup_df['earnings_date'])
                    log_info(f"从备份文件加载了{len(backup_df)}条数据")
                    return backup_df
                except Exception as e:
                    log_error(f"加载备份数据出错: {e}")
            
            # 如果没有备份或加载失败
            log_error("无法获取收益公告数据，且没有可用的备份数据")
            # 返回空DataFrame，不使用模拟数据
            return pd.DataFrame()
            
        # 如果成功获取了数据，筛选有效股票
        filtered_df = self.filter_valid_stocks(df)
        
        # 保存为备份，以便下次使用
        if not filtered_df.empty:
            filtered_df.to_csv(os.path.join('data', 'earnings_backup.csv'), index=False)
            log_info(f"已将{len(filtered_df)}条收益公告数据保存为备份")
        
        return filtered_df
    
    def filter_valid_stocks(self, df):
        """
        筛选有效的股票并以追加方式保存数据
        
        参数:
            df: DataFrame，包含股票数据
                
        返回:
            DataFrame: 筛选后的数据
        """
        if df.empty:
            return df
        
        log_info("开始筛选有效股票...")
        original_count = len(df)
        
        # 1. 过滤NaN值
        df = df.dropna(subset=['symbol']).copy()
        
        # 2. 过滤OTC/粉单市场股票 (通常股票代码包含特殊字符)
        if self.exclude_otc:
            # 检查字符模式，如5个字母以上、包含点号等
            otc_pattern = r'^\w{5,}$|\.|\-'
            
            # 使用正则表达式过滤股票代码
            mask = ~df['symbol'].str.contains(otc_pattern, regex=True, na=False)
            df = df[mask].copy()
            log_info(f"过滤OTC/粉单股票后剩余 {len(df)} 条记录 (原 {original_count})")
        
        # 3. 过滤低价股
        if 'price' in df.columns and self.min_price > 0:
            # 先转换价格确保为数值类型
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df[df['price'] >= self.min_price].copy()
            log_info(f"过滤低于 ${self.min_price} 价格的股票后剩余 {len(df)} 条记录")
        
        # 4. 过滤低交易量股票
        if 'volume' in df.columns and self.min_volume > 0:
            # 转换交易量确保为数值类型
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df = df[df['volume'] >= self.min_volume].copy()
            log_info(f"过滤日交易量低于 {self.min_volume} 的股票后剩余 {len(df)} 条记录")
        
        log_info(f"筛选完成：从 {original_count} 条记录筛选到 {len(df)} 条有效记录")
        
        # 保存筛选后的结果
        if not df.empty:
            output_file = os.path.join('data', 'filtered_earnings_calendar.csv')
            
            # 检查文件是否存在
            file_exists = os.path.exists(output_file)
            
            if file_exists:
                # 如果文件存在，先读取现有数据
                try:
                    existing_df = pd.read_csv(output_file)
                    
                    # 确保日期列是日期类型
                    if 'earnings_date' in existing_df.columns:
                        existing_df['earnings_date'] = pd.to_datetime(existing_df['earnings_date'])
                    if 'earnings_date' in df.columns:
                        df['earnings_date'] = pd.to_datetime(df['earnings_date'])
                    
                    # 创建用于检测重复的键
                    if 'earnings_date' in df.columns and 'symbol' in df.columns:
                        # 使用股票代码和收益日期的组合作为唯一标识
                        df['unique_key'] = df['symbol'] + '_' + df['earnings_date'].dt.strftime('%Y-%m-%d')
                        existing_df['unique_key'] = existing_df['symbol'] + '_' + existing_df['earnings_date'].dt.strftime('%Y-%m-%d')
                        
                        # 找出新数据中不存在于现有数据中的记录
                        new_records = df[~df['unique_key'].isin(existing_df['unique_key'])]
                        
                        if not new_records.empty:
                            # 删除辅助列
                            if 'unique_key' in new_records.columns:
                                new_records = new_records.drop('unique_key', axis=1)
                            
                            # 合并数据
                            combined_df = pd.concat([existing_df.drop('unique_key', axis=1), new_records], ignore_index=True)
                            
                            # 按收益日期排序
                            if 'earnings_date' in combined_df.columns:
                                combined_df = combined_df.sort_values('earnings_date')
                            
                            # 保存合并后的数据
                            combined_df.to_csv(output_file, index=False)
                            log_info(f"追加了 {len(new_records)} 条新记录到 {output_file}，总计 {len(combined_df)} 条记录")
                            
                            # 返回合并后的数据
                            return combined_df
                        else:
                            log_info(f"没有发现新的收益公告记录，保持原有 {len(existing_df)} 条记录不变")
                            
                            # 删除辅助列并返回现有数据
                            if 'unique_key' in existing_df.columns:
                                existing_df = existing_df.drop('unique_key', axis=1)
                            return existing_df
                    else:
                        # 如果没有可用于识别重复的列，直接追加
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df.to_csv(output_file, index=False)
                        log_info(f"追加了数据到 {output_file}，总计 {len(combined_df)} 条记录")
                        return combined_df
                        
                except Exception as e:
                    log_error(f"读取或合并现有数据时出错: {str(e)}")
                    log_error(traceback.format_exc())
                    # 如果出错，覆盖现有文件
                    df.to_csv(output_file, index=False)
                    log_info(f"由于错误，覆盖保存了 {len(df)} 条记录到 {output_file}")
            else:
                # 如果文件不存在，直接创建
                df.to_csv(output_file, index=False)
                log_info(f"已保存筛选后的数据到新文件 {output_file}，共 {len(df)} 条记录")
        
        return df
        
    def add_market_cap_data(self, df):
        """
        添加市值数据到DataFrame
        
        参数:
            df: 包含symbol列的DataFrame
        
        返回:
            添加了market_cap列的DataFrame
        """
        if df.empty or 'symbol' not in df.columns:
            return df
        
        log_info("获取股票市值数据...")
        
        # 获取唯一股票列表
        symbols = df['symbol'].unique().tolist()
        total_symbols = len(symbols)
        
        # 准备新的市值列，如果不存在的话
        if 'market_cap' not in df.columns:
            df['market_cap'] = np.nan
        if 'price' not in df.columns:
            df['price'] = np.nan
        if 'volume' not in df.columns:
            df['volume'] = np.nan
        
        # 分批处理，避免一次请求过多
        batch_size = 10
        num_batches = (total_symbols + batch_size - 1) // batch_size
        
        symbols_processed = 0
        for i in range(num_batches):
            batch_symbols = symbols[i*batch_size:(i+1)*batch_size]
            log_info(f"处理第 {i+1}/{num_batches} 批股票，共 {len(batch_symbols)} 只")
            
            for symbol in batch_symbols:
                try:
                    # 检查缓存中是否已有数据
                    cache_key = f"{symbol}_{dt.datetime.now().strftime('%Y%m%d')}"
                    if cache_key in self.market_cap_cache:
                        market_cap = self.market_cap_cache[cache_key]['market_cap']
                        price = self.market_cap_cache[cache_key]['price']
                        volume = self.market_cap_cache[cache_key]['volume']
                        log_debug(f"使用缓存中的 {symbol} 市值数据: {market_cap}")
                    else:
                        # 获取股票信息
                        stock = yf.Ticker(symbol)
                        info = stock.info
                        
                        # 尝试获取市值
                        market_cap = info.get('marketCap', np.nan)
                        price = info.get('regularMarketPrice', info.get('currentPrice', np.nan))
                        volume = info.get('averageDailyVolume10Day', info.get('volume', np.nan))
                        
                        # 更新缓存
                        self.market_cap_cache[cache_key] = {
                            'market_cap': market_cap if not pd.isna(market_cap) else 0,
                            'price': price if not pd.isna(price) else 0,
                            'volume': volume if not pd.isna(volume) else 0
                        }
                    
                    # 更新DataFrame
                    df.loc[df['symbol'] == symbol, 'market_cap'] = market_cap
                    df.loc[df['symbol'] == symbol, 'price'] = price
                    df.loc[df['symbol'] == symbol, 'volume'] = volume
                    
                    symbols_processed += 1
                    if symbols_processed % 10 == 0:
                        log_info(f"已处理 {symbols_processed}/{total_symbols} 只股票")
                    
                except Exception as e:
                    log_error(f"获取 {symbol} 市值数据出错: {str(e)}")
                    continue
                
                # 短暂延迟，避免API限流
                time.sleep(random.uniform(2.0, 3.0))
            
            # 每批处理后保存缓存
            self._save_market_cap_cache()
        
        # 输出结果统计
        valid_market_cap = df['market_cap'].notna().sum()
        log_info(f"成功获取 {valid_market_cap}/{total_symbols} 只股票的市值数据")
        
        return df
        
class EarningsReversalStrategy:
    """收益公告反转策略 - 优化版"""
    
    def __init__(self, ib_connection, earnings_data_path=None, cooldown_days=10, 
             stop_loss_percent=0.05, min_price=5.0, min_volume=100000, exclude_otc=True):
        """
        初始化策略
        
        参数:
            ib_connection: Interactive Brokers连接对象
            earnings_data_path: 收益公告数据文件路径，如不提供则自动获取
            cooldown_days: 冷却期（天），同一股票在此期间内不再交易
            stop_loss_percent: 止损百分比
            min_price: 最低股票价格筛选
            min_volume: 最低日均交易量筛选
            exclude_otc: 是否排除场外交易股票
        """
        self.ib = ib_connection
        self.earnings_data = None
        self.positions = {}  # 当前持仓
        self.stop_loss_prices = {}  # 止损价格
        self.last_buy_time = {}  # 上次买入时间
        self.cooldown_seconds = cooldown_days * 24 * 60 * 60  # 冷却期（秒）
        self.stop_loss_percent = stop_loss_percent  # 止损百分比
        self.max_positions = 20  # 最大持仓数量（多头和空头各20）
        self.min_price = min_price
        self.min_volume = min_volume
        self.exclude_otc = exclude_otc
        self.price_cache = {}  # 价格缓存，避免频繁请求
        self.data_collector = None  # 初始化data_collector属性为None，会在load_earnings_data中设置
        self.trade_history = []  # 交易历史记录
        
        # 创建交易历史目录
        os.makedirs(os.path.join('data', 'trade_history'), exist_ok=True)
        
        # 初始化
        log_info("初始化收益公告反转策略")
        
        # 加载或获取收益公告数据
        self.load_earnings_data(earnings_data_path)
        
        # 加载交易历史
        self.load_trade_history()

    def load_trade_history(self):
        """
        加载交易历史记录
        """
        try:
            # 获取今天的日期
            today = dt.datetime.now().date()
            today_str = today.strftime('%Y-%m-%d')
            
            # 交易历史文件路径
            history_file = os.path.join('data', 'trade_history', f'trades_{today_str}.csv')
            
            # 如果存在今天的交易历史文件，加载它
            if os.path.exists(history_file):
                try:
                    history_df = pd.read_csv(history_file)
                    self.trade_history = history_df.to_dict('records')
                    log_info(f"加载了{len(self.trade_history)}条今日交易历史记录")
                except Exception as e:
                    log_error(f"加载交易历史记录出错: {str(e)}")
                    self.trade_history = []
            else:
                # 如果不存在，初始化为空列表
                self.trade_history = []
                log_info("今日还没有交易历史记录")
        
        except Exception as e:
            log_error(f"加载交易历史记录出错: {str(e)}")
            self.trade_history = []

    def record_trade(self, symbol, action, quantity, price):
        """
        记录交易到历史记录
        
        参数:
            symbol: 股票代码
            action: 交易行为 (BUY 或 SELL)
            quantity: 交易数量
            price: 交易价格
        """
        try:
            # 获取当前时间
            now = dt.datetime.now()
            
            # 创建交易记录
            trade_record = {
                'date': now.date().strftime('%Y-%m-%d'),
                'time': now.time().strftime('%H:%M:%S'),
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'price': price,
                'value': quantity * price
            }
            
            # 添加到内存中的交易历史
            self.trade_history.append(trade_record)
            
            # 保存到文件
            today_str = now.date().strftime('%Y-%m-%d')
            history_file = os.path.join('data', 'trade_history', f'trades_{today_str}.csv')
            
            # 转换为DataFrame
            history_df = pd.DataFrame(self.trade_history)
            
            # 保存到CSV
            history_df.to_csv(history_file, index=False)
            
            log_info(f"记录交易: {action} {quantity} 股 {symbol} @ ${price:.2f}, 总价值: ${quantity * price:.2f}")
        
        except Exception as e:
            log_error(f"记录交易历史出错: {str(e)}")

    def is_traded_today(self, symbol, action=None):
        """
        检查今天是否已经交易过指定股票
        
        参数:
            symbol: 股票代码
            action: 交易行为 (BUY 或 SELL)，如果为None则检查任何交易行为
            
        返回:
            bool: 是否已交易
        """
        try:
            today = dt.datetime.now().date().strftime('%Y-%m-%d')
            
            # 如果没有交易历史，返回False
            if not self.trade_history:
                return False
            
            # 筛选今天的交易
            today_trades = [t for t in self.trade_history if t['date'] == today]
            
            # 如果今天没有交易，返回False
            if not today_trades:
                return False
            
            # 查找特定股票的交易
            symbol_trades = [t for t in today_trades if t['symbol'] == symbol]
            
            # 如果没有指定股票的交易，返回False
            if not symbol_trades:
                return False
            
            # 如果指定了交易行为，进一步筛选
            if action is not None:
                action_trades = [t for t in symbol_trades if t['action'] == action]
                return len(action_trades) > 0
            
            # 如果没有指定交易行为，只要有交易就返回True
            return True
        
        except Exception as e:
            log_error(f"检查今日交易历史出错: {str(e)}")
            # 出错时返回False，避免漏交易
            return False

    def load_earnings_data(self, earnings_data_path=None):
        """
        加载或获取收益公告数据
        
        参数:
            earnings_data_path: 收益公告数据文件路径，如不提供则自动获取
            
        返回:
            bool: 是否成功加载数据
        """
        try:
            if earnings_data_path and os.path.exists(earnings_data_path):
                # 如果提供了路径且文件存在，直接加载
                self.earnings_data = pd.read_csv(earnings_data_path)
                # 确保日期列是日期类型
                if 'earnings_date' in self.earnings_data.columns:
                    self.earnings_data['earnings_date'] = pd.to_datetime(self.earnings_data['earnings_date'])
                log_info(f"从{earnings_data_path}加载了{len(self.earnings_data)}条收益公告数据")
            else:
                # 如果没有提供路径或文件不存在，尝试获取新数据
                log_info("未提供收益公告数据文件或文件不存在，开始获取新数据...")
                
                # 创建数据收集器实例并保存为类属性，使其可以在其他方法中访问
                self.data_collector = EarningsDataCollector(
                    min_price=self.min_price, 
                    min_volume=self.min_volume, 
                    exclude_otc=self.exclude_otc
                )
                
                # 使用收集器获取数据
                self.earnings_data = self.data_collector.get_earnings_data()
                
                if self.earnings_data is None or self.earnings_data.empty:
                    log_error("获取收益公告数据失败")
                    return False
                
                log_info(f"成功获取了{len(self.earnings_data)}条收益公告数据")
            
            return True
        
        except Exception as e:
            log_error(f"加载收益公告数据出错: {str(e)}")
            log_error(traceback.format_exc())
            return False

    # 在EarningsReversalStrategy类的__init__方法中添加data_collector属性的初始化
    def __init__(self, ib_connection, earnings_data_path=None, cooldown_days=10, 
                stop_loss_percent=0.05, min_price=5.0, min_volume=100000, exclude_otc=True):
        """
        初始化策略
        
        参数:
            ib_connection: Interactive Brokers连接对象
            earnings_data_path: 收益公告数据文件路径，如不提供则自动获取
            cooldown_days: 冷却期（天），同一股票在此期间内不再交易
            stop_loss_percent: 止损百分比
            min_price: 最低股票价格筛选
            min_volume: 最低日均交易量筛选
            exclude_otc: 是否排除场外交易股票
        """
        self.ib = ib_connection
        self.earnings_data = None
        self.positions = {}  # 当前持仓
        self.stop_loss_prices = {}  # 止损价格
        self.last_buy_time = {}  # 上次买入时间
        self.cooldown_seconds = cooldown_days * 24 * 60 * 60  # 冷却期（秒）
        self.stop_loss_percent = stop_loss_percent  # 止损百分比
        self.max_positions = 20  # 最大持仓数量（多头和空头各20）
        self.min_price = min_price
        self.min_volume = min_volume
        self.exclude_otc = exclude_otc
        self.price_cache = {}  # 价格缓存，避免频繁请求
        self.data_collector = None  # 初始化data_collector属性为None，会在load_earnings_data中设置
        
        # 初始化
        log_info("初始化收益公告反转策略")
        
        # 加载或获取收益公告数据
        self.load_earnings_data(earnings_data_path)

    def filter_valid_stocks(self, symbols):
        """
        过滤出在IBKR中有效的股票
        
        参数:
            symbols: 股票代码列表
            
        返回:
            list: 有效的股票代码列表
        """
        valid_symbols = []
        invalid_symbols = []
        
        for symbol in symbols:
            try:
                # 跳过无效股票代码
                if pd.isna(symbol) or not symbol or len(symbol) > 5 or '-' in symbol or '.' in symbol:
                    invalid_symbols.append(symbol)
                    continue
                
                # 创建合约对象
                contract = Stock(symbol, 'SMART', 'USD')
                
                # 尝试验证合约
                qualified_contracts = self.ib.qualifyContracts(contract)
                
                # 如果能够正确验证合约，则为有效股票
                if qualified_contracts:
                    valid_symbols.append(symbol)
                else:
                    invalid_symbols.append(symbol)
                    log_info(f"股票{symbol}在IBKR中无法验证，跳过")
            except Exception as e:
                log_error(f"验证股票{symbol}时出错: {str(e)}")
                invalid_symbols.append(symbol)
        
        # 记录结果
        valid_count = len(valid_symbols)
        invalid_count = len(invalid_symbols)
        total_count = valid_count + invalid_count
        
        log_info(f"共验证{total_count}只股票，有效:{valid_count}只，无效:{invalid_count}只")
        if invalid_count > 0:
            # 只显示前20个无效股票，避免日志过长
            display_count = min(20, invalid_count)
            invalid_display = ', '.join(invalid_symbols[:display_count])
            if invalid_count > display_count:
                invalid_display += f"...等{invalid_count}只"
            log_info(f"无效股票列表: {invalid_display}")
        
        return valid_symbols

    def get_current_positions(self):
        """
        获取当前持仓
        
        返回:
            dict: 当前持仓信息
        """
        try:
            # 获取账户持仓
            portfolio = self.ib.portfolio()
            
            # 清空当前持仓记录
            self.positions = {}
            
            # 更新持仓信息
            for position in portfolio:
                symbol = position.contract.symbol
                quantity = position.position
                self.positions[symbol] = quantity
                
            log_info(f"更新持仓信息成功, 共{len(portfolio)}个持仓")
            return self.positions
            
        except Exception as e:
            log_error(f"获取持仓信息出错: {str(e)}")
            log_error(traceback.format_exc())
            return {}
            
    def check_stop_loss(self, symbol, current_price):
        """
        检查是否触发止损
        
        参数:
            symbol: 股票代码
            current_price: 当前价格
            
        返回:
            bool: 是否触发止损
        """
        if symbol in self.stop_loss_prices and current_price <= self.stop_loss_prices[symbol]:
            return True
        return False
        
    def can_buy_again(self, symbol):
        """
        检查是否可以再次买入
        
        参数:
            symbol: 股票代码
            
        返回:
            bool: 是否可以再次买入
        """
        if symbol not in self.last_buy_time:
            return True
            
        elapsed_time = time.time() - self.last_buy_time[symbol]
        return elapsed_time >= self.cooldown_seconds

    def get_latest_price(self, symbol, use_yfinance=True):
        """
        获取最新价格，增加了yfinance作为备用方案
        
        参数:
            symbol: 股票代码
            use_yfinance: 是否使用yfinance作为备用方案
            
        返回:
            float: 最新价格，如果获取失败则返回0
        """
        try:
            # 检查缓存中是否有最近的价格数据
            cache_key = f"{symbol}_{dt.datetime.now().strftime('%Y%m%d_%H')}"
            if cache_key in self.price_cache:
                return self.price_cache[cache_key]
            
            # 使用IB API获取价格
            contract = Stock(symbol, 'SMART', 'USD')
            try:
                self.ib.qualifyContracts(contract)
                
                # 获取市场数据
                ticker = self.ib.reqMktData(contract)
                # 等待数据返回
                for _ in range(5):  # 尝试最多5次
                    self.ib.sleep(0.2)  # 每次等待200毫秒
                    # 检查是否获取到价格
                    if ticker.last > 0 or ticker.close > 0 or ticker.bid > 0 or ticker.ask > 0:
                        break
                
                # 尝试获取最新价格，使用多种备选方案
                price = ticker.last  # 首选最新成交价
                if price <= 0:
                    price = ticker.close  # 其次用收盘价
                if price <= 0 and ticker.bid > 0 and ticker.ask > 0:
                    price = (ticker.bid + ticker.ask) / 2  # 再次用买卖价的中间值
                
                # 取消市场数据订阅
                self.ib.cancelMktData(contract)
                
                if price > 0:
                    # 缓存价格数据
                    self.price_cache[cache_key] = price
                    return price
                        
            except Exception as e:
                log_warning(f"使用IB API获取{symbol}价格失败: {str(e)}，尝试使用yfinance")
                
            # 如果IB API获取失败或价格为0，尝试使用yfinance
            if use_yfinance:
                try:
                    stock = yf.Ticker(symbol)
                    # 获取最新价格数据
                    hist = stock.history(period="1d")
                    if not hist.empty:
                        # 使用最近的收盘价
                        price = hist['Close'].iloc[-1]
                        # 缓存价格数据
                        self.price_cache[cache_key] = price
                        log_info(f"使用yfinance获取{symbol}价格成功: {price}")
                        return price
                except Exception as e:
                    log_error(f"使用yfinance获取{symbol}价格也失败: {str(e)}")
            
            # 如果两种方法都失败，返回0
            log_error(f"获取{symbol}价格失败")
            return 0
                
        except Exception as e:
            log_error(f"获取{symbol}最新价格出错: {str(e)}")
            log_error(traceback.format_exc())
            return 0

    def place_order(self, symbol, action, quantity, reason="", use_limit_price=True):
        """
        下单函数 - 支持盘前盘后交易，添加了更健壮的错误处理
        
        参数:
            symbol: 股票代码
            action: 交易行为，'BUY'或'SELL'
            quantity: 交易数量
            reason: 交易原因，用于日志
            use_limit_price: 是否使用限价单，默认为True
            
        返回:
            bool: 是否成功下单
        """
        try:
            # 确保数量为整数并且大于0
            if pd.isna(quantity) or quantity <= 0:
                log_error(f"{symbol}交易数量无效: {quantity}，跳过下单")
                return False
                
            quantity = int(quantity)
            if quantity <= 0:
                log_error(f"{symbol}交易数量为0或负数，跳过下单")
                return False
            
            # 记录下单详情到日志
            order_info = f"准备下单: {action} {quantity} 股 {symbol}, 原因: {reason}"
            log_trade(order_info)
            
            # 创建合约
            contract = Stock(symbol, 'SMART', 'USD')
            qualified_contracts = self.ib.qualifyContracts(contract)
            
            if not qualified_contracts:
                log_error(f"无法验证{symbol}合约，下单失败")
                return False
            
            # 获取当前市场价格作为限价基础
            current_price = self.get_latest_price(symbol)
            if current_price <= 0:
                log_error(f"无法获取{symbol}当前价格，下单失败")
                return False
            
            # 检查当前是否在常规交易时间
            current_time = dt.datetime.now().time()
            is_regular_hours = (
                (current_time >= dt.time(9, 30) and current_time <= dt.time(16, 0)) and
                (dt.datetime.now().weekday() < 5)  # 工作日
            )
            
            # 创建订单
            if use_limit_price or not is_regular_hours:
                # 使用限价单
                if action == 'BUY':
                    # 买入时略高于市价以确保成交
                    limit_price = round(current_price * 1.01, 2)  # 高于市价1%
                else:  # 'SELL'
                    # 卖出时略低于市价以确保成交
                    limit_price = round(current_price * 0.99, 2)  # 低于市价1%
                
                order = LimitOrder(action, quantity, limit_price)
                log_trade(f"使用限价单 - 价格: {limit_price}")
            else:
                # 使用市价单（仅常规交易时间）
                order = MarketOrder(action, quantity)
                log_trade("使用市价单")
            
            # 设置为允许在盘前盘后交易
            order.outsideRth = True
            log_trade("启用盘前盘后交易")
            
            # 订单有效期 - 设置为当天有效
            order.tif = 'DAY'
            
            # 实际下单
            trade = self.ib.placeOrder(contract, order)
            
            # 等待订单状态更新
            for _ in range(10):  # 增加等待尝试次数
                self.ib.sleep(1)  # 等待1秒
                if trade.orderStatus.status in ['Filled', 'Cancelled', 'ApiCancelled', 'Inactive']:
                    break
            
            # 获取订单状态
            status = trade.orderStatus.status
            filled = trade.orderStatus.filled
            remaining = trade.orderStatus.remaining
            avg_fill_price = trade.orderStatus.avgFillPrice
            
            # 记录订单状态
            status_info = f"订单状态: {status}, 已成交: {filled}, 剩余: {remaining}, 平均成交价: {avg_fill_price}"
            log_trade(status_info)
            
            # 获取最新价格或使用成交价
            execution_price = avg_fill_price if avg_fill_price > 0 else current_price
            
            # 如果是买入订单，更新最后买入时间和设置止损价格
            if action == 'BUY' and filled > 0:
                self.last_buy_time[symbol] = time.time()
                
                # 设置止损价格
                stop_loss_price = execution_price * (1 - self.stop_loss_percent)
                self.stop_loss_prices[symbol] = stop_loss_price
                log_trade(f"设置止损价格: {stop_loss_price}")
            
            # 检查订单是否完全成交
            if status == 'Filled':
                log_trade(f"{symbol} {action} 订单完全成交")
                return True
            elif filled > 0:
                log_trade(f"{symbol} {action} 订单部分成交: {filled}/{quantity}")
                return True
            else:
                log_trade(f"{symbol} {action} 订单未成交")
                return False
                
        except Exception as e:
            log_error(f"下单出错: {str(e)}")
            log_error(traceback.format_exc())
            return False

    # def get_upcoming_earnings(self, days_ahead=30):
    #     """
    #     获取即将到来的收益公告 - 增强的重试机制和错误处理
        
    #     参数:
    #         days_ahead: 未来几天的收益公告
            
    #     返回:
    #         DataFrame: 收益公告数据
    #     """
    #     try:
    #         log_info(f"获取未来{days_ahead}天的收益公告数据...")
            
    #         # 获取当前日期
    #         today = dt.datetime.now().date()
    #         end_date = today + dt.timedelta(days=days_ahead)
            
    #         # 准备收益数据列表
    #         earnings_data = []
            
    #         # 每批处理的股票数量
    #         batch_size = 20
            
    #         # 计算总批次
    #         total_batches = (len(self.stock_universe) + batch_size - 1) // batch_size
            
    #         # 处理每一批股票
    #         for batch_index in range(total_batches):
    #             start_idx = batch_index * batch_size
    #             end_idx = min(start_idx + batch_size, len(self.stock_universe))
    #             current_batch = self.stock_universe[start_idx:end_idx]
                
    #             log_info(f"正在处理第 {batch_index+1}/{total_batches} 批股票 (共{len(current_batch)}只)")
                
    #             # 处理当前批次的每只股票
    #             for ticker in current_batch:
    #                 # 对每只股票进行多次尝试
    #                 max_attempts = 3
    #                 for attempt in range(max_attempts):
    #                     try:
    #                         # 获取股票对象
    #                         stock = yf.Ticker(ticker)
                            
    #                         # 获取收益信息
    #                         info = stock.info
    #                         earnings_date = None
                            
    #                         # 尝试各种可能的键名
    #                         if 'earningsDate' in info:
    #                             # 获取earnings_date值
    #                             earnings_date_value = info['earningsDate']
                                
    #                             # 判断类型并适当处理
    #                             if isinstance(earnings_date_value, list) and len(earnings_date_value) > 0:
    #                                 # 如果是列表，取第一个元素
    #                                 earnings_date = earnings_date_value[0]
    #                             elif hasattr(earnings_date_value, '__len__') and len(earnings_date_value) > 0:
    #                                 # 如果是numpy数组或类似的序列类型，取第一个元素 (通过索引访问)
    #                                 earnings_date = earnings_date_value[0]
    #                             else:
    #                                 # 其他情况直接使用
    #                                 earnings_date = earnings_date_value
    #                         elif 'nextEarningsDate' in info:
    #                             earnings_date = info['nextEarningsDate']
    #                         else:
    #                             # 尝试获取公司日历
    #                             calendar = stock.calendar
                                
    #                             # 安全处理不同类型的calendar对象
    #                             if calendar is not None:
    #                                 # 如果是字典
    #                                 if isinstance(calendar, dict):
    #                                     if 'Earnings Date' in calendar:
    #                                         calendar_value = calendar['Earnings Date']
    #                                         # 检查是否是numpy数组
    #                                         if hasattr(calendar_value, '__len__') and not isinstance(calendar_value, (str, dict)):
    #                                             earnings_date = calendar_value[0] if len(calendar_value) > 0 else None
    #                                         else:
    #                                             earnings_date = calendar_value
    #                                     elif 'earningsDate' in calendar:
    #                                         calendar_value = calendar['earningsDate']
    #                                         # 检查是否是numpy数组
    #                                         if hasattr(calendar_value, '__len__') and not isinstance(calendar_value, (str, dict)):
    #                                             earnings_date = calendar_value[0] if len(calendar_value) > 0 else None
    #                                         else:
    #                                             earnings_date = calendar_value
    #                                 # 如果是DataFrame
    #                                 elif isinstance(calendar, pd.DataFrame):
    #                                     if 'Earnings Date' in calendar.index:
    #                                         earnings_date = calendar.loc['Earnings Date'].iloc[0]
    #                                     elif 'earningsDate' in calendar.index:
    #                                         earnings_date = calendar.loc['earningsDate'].iloc[0]
    #                                 # 如果是Series
    #                                 elif isinstance(calendar, pd.Series):
    #                                     if 'Earnings Date' in calendar.index:
    #                                         earnings_date = calendar['Earnings Date']
    #                                     elif 'earningsDate' in calendar.index:
    #                                         earnings_date = calendar['earningsDate']
                            
    #                         # 如果成功获取到收益日期
    #                         if earnings_date is not None:
    #                             # 转换为datetime
    #                             try:
    #                                 earnings_date = pd.to_datetime(earnings_date)
    #                             except Exception as date_err:
    #                                 log_debug(f"{ticker}收益日期格式转换失败: {earnings_date}, 错误: {date_err}")
    #                                 continue
                                
    #                             # 检查日期是否在范围内
    #                             if today <= earnings_date.date() <= end_date:
    #                                 # 获取股票信息
    #                                 company_name = info.get('shortName', info.get('longName', ticker))
    #                                 market_cap = info.get('marketCap', None)
    #                                 price = info.get('regularMarketPrice', info.get('currentPrice', None))
    #                                 volume = info.get('averageDailyVolume10Day', info.get('volume', None))
                                    
    #                                 # 添加到结果列表
    #                                 earnings_data.append({
    #                                     'symbol': ticker,
    #                                     'company_name': company_name,
    #                                     'earnings_date': earnings_date,
    #                                     'estimated_eps': info.get('trailingEps', None),
    #                                     'market_cap': market_cap,
    #                                     'price': price,
    #                                     'volume': volume,
    #                                     'time_of_day': "After Market Close" if earnings_date.hour >= 12 else "Before Market Open"
    #                                 })
                                    
    #                                 log_info(f"找到{ticker}的收益公告: {earnings_date.strftime('%Y-%m-%d')}")
                            
    #                         # 成功处理，跳出重试循环
    #                         break
                                
    #                     except Exception as e:
    #                         if attempt < max_attempts - 1:
    #                             log_warning(f"处理{ticker}时出错(尝试{attempt+1}/{max_attempts}): {str(e)}，重试...")
    #                             # 增加指数退避重试延迟
    #                             retry_delay = 2 ** attempt * random.uniform(1.0, 2.0)
    #                             time.sleep(retry_delay)
    #                         else:
    #                             log_error(f"处理{ticker}时最终失败: {str(e)}")
                    
    #                 # 添加随机延迟，避免请求过度并防止IP被封
    #                 time.sleep(random.uniform(1.0, 2.0))
                
    #             # 每批完成后保存中间结果，防止中途失败
    #             if earnings_data and batch_index > 0 and batch_index % 3 == 0:
    #                 temp_df = pd.DataFrame(earnings_data)
    #                 temp_file = os.path.join('data', f'earnings_temp_batch_{batch_index}.csv')
    #                 temp_df.to_csv(temp_file, index=False)
    #                 log_info(f"已保存中间结果到{temp_file}，当前共{len(temp_df)}条记录")
                    
    #             # 每批之间添加更长的延迟
    #             if batch_index < total_batches - 1:
    #                 batch_delay = random.uniform(5.0, 10.0)
    #                 log_info(f"批次{batch_index+1}完成，等待{batch_delay:.1f}秒后处理下一批...")
    #                 time.sleep(batch_delay)
                    
    #         # 转换为DataFrame
    #         df = pd.DataFrame(earnings_data)
            
    #         if df.empty:
    #             log_info("未找到任何即将到来的收益公告")
    #             return df
            
    #         # 清理中间文件
    #         for batch_index in range(0, total_batches, 3):
    #             temp_file = os.path.join('data', f'earnings_temp_batch_{batch_index}.csv')
    #             if os.path.exists(temp_file):
    #                 try:
    #                     os.remove(temp_file)
    #                 except Exception:
    #                     pass
            
    #         # 保存原始数据
    #         output_file = os.path.join('data', 'earnings_calendar_raw.csv')
    #         df.to_csv(output_file, index=False)
    #         log_info(f"成功获取并保存 {len(df)} 条收益公告数据到 {output_file}")
            
    #         return df
                
    #     except Exception as e:
    #         log_error(f"获取收益公告数据出错: {str(e)}")
    #         log_error(traceback.format_exc())
            
    #         # 尝试合并所有中间结果
    #         try:
    #             log_info("尝试从中间结果恢复数据...")
    #             all_dfs = []
                
    #             # 查找所有临时文件
    #             import glob
    #             temp_files = glob.glob(os.path.join('data', 'earnings_temp_batch_*.csv'))
                
    #             for temp_file in temp_files:
    #                 try:
    #                     temp_df = pd.read_csv(temp_file)
    #                     all_dfs.append(temp_df)
    #                     log_info(f"从{temp_file}加载了{len(temp_df)}条记录")
    #                 except Exception:
    #                     pass
                
    #             if all_dfs:
    #                 combined_df = pd.concat(all_dfs, ignore_index=True)
    #                 if not combined_df.empty:
    #                     log_info(f"从中间结果恢复了{len(combined_df)}条记录")
    #                     return combined_df
    #         except Exception as recovery_err:
    #             log_error(f"恢复数据失败: {str(recovery_err)}")
            
    #         return pd.DataFrame()
    def get_upcoming_earnings(self, days_range=5):
        """
        获取即将到来的收益公告 - 修复版，不直接使用stock_universe属性
        
        参数:
            days_range: 未来几天的收益公告
            
        返回:
            DataFrame: 即将到来的收益公告数据
        """
        try:
            # 确保数据已加载
            if self.earnings_data is None or self.earnings_data.empty:
                log_error("收益公告数据未加载或为空")
                return pd.DataFrame()
                    
            today = dt.datetime.now().date()
            
            # 确保earnings_date列是日期时间格式
            if 'earnings_date' in self.earnings_data.columns:
                if not pd.api.types.is_datetime64_any_dtype(self.earnings_data['earnings_date']):
                    self.earnings_data['earnings_date'] = pd.to_datetime(self.earnings_data['earnings_date'])
            else:
                log_error("收益公告数据中缺少earnings_date列")
                return pd.DataFrame()
            
            # 筛选未来几天的收益公告
            upcoming = self.earnings_data[
                (self.earnings_data['earnings_date'].dt.date >= today) &
                (self.earnings_data['earnings_date'].dt.date <= today + dt.timedelta(days=days_range))
            ].copy()
            
            if upcoming.empty:
                log_info(f"未找到未来{days_range}天内的收益公告")
                return upcoming
                    
            # 获取所有股票代码并过滤有效的
            all_symbols = upcoming['symbol'].unique().tolist()
            log_info(f"找到{len(all_symbols)}只即将公布收益的股票，开始验证是否可在IBKR交易...")
            
            # 筛选出在IBKR有效的股票
            valid_symbols = self.filter_valid_stocks(all_symbols)
            
            # 筛选只包含有效股票的收益公告
            filtered_upcoming = upcoming[upcoming['symbol'].isin(valid_symbols)].copy()
            
            # 确保所有必要列都存在，添加缺失的列
            for col in ['market_cap', 'price', 'volume']:
                if col not in filtered_upcoming.columns:
                    filtered_upcoming[col] = np.nan
            
            log_info(f"筛选后剩余{len(filtered_upcoming)}条有效收益公告记录")
            return filtered_upcoming
                
        except Exception as e:
            log_error(f"获取即将到来的收益公告出错: {str(e)}")
            log_error(traceback.format_exc())
            return pd.DataFrame()

    def calculate_pre_earnings_returns(self, symbols):
        """
        计算收益公告前的回报率，修复了yfinance的period参数问题
        
        参数:
            symbols: 股票代码列表
            
        返回:
            dict: 股票代码到回报率的映射
        """
        returns = {}
        failed_symbols = []
        
        for symbol in symbols:
            try:
                # 使用yfinance获取历史数据 - 使用有效的period参数
                ticker = yf.Ticker(symbol)
                # 使用'5d'替代'10d'，因为'10d'不是有效的period参数
                hist = ticker.history(period="5d")
                
                if not hist.empty and len(hist) >= 3:
                    # 计算t-3到t-1的回报率 (使用可用数据点)
                    pre_earnings_return = (hist['Close'].iloc[-1] / hist['Close'].iloc[0] - 1) * 100
                    returns[symbol] = pre_earnings_return
                    log_info(f"成功计算{symbol}的收益公告前回报率: {pre_earnings_return:.2f}%")
                else:
                    log_info(f"无法获取足够的{symbol}历史数据来计算收益公告前回报率，尝试使用IB API")
                    
                    # 尝试使用IB API获取历史数据
                    hist_data = self._get_historical_data_from_ib(symbol, days=10)
                    
                    if hist_data is not None and len(hist_data) >= 3:
                        # 计算回报率
                        pre_earnings_return = (hist_data['close'].iloc[-1] / hist_data['close'].iloc[0] - 1) * 100
                        returns[symbol] = pre_earnings_return
                        log_info(f"使用IB API成功计算{symbol}的收益公告前回报率: {pre_earnings_return:.2f}%")
                    else:
                        log_warning(f"无法获取足够的{symbol}历史数据来计算收益公告前回报率")
                        failed_symbols.append(symbol)
                        returns[symbol] = None
                        
            except Exception as e:
                log_error(f"计算{symbol}收益公告前回报率出错: {str(e)}")
                failed_symbols.append(symbol)
                returns[symbol] = None
                    
        # 记录回报率计算结果
        success_count = sum(1 for v in returns.values() if v is not None)
        log_info(f"成功计算{success_count}/{len(symbols)}只股票的收益公告前回报率")
        
        if failed_symbols:
            log_info(f"无法计算回报率的股票: {', '.join(failed_symbols[:20])}" + 
                    (f"...等{len(failed_symbols)}只" if len(failed_symbols) > 20 else ""))
                    
        return returns

    def _get_historical_data_from_ib(self, symbol, days=10):
        """
        从IB获取历史数据 - 修复时区警告问题
        
        参数:
            symbol: 股票代码
            days: 历史数据天数
            
        返回:
            DataFrame: 历史数据
        """
        try:
            # 使用协调世界时(UTC)格式的时间
            end_date = dt.datetime.now()
            end_date_str = end_date.strftime('%Y%m%d-%H:%M:%S')
            
            contract = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)
            
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=end_date_str,
                durationStr=f'{days} D',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=2  # 1: 字符串格式, 2: Unix时间戳（避免时区问题）
            )
            
            if bars:
                # 转换为DataFrame
                df = util.df(bars)
                return df
            return None
            
        except Exception as e:
            log_error(f"从IB获取{symbol}历史数据出错: {str(e)}")
            return None

    def select_stocks(self):
        """
        选择交易股票 - 修复版本，确保不会出现同一股票同时在多头和空头列表中的情况
        
        返回:
            tuple: (多头股票列表, 空头股票列表)
        """
        try:
            # 获取即将公布收益的股票
            upcoming_earnings = self.get_upcoming_earnings()
            
            if upcoming_earnings.empty:
                log_info("没有找到即将公布收益的股票")
                return [], []
                
            # 获取所有符合条件的股票代码
            symbols = upcoming_earnings['symbol'].unique().tolist()
            
            if not symbols:
                return [], []
                
            # 如果earnings_data中已经有market_cap列，则直接使用
            if 'market_cap' in upcoming_earnings.columns and not upcoming_earnings['market_cap'].isna().all():
                log_info("使用数据源中的市值数据")
                # 提取每个股票的最大市值（以防有重复股票）
                market_caps = {}
                for symbol in symbols:
                    symbol_data = upcoming_earnings[upcoming_earnings['symbol'] == symbol]
                    if not symbol_data.empty and not symbol_data['market_cap'].isna().all():
                        market_caps[symbol] = symbol_data['market_cap'].max()
                    else:
                        # 尝试从yfinance获取市值数据
                        try:
                            ticker = yf.Ticker(symbol)
                            info = ticker.info
                            market_cap = info.get('marketCap', 0)
                            market_caps[symbol] = market_cap
                        except Exception:
                            market_caps[symbol] = 0
            else:
                # 否则，使用yfinance获取市值数据
                log_info("从yfinance获取市值数据")
                market_caps = {}
                for symbol in symbols:
                    try:
                        ticker = yf.Ticker(symbol)
                        info = ticker.info
                        market_cap = info.get('marketCap', 0)
                        market_caps[symbol] = market_cap
                    except Exception:
                        market_caps[symbol] = 0
            
            # 按市值排序，选取前20%的大公司
            market_cap_df = pd.DataFrame({
                'symbol': list(market_caps.keys()),
                'market_cap': list(market_caps.values())
            })
            market_cap_df = market_cap_df.sort_values('market_cap', ascending=False)
            
            # 过滤掉市值为0或NaN的股票
            market_cap_df = market_cap_df[market_cap_df['market_cap'] > 0].copy()
            
            if market_cap_df.empty:
                log_info("没有获取到有效市值数据的股票")
                return [], []
            
            # 选取市值最大的20%
            top_quintile_count = max(1, int(len(market_cap_df) * 0.2))
            top_quintile = market_cap_df.head(top_quintile_count)['symbol'].tolist()
            
            log_info(f"按市值选出前20%的大公司，共{len(top_quintile)}只: {', '.join(top_quintile[:10])}" + 
                    (f"...等" if len(top_quintile) > 10 else ""))
            
            # 计算收益公告前的回报率
            pre_earnings_returns = self.calculate_pre_earnings_returns(top_quintile)
            
            # 筛选出有回报率数据的股票
            valid_returns = {k: v for k, v in pre_earnings_returns.items() if v is not None}
            
            if not valid_returns:
                log_info("没有找到足够的历史数据来计算收益公告前回报率")
                return [], []
            
            # 转换为DataFrame并排序
            returns_df = pd.DataFrame({
                'symbol': list(valid_returns.keys()),
                'pre_earnings_return': list(valid_returns.values())
            })
            returns_df = returns_df.sort_values('pre_earnings_return')
            
            # 检查是否有足够的股票进行分类
            if len(returns_df) < 2:
                log_info(f"只找到{len(returns_df)}只股票，无法同时生成做多和做空列表")
                if len(returns_df) == 1:
                    # 如果只有一只股票，则根据回报率决定是做多还是做空
                    symbol = returns_df.iloc[0]['symbol']
                    return_value = returns_df.iloc[0]['pre_earnings_return']
                    if return_value < 0:
                        # 如果回报率为负，则做多
                        log_info(f"只有一只股票 {symbol}，回报率为负 ({return_value:.2f}%)，建议做多")
                        return [symbol], []
                    else:
                        # 如果回报率为正，则做空
                        log_info(f"只有一只股票 {symbol}，回报率为正 ({return_value:.2f}%)，建议做空")
                        return [], [symbol]
                return [], []
                
            # 确保至少有3只股票才进行分类，避免数据太少导致分类不准确
            min_stocks_for_quintiles = 3
            if len(returns_df) < min_stocks_for_quintiles:
                log_info(f"只找到{len(returns_df)}只股票，少于{min_stocks_for_quintiles}只，无法可靠地分为五分位")
                # 简单地将靠前的一半做多，靠后的一半做空
                mid_point = len(returns_df) // 2
                long_candidates = returns_df.iloc[:mid_point]['symbol'].tolist()
                short_candidates = returns_df.iloc[mid_point:]['symbol'].tolist()
            else:
                # 选择表现最差的股票做多（底部五分位）
                bottom_quintile_count = max(1, int(len(returns_df) * 0.2))
                long_candidates = returns_df.head(bottom_quintile_count)['symbol'].tolist()
                
                # 选择表现最好的股票做空（顶部五分位）
                short_candidates = returns_df.tail(bottom_quintile_count)['symbol'].tolist()
            
            # 确保同一只股票不会同时出现在做多和做空列表中
            common_stocks = set(long_candidates).intersection(set(short_candidates))
            if common_stocks:
                log_warning(f"发现{len(common_stocks)}只股票同时出现在做多和做空列表中，正在修正...")
                for stock in common_stocks:
                    # 查找该股票在回报率中的位置
                    stock_index = returns_df[returns_df['symbol'] == stock].index[0]
                    stock_position = stock_index / len(returns_df)
                    
                    # 根据相对位置决定保留在哪个列表中
                    if stock_position < 0.5:  # 在前半部分，做多
                        short_candidates.remove(stock)
                        log_info(f"股票 {stock} 从做空列表中移除，保留在做多列表中")
                    else:  # 在后半部分，做空
                        long_candidates.remove(stock)
                        log_info(f"股票 {stock} 从做多列表中移除，保留在做空列表中")
            
            # 限制最大持仓数量
            long_candidates = long_candidates[:self.max_positions]
            short_candidates = short_candidates[:self.max_positions]
            
            # 再次检查是否有重复股票
            common_stocks = set(long_candidates).intersection(set(short_candidates))
            if common_stocks:
                log_warning(f"修正后仍有{len(common_stocks)}只股票同时出现在两个列表中，强制移除...")
                for stock in common_stocks:
                    if stock in long_candidates:
                        long_candidates.remove(stock)
                        log_info(f"强制从做多列表中移除股票 {stock}")
            
            # 打印选股结果
            self.print_stock_selection(long_candidates, short_candidates, returns_df, market_cap_df)
            
            return long_candidates, short_candidates
            
        except Exception as e:
            log_error(f"选股出错: {str(e)}")
            log_error(traceback.format_exc())
            return [], []

    def print_stock_selection(self, long_stocks, short_stocks, returns_df, market_cap_df):
        """
        使用PrettyTable打印选股结果
        
        参数:
            long_stocks: 做多股票列表
            short_stocks: 做空股票列表
            returns_df: 回报率数据
            market_cap_df: 市值数据
        """
        try:
            # 创建PrettyTable对象
            long_table = PrettyTable()
            short_table = PrettyTable()
            
            # 设置表头
            long_table.field_names = ["做多股票", "收益公告前回报率(%)", "市值(百万)", "当前价格"]
            short_table.field_names = ["做空股票", "收益公告前回报率(%)", "市值(百万)", "当前价格"]
            
            # 设置对齐方式
            long_table.align["做多股票"] = "l"
            long_table.align["收益公告前回报率(%)"] = "r"
            long_table.align["市值(百万)"] = "r"
            long_table.align["当前价格"] = "r"
            
            short_table.align["做空股票"] = "l"
            short_table.align["收益公告前回报率(%)"] = "r"
            short_table.align["市值(百万)"] = "r"
            short_table.align["当前价格"] = "r"
            
            # 合并市值和回报率数据
            data = pd.merge(returns_df, market_cap_df, on='symbol', how='left')
            
            # 添加数据
            for symbol in long_stocks:
                try:
                    row_data = data[data['symbol'] == symbol]
                    if row_data.empty:
                        continue
                        
                    pre_ret = row_data['pre_earnings_return'].values[0]
                    
                    # 获取市值
                    market_cap = row_data['market_cap_y'].values[0] if 'market_cap_y' in row_data.columns else row_data['market_cap'].values[0]
                    if pd.isna(market_cap):
                        market_cap = 0
                        
                    # 获取当前价格
                    current_price = self.get_latest_price(symbol)
                    
                    # 格式化市值 (转换为百万美元)
                    market_cap_formatted = f"{market_cap/1000000:.2f}" if market_cap > 0 else "N/A"
                    
                    long_table.add_row([
                        symbol, 
                        f"{pre_ret:.2f}", 
                        market_cap_formatted,
                        f"{current_price:.2f}" if current_price > 0 else "N/A"
                    ])
                except Exception as e:
                    log_error(f"打印{symbol}做多信息出错: {str(e)}")
                    continue
                    
            for symbol in short_stocks:
                try:
                    row_data = data[data['symbol'] == symbol]
                    if row_data.empty:
                        continue
                        
                    pre_ret = row_data['pre_earnings_return'].values[0]
                    
                    # 获取市值
                    market_cap = row_data['market_cap_y'].values[0] if 'market_cap_y' in row_data.columns else row_data['market_cap'].values[0]
                    if pd.isna(market_cap):
                        market_cap = 0
                        
                    # 获取当前价格
                    current_price = self.get_latest_price(symbol)
                    
                    # 格式化市值 (转换为百万美元)
                    market_cap_formatted = f"{market_cap/1000000:.2f}" if market_cap > 0 else "N/A"
                    
                    short_table.add_row([
                        symbol, 
                        f"{pre_ret:.2f}", 
                        market_cap_formatted,
                        f"{current_price:.2f}" if current_price > 0 else "N/A"
                    ])
                except Exception as e:
                    log_error(f"打印{symbol}做空信息出错: {str(e)}")
                    continue
                    
            # 打印表格
            log_info("\n做多股票列表:")
            log_info("\n" + str(long_table))
            log_info("\n做空股票列表:")
            log_info("\n" + str(short_table))
            
        except Exception as e:
            log_error(f"打印选股结果出错: {str(e)}")
            log_error(traceback.format_exc())
    def is_trading_day(self, date=None):
        """
        检查指定日期是否为交易日(非周末和美国主要假日)
        
        参数:
            date: 要检查的日期，默认为今天
            
        返回:
            bool: 是否为交易日
        """
        if date is None:
            date = dt.datetime.now().date()
        
        # 检查是否为周末
        if date.weekday() >= 5:  # 5=周六, 6=周日
            return False
        
        # 美国主要假日列表(简化版)
        # 实际应用中可以使用更完整的假日日历
        us_holidays_2025 = [
            dt.date(2025, 1, 1),   # 元旦
            dt.date(2025, 1, 20),  # 马丁·路德·金日
            dt.date(2025, 2, 17),  # 总统日
            dt.date(2025, 4, 18),  # 耶稣受难日
            dt.date(2025, 5, 26),  # 阵亡将士纪念日
            dt.date(2025, 7, 4),   # 独立日
            dt.date(2025, 9, 1),   # 劳动节
            dt.date(2025, 11, 27), # 感恩节
            dt.date(2025, 12, 25)  # 圣诞节
        ]
        
        # 检查是否为假日
        if date in us_holidays_2025:
            return False
        
        return True

    def can_trade_now(self):
        """
        检查当前时间是否可以交易(考虑盘前盘后交易时段)
        
        返回:
            bool: 是否可以交易
            str: 当前交易时段描述
        """
        now = dt.datetime.now()
        current_time = now.time()
        current_date = now.date()
        
        # 首先检查是否为交易日
        if not self.is_trading_day(current_date):
            return False, "非交易日，市场休市"
        
        # 定义交易时段
        pre_market_start = dt.time(4, 0)  # 盘前4:00 AM开始
        regular_market_start = dt.time(9, 30)  # 常规交易9:30 AM开始
        regular_market_end = dt.time(16, 0)  # 常规交易4:00 PM结束
        after_market_end = dt.time(20, 0)  # 盘后8:00 PM结束
        
        # 检查当前时间是否在交易时段内
        if pre_market_start <= current_time < regular_market_start:
            return True, "盘前交易时段"
        elif regular_market_start <= current_time < regular_market_end:
            return True, "常规交易时段"
        elif regular_market_end <= current_time < after_market_end:
            return True, "盘后交易时段"
        else:
            return False, "当前为非交易时间"

    def execute_trades(self, long_stocks, short_stocks):
        """
        执行交易 - 增强了错误处理、NaN值的防护，并添加防止重复交易和交易时间检查的逻辑
        
        参数:
            long_stocks: 做多股票列表
            short_stocks: 做空股票列表
        """
        try:
            # 检查当前是否可以交易
            can_trade, trading_session = self.can_trade_now()
            if not can_trade:
                log_warning(f"当前无法交易: {trading_session}，跳过交易执行")
                return
            else:
                log_info(f"当前为{trading_session}，可以执行交易")
                
            # 更新当前持仓
            self.get_current_positions()
            
            # 如果没有符合条件的交易标的，直接返回
            if not long_stocks and not short_stocks:
                log_info("没有符合条件的交易")
                return
                
            # 获取账户净资产
            account_summary = self.ib.accountSummary()
            net_liquidation = 0
            for summary in account_summary:
                if summary.tag == 'NetLiquidation' and summary.currency == 'USD':
                    try:
                        net_liquidation = float(summary.value)
                    except (ValueError, TypeError):
                        log_error(f"无法转换净资产值: {summary.value}")
                    break
                    
            if net_liquidation <= 0:
                log_error("获取账户净值失败或净值为零")
                return
            
            # =============== 调整资金配置的参数 ===============
            # 设置要使用的账户总资产比例 (0.0 - 1.0)
            capital_ratio = 0.3  # 使用30%的账户资金
            
            # 多空资金分配比例
            long_allocation_ratio = 0.5  # 分配50%给多头
            short_allocation_ratio = 0.5  # 分配50%给空头
            
            # 计算每个方向的资金
            total_capital = net_liquidation * capital_ratio
            long_capital = total_capital * long_allocation_ratio
            short_capital = total_capital * short_allocation_ratio
            
            # 记录资金分配情况
            log_info(f"账户总资产: ${net_liquidation:.2f}")
            log_info(f"用于交易的资金: ${total_capital:.2f} ({capital_ratio*100:.0f}%的总资产)")
            log_info(f"多头资金: ${long_capital:.2f} ({long_allocation_ratio*100:.0f}%的交易资金)")
            log_info(f"空头资金: ${short_capital:.2f} ({short_allocation_ratio*100:.0f}%的交易资金)")
            # ================================================
            
            # 检查当前是否在常规交易时间以决定使用什么类型的订单
            current_time = dt.datetime.now().time()
            is_regular_hours = (
                (current_time >= dt.time(9, 30) and current_time <= dt.time(16, 0)) and
                (dt.datetime.now().weekday() < 5)  # 工作日
            )
            
            # 决定是否使用限价单
            use_limit_order = not is_regular_hours  # 盘前盘后使用限价单
            
            # 计算每个股票的资金
            long_capital_per_stock = long_capital / max(len(long_stocks), 1) if long_stocks else 0
            short_capital_per_stock = short_capital / max(len(short_stocks), 1) if short_stocks else 0
            
            # 记录每股分配资金
            if long_stocks:
                log_info(f"多头每股分配: ${long_capital_per_stock:.2f} x {len(long_stocks)}只")
            if short_stocks:
                log_info(f"空头每股分配: ${short_capital_per_stock:.2f} x {len(short_stocks)}只")
            
            # 记录交易时段
            if is_regular_hours:
                log_info("当前为常规交易时段")
            else:
                log_info("当前为盘前/盘后交易时段，将使用限价单")
            
            # 加载交易记录，用于防止重复交易
            self.load_trade_history()
            
            # 执行多头交易
            long_success = 0
            for symbol in long_stocks:
                try:
                    # 检查股票代码有效性
                    if not symbol or pd.isna(symbol):
                        log_warning(f"跳过无效股票代码: {symbol}")
                        continue
                    
                    # 检查是否已经持有该股票（防止重复买入）
                    if symbol in self.positions and self.positions[symbol] > 0:
                        log_info(f"已持有{symbol}多头仓位({self.positions[symbol]}股)，跳过买入")
                        continue
                    
                    # 检查今日是否已经交易过该股票（防止当天重复交易）
                    if self.is_traded_today(symbol, 'BUY'):
                        log_info(f"{symbol}今日已执行过买入交易，跳过")
                        continue
                    
                    # 检查冷却期
                    if not self.can_buy_again(symbol):
                        log_info(f"{symbol}在冷却期内，跳过")
                        continue
                        
                    # 获取当前价格
                    current_price = self.get_latest_price(symbol)
                    if current_price <= 0:
                        log_error(f"获取{symbol}价格失败")
                        continue
                        
                    # 计算购买数量
                    quantity = int(long_capital_per_stock / current_price)
                    if quantity <= 0:
                        log_info(f"{symbol}计算买入数量为0，跳过")
                        continue
                        
                    # 执行买入
                    success = self.place_order(
                        symbol=symbol, 
                        action="BUY", 
                        quantity=quantity, 
                        reason="收益公告反转策略-做多",
                        use_limit_price=use_limit_order
                    )
                    
                    if success:
                        long_success += 1
                        # 记录交易到历史记录
                        self.record_trade(symbol, 'BUY', quantity, current_price)
                
                except Exception as e:
                    log_error(f"执行{symbol}多头交易出错: {str(e)}")
                    log_error(traceback.format_exc())
                    continue
            
            # 执行空头交易
            short_success = 0
            for symbol in short_stocks:
                try:
                    # 检查股票代码有效性
                    if not symbol or pd.isna(symbol):
                        log_warning(f"跳过无效股票代码: {symbol}")
                        continue
                    
                    # 检查是否已经持有该股票空头仓位（防止重复卖出）
                    if symbol in self.positions and self.positions[symbol] < 0:
                        log_info(f"已持有{symbol}空头仓位({abs(self.positions[symbol])}股)，跳过卖出")
                        continue
                    
                    # 检查今日是否已经交易过该股票（防止当天重复交易）
                    if self.is_traded_today(symbol, 'SELL'):
                        log_info(f"{symbol}今日已执行过卖出交易，跳过")
                        continue
                    
                    # 检查冷却期
                    if not self.can_buy_again(symbol):
                        log_info(f"{symbol}在冷却期内，跳过")
                        continue
                        
                    # 获取当前价格
                    current_price = self.get_latest_price(symbol)
                    if current_price <= 0:
                        log_error(f"获取{symbol}价格失败")
                        continue
                        
                    # 计算卖出数量
                    quantity = int(short_capital_per_stock / current_price)
                    if quantity <= 0:
                        log_info(f"{symbol}计算卖出数量为0，跳过")
                        continue
                        
                    # 执行卖空
                    success = self.place_order(
                        symbol=symbol, 
                        action="SELL", 
                        quantity=quantity, 
                        reason="收益公告反转策略-做空",
                        use_limit_price=use_limit_order
                    )
                    
                    if success:
                        short_success += 1
                        # 记录交易到历史记录
                        self.record_trade(symbol, 'SELL', quantity, current_price)
                
                except Exception as e:
                    log_error(f"执行{symbol}空头交易出错: {str(e)}")
                    log_error(traceback.format_exc())
                    continue
                    
            log_info(f"交易执行完成 - 成功执行{long_success}笔多头交易和{short_success}笔空头交易")
            
        except Exception as e:
            log_error(f"执行交易出错: {str(e)}")
            log_error(traceback.format_exc())
    def monitor_positions(self):
        """
        监控持仓
        """
        try:
            # 更新当前持仓
            positions = self.get_current_positions()
            
            if not positions:
                log_info("当前没有持仓，跳过监控")
                return
                
            log_info(f"开始监控{len(positions)}个持仓")
            
            # 遍历所有持仓
            for symbol, quantity in positions.items():
                try:
                    # 获取当前价格
                    current_price = self.get_latest_price(symbol)
                    
                    if current_price <= 0:
                        log_warning(f"无法获取{symbol}当前价格，跳过止损检查")
                        continue
                    
                    # 检查止损
                    if quantity > 0 and self.check_stop_loss(symbol, current_price):
                        log_trade(f"{symbol}触发止损, 当前价格: {current_price}, 止损价格: {self.stop_loss_prices[symbol]}")
                        # 平仓
                        self.place_order(symbol, "SELL", abs(quantity), "触发止损")
                except Exception as e:
                    log_error(f"监控{symbol}持仓出错: {str(e)}")
                    continue
                    
            log_info("持仓监控完成")
            
        except Exception as e:
            log_error(f"监控持仓出错: {str(e)}")
            log_error(traceback.format_exc())
            
    def check_exit_positions(self):
        """
        检查是否需要平仓
        """
        try:
            # 更新当前持仓
            positions = self.get_current_positions()
            
            if not positions:
                log_info("当前没有持仓，跳过平仓检查")
                return
                
            # 获取当前日期
            today = dt.datetime.now().date()
            
            # 确保earnings_data的earnings_date列是日期类型
            if self.earnings_data is not None and not self.earnings_data.empty:
                if 'earnings_date' in self.earnings_data.columns:
                    if not pd.api.types.is_datetime64_any_dtype(self.earnings_data['earnings_date']):
                        self.earnings_data['earnings_date'] = pd.to_datetime(self.earnings_data['earnings_date'])
            
            # 获取收益公告日期
            earnings_dates = {}
            if self.earnings_data is not None and not self.earnings_data.empty:
                for symbol in positions.keys():
                    symbol_earnings = self.earnings_data[self.earnings_data['symbol'] == symbol]
                    if not symbol_earnings.empty:
                        earnings_date = pd.to_datetime(symbol_earnings['earnings_date'].iloc[0]).date()
                        earnings_dates[symbol] = earnings_date
            
            # 检查每个持仓是否需要平仓
            for symbol, quantity in positions.items():
                try:
                    if symbol in earnings_dates:
                        earnings_date = earnings_dates[symbol]
                        
                        # 如果今天是收益公告后第二天(t+2)，则平仓
                        if today >= earnings_date + dt.timedelta(days=2):
                            log_trade(f"{symbol}达到持有期限，准备平仓")
                            
                            if quantity > 0:
                                # 平多仓
                                self.place_order(symbol, "SELL", abs(quantity), "持有期结束")
                            elif quantity < 0:
                                # 平空仓
                                self.place_order(symbol, "BUY", abs(quantity), "持有期结束")
                except Exception as e:
                    log_error(f"检查{symbol}平仓出错: {str(e)}")
                    continue
                            
            log_info("检查平仓完成")
            
        except Exception as e:
            log_error(f"检查平仓出错: {str(e)}")
            log_error(traceback.format_exc())

    def print_earnings_calendar(self, days_range=7):
        """
        打印未来几天的收益公告日历
        
        参数:
            days_range: 查看未来几天的收益公告
        """
        try:
            # 确保数据已加载
            if self.earnings_data is None or self.earnings_data.empty:
                log_error("收益公告数据未加载或为空")
                return
            
            # 获取当前日期
            today = dt.datetime.now().date()
            
            # 确保earnings_date列是日期时间格式
            if not pd.api.types.is_datetime64_any_dtype(self.earnings_data['earnings_date']):
                self.earnings_data['earnings_date'] = pd.to_datetime(self.earnings_data['earnings_date'])
            
            # 筛选未来几天的收益公告
            upcoming = self.earnings_data[
                (self.earnings_data['earnings_date'].dt.date >= today) &
                (self.earnings_data['earnings_date'].dt.date <= today + dt.timedelta(days=days_range))
            ]
            
            if upcoming.empty:
                log_info(f"未来{days_range}天没有收益公告")
                return
            
            # 按日期分组
            grouped = upcoming.groupby(upcoming['earnings_date'].dt.date)
            
            # 遍历每一天
            for date, group in grouped:
                # 创建表格
                table = PrettyTable()
                table.field_names = ["股票", "公司名称", "公告时段", "预期EPS"]
                
                # 设置对齐
                table.align["股票"] = "l"
                table.align["公司名称"] = "l"
                table.align["公告时段"] = "c"
                table.align["预期EPS"] = "r"
                
                # 添加数据
                for _, row in group.iterrows():
                    eps = row['estimated_eps'] if 'estimated_eps' in row and pd.notna(row['estimated_eps']) else 'N/A'
                    time_of_day = row['time_of_day'] if 'time_of_day' in row and pd.notna(row['time_of_day']) else 'N/A'
                    company = row['company_name'] if 'company_name' in row and pd.notna(row['company_name']) else 'N/A'
                    
                    table.add_row([
                        row['symbol'],
                        company[:30] + '...' if isinstance(company, str) and len(company) > 30 else company,
                        time_of_day,
                        eps
                    ])
                
                # 打印表格
                date_str = date.strftime('%Y-%m-%d')
                log_info(f"\n{date_str} 收益公告 ({len(group)}只股票):")
                log_info("\n" + str(table))
        
        except Exception as e:
            log_error(f"打印收益公告日历出错: {str(e)}")
            log_error(traceback.format_exc())

    def print_strategy_dashboard(self):
        """
        打印策略仪表盘 - 汇总重要信息
        """
        try:
            # 打印当前时间
            current_time = dt.datetime.now()
            log_info("\n" + "="*80)
            log_info(f"收益公告反转策略仪表盘 - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            log_info("="*80)
            
            # 打印投资组合摘要
            self.print_portfolio_summary()
            
            # 打印最近的交易历史
            self.print_trade_history(days=3)
            
            # 打印未来几天的收益公告
            log_info("\n未来收益公告日历:")
            self.print_earnings_calendar(days_range=5)
            
            # 打印策略状态
            account_summary = self.get_account_summary()
            allocated_capital = account_summary.get('账户净值', 0) * 0.8  # 假设分配80%资金
            
            # 创建策略状态表格
            status_table = PrettyTable()
            status_table.field_names = ["指标", "值"]
            status_table.align["指标"] = "l"
            status_table.align["值"] = "r"
            
            # 添加策略状态数据
            status_table.add_row(["策略状态", "运行中"])
            status_table.add_row(["分配资金", f"${allocated_capital:,.2f}"])
            status_table.add_row(["当前持仓数量", f"{len(self.positions)}"])
            status_table.add_row(["止损比例", f"{self.stop_loss_percent*100:.1f}%"])
            status_table.add_row(["交易冷却期", f"{self.cooldown_seconds//86400}天"])
            
            # 打印策略状态
            log_info("\n策略状态:")
            log_info("\n" + str(status_table))
            
            log_info("\n" + "="*80)
            
        except Exception as e:
            log_error(f"打印策略仪表盘出错: {str(e)}")
            log_error(traceback.format_exc())

    def print_portfolio_summary(self):
        """
        打印投资组合摘要
        """
        try:
            # 获取当前持仓
            portfolio = self.ib.portfolio()
            
            if not portfolio:
                log_info("当前没有持仓")
                return
            
            # 创建PrettyTable对象
            table = PrettyTable()
            
            # 设置表头
            table.field_names = ["股票", "持仓数量", "平均成本", "当前价格", "市值", "盈亏", "盈亏%"]
            
            # 设置对齐方式
            table.align = "r"
            table.align["股票"] = "l"
            
            # 添加数据
            total_market_value = 0
            total_cost_basis = 0
            total_pnl = 0
            
            for position in portfolio:
                try:
                    symbol = position.contract.symbol
                    quantity = position.position
                    avg_cost = position.averageCost
                    market_price = position.marketPrice
                    market_value = position.marketValue
                    unrealized_pnl = position.unrealizedPNL
                    
                    # 计算成本基础
                    cost_basis = avg_cost * abs(quantity)
                    
                    # 计算盈亏百分比
                    pnl_percent = (unrealized_pnl / cost_basis) * 100 if cost_basis != 0 else 0
                    
                    # 添加到总计
                    total_market_value += market_value
                    total_cost_basis += cost_basis
                    total_pnl += unrealized_pnl
                    
                    # 格式化盈亏显示
                    pnl_str = f"{unrealized_pnl:.2f}"
                    pnl_percent_str = f"{pnl_percent:.2f}%"
                    
                    table.add_row([
                        symbol,
                        f"{int(quantity)}",
                        f"{avg_cost:.2f}",
                        f"{market_price:.2f}",
                        f"{market_value:.2f}",
                        pnl_str,
                        pnl_percent_str
                    ])
                except Exception as e:
                    log_error(f"处理{symbol}持仓信息出错: {str(e)}")
                    continue
            
            # 计算总体盈亏百分比
            total_pnl_percent = (total_pnl / total_cost_basis) * 100 if total_cost_basis != 0 else 0
            
            # 添加总计行
            table.add_row([
                "总计",
                "",
                "",
                "",
                f"{total_market_value:.2f}",
                f"{total_pnl:.2f}",
                f"{total_pnl_percent:.2f}%"
            ])
            
            # 打印表格
            log_info("\n当前投资组合摘要:")
            log_info("\n" + str(table))
            
            # 获取账户摘要
            account_summary = self.get_account_summary()
            
            # 创建账户信息表格
            account_table = PrettyTable()
            account_table.field_names = ["指标", "金额"]
            account_table.align["指标"] = "l"
            account_table.align["金额"] = "r"
            
            # 添加账户数据
            for key, value in account_summary.items():
                account_table.add_row([key, f"{value:,.2f}"])
            
            # 打印账户信息
            log_info("\n账户摘要:")
            log_info("\n" + str(account_table))
            
        except Exception as e:
            log_error(f"打印投资组合摘要出错: {str(e)}")
            log_error(traceback.format_exc())

    def get_account_summary(self):
        """
        获取账户摘要信息
        
        返回:
            dict: 账户摘要信息字典
        """
        try:
            # 获取账户摘要
            account_summary = self.ib.accountSummary()
            
            # 创建结果字典
            result = {}
            
            # 提取感兴趣的字段
            for summary in account_summary:
                if summary.currency == 'USD':
                    if summary.tag == 'NetLiquidation':
                        try:
                            result['账户净值'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['账户净值'] = 0.0
                    elif summary.tag == 'TotalCashValue':
                        try:
                            result['现金余额'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['现金余额'] = 0.0
                    elif summary.tag == 'UnrealizedPnL':
                        try:
                            result['未实现盈亏'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['未实现盈亏'] = 0.0
                    elif summary.tag == 'RealizedPnL':
                        try:
                            result['已实现盈亏'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['已实现盈亏'] = 0.0
                    elif summary.tag == 'AvailableFunds':
                        try:
                            result['可用资金'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['可用资金'] = 0.0
                    elif summary.tag == 'BuyingPower':
                        try:
                            result['购买力'] = float(summary.value)
                        except (ValueError, TypeError):
                            result['购买力'] = 0.0
            
            return result
        
        except Exception as e:
            log_error(f"获取账户摘要出错: {str(e)}")
            log_error(traceback.format_exc())
            return {}

    def print_trade_history(self, days=3):
        """
        打印最近的交易历史 - 修复版本，兼容新版IB API的Execution对象结构
        
        参数:
            days: 查询过去几天的交易历史
        """
        try:
            # 获取交易历史
            executions = self.ib.executions()
            
            if not executions:
                log_info(f"过去{days}天没有交易记录")
                return
                
            # 获取所有交易的详细信息，包括合约信息
            trades = []
            for execution in executions:
                try:
                    # 检查execution是否有contract属性
                    if hasattr(execution, 'contract'):
                        # 旧版API结构
                        contract = execution.contract
                        symbol = contract.symbol
                        exchange = execution.exchange
                    else:
                        # 新版API结构 - 需要通过其他方式获取合约信息
                        # 方法1: 如果execution有其他属性包含symbol信息
                        if hasattr(execution, 'symbol'):
                            symbol = execution.symbol
                        elif hasattr(execution, 'ticker'):
                            symbol = execution.ticker
                        else:
                            # 如果找不到symbol，使用占位符
                            symbol = "未知"
                            
                        # 同样尝试获取exchange
                        exchange = execution.exchange if hasattr(execution, 'exchange') else '未知'
                    
                    # 提取其他执行细节
                    exec_time = execution.time if hasattr(execution, 'time') else dt.datetime.now()
                    shares = execution.shares if hasattr(execution, 'shares') else 0
                    price = execution.price if hasattr(execution, 'price') else 0
                    side = execution.side if hasattr(execution, 'side') else '未知'
                    
                    trades.append({
                        'date': exec_time.date(),
                        'time': exec_time.time(),
                        'symbol': symbol,
                        'side': side,
                        'shares': shares,
                        'price': price,
                        'value': shares * price,
                        'exchange': exchange
                    })
                except Exception as e:
                    log_error(f"处理交易执行记录时出错: {str(e)}")
                    continue
            
            # 筛选最近几天的交易
            today = dt.datetime.now().date()
            recent_trades = [t for t in trades if (today - t['date']).days <= days]
            
            if not recent_trades:
                log_info(f"过去{days}天没有交易记录")
                return
            
            # 创建表格
            table = PrettyTable()
            table.field_names = ["日期", "时间", "股票", "方向", "数量", "价格", "金额", "交易所"]
            
            # 设置对齐
            table.align = "r"
            table.align["日期"] = "l"
            table.align["时间"] = "l"
            table.align["股票"] = "l"
            table.align["方向"] = "c"
            table.align["交易所"] = "l"
            
            # 添加数据
            for trade in sorted(recent_trades, key=lambda x: x['date'], reverse=True):
                table.add_row([
                    trade['date'].strftime('%Y-%m-%d'),
                    trade['time'].strftime('%H:%M:%S'),
                    trade['symbol'],
                    trade['side'],
                    f"{int(trade['shares'])}",
                    f"{trade['price']:.2f}",
                    f"{trade['value']:.2f}",
                    trade['exchange']
                ])
            
            # 打印表格
            log_info(f"\n最近{days}天的交易历史:")
            log_info("\n" + str(table))
            
            # 统计买入和卖出
            buy_count = sum(1 for t in recent_trades if t['side'] == 'BOT')
            sell_count = sum(1 for t in recent_trades if t['side'] == 'SLD')
            buy_value = sum(t['value'] for t in recent_trades if t['side'] == 'BOT')
            sell_value = sum(t['value'] for t in recent_trades if t['side'] == 'SLD')
            
            # 打印统计信息
            log_info(f"\n交易统计:")
            log_info(f"买入交易: {buy_count}笔, 总金额: ${buy_value:.2f}")
            log_info(f"卖出交易: {sell_count}笔, 总金额: ${sell_value:.2f}")
            log_info(f"净买入金额: ${(buy_value - sell_value):.2f}")
            
        except Exception as e:
            log_error(f"打印交易历史出错: {str(e)}")
            log_error(traceback.format_exc())
    def run(self):
        """
        运行策略
        
        返回:
            bool: 策略运行是否成功
        """
        log_info("开始运行收益公告反转策略")
        
        try:
            # 检查是否有收益公告数据
            if self.earnings_data is None or self.earnings_data.empty:
                log_error("收益公告数据为空，尝试重新获取...")
                if not self.load_earnings_data():
                    log_error("无法获取收益公告数据，策略无法运行")
                    return False
            
            # 打印策略仪表盘
            self.print_strategy_dashboard()
            
            # 选股
            long_stocks, short_stocks = self.select_stocks()
            
            # 执行交易
            self.execute_trades(long_stocks, short_stocks)
            
            # 监控持仓
            self.monitor_positions()
            
            # 检查平仓
            self.check_exit_positions()
            
            log_info("策略执行完成")
            return True
            
        except Exception as e:
            log_error(f"策略运行出错: {str(e)}")
            log_error(traceback.format_exc())
            return False
class EarningsReversalApp:
    """收益公告反转策略应用 - 优化版"""
    
    def __init__(self):
        self.ib = None
        self.strategy = None
        self.is_running = False
        self.run_interval = 60 * 60  # 默认每小时运行一次
    
    def connect_to_ib(self, host='127.0.0.1', port=7497, client_id=1, max_attempts=3, retry_interval=5):
        """
        连接到Interactive Brokers，增加了重试机制
        
        参数:
            host: IB Gateway/TWS主机地址
            port: 端口号
            client_id: 客户端ID
            max_attempts: 最大尝试次数
            retry_interval: 重试间隔（秒）
            
        返回:
            bool: 是否成功连接
        """
        attempt = 0
        
        while attempt < max_attempts:
            try:
                attempt += 1
                log_info(f"尝试连接到IB (host={host}, port={port}, client_id={client_id})... 第{attempt}次尝试")
                
                # 创建IB连接
                self.ib = IB()
                self.ib.connect(host, port, clientId=client_id)
                
                if self.ib.isConnected():
                    log_info("成功连接到Interactive Brokers")
                    return True
                else:
                    log_error("连接到Interactive Brokers失败")
                    
                    if attempt < max_attempts:
                        log_info(f"将在{retry_interval}秒后重试...")
                        time.sleep(retry_interval)
            except Exception as e:
                log_error(f"连接到Interactive Brokers时出错: {str(e)}")
                log_error(traceback.format_exc())
                
                if attempt < max_attempts:
                    log_info(f"将在{retry_interval}秒后重试...")
                    time.sleep(retry_interval)
        
        log_error(f"尝试{max_attempts}次后仍无法连接到IB")
        return False
    
    def initialize_strategy(self, earnings_data_path=None, cooldown_days=10, 
                           stop_loss_percent=0.05, min_price=5.0, min_volume=100000, exclude_otc=True):
        """
        初始化策略
        
        参数:
            earnings_data_path: 收益公告数据文件路径
            cooldown_days: 冷却期（天）
            stop_loss_percent: 止损百分比
            min_price: 最低股票价格筛选
            min_volume: 最低日均交易量筛选
            exclude_otc: 是否排除场外交易股票
            
        返回:
            bool: 是否成功初始化策略
        """
        try:
            if self.ib is None or not self.ib.isConnected():
                log_error("未连接到IB，无法初始化策略")
                return False
            
            # 初始化策略
            self.strategy = EarningsReversalStrategy(
                ib_connection=self.ib,
                earnings_data_path=earnings_data_path,
                cooldown_days=cooldown_days,
                stop_loss_percent=stop_loss_percent,
                min_price=min_price,
                min_volume=min_volume,
                exclude_otc=exclude_otc
            )
            
            log_info("策略初始化完成")
            return True
                
        except Exception as e:
            log_error(f"初始化策略时出错: {str(e)}")
            log_error(traceback.format_exc())
            return False
    
    def run_strategy_once(self):
        """
        运行一次策略
        
        返回:
            bool: 策略运行是否成功
        """
        try:
            if self.strategy is None:
                log_error("策略未初始化，无法运行")
                return False
            
            # 验证IB连接状态
            if not self.ib or not self.ib.isConnected():
                log_error("IB连接已断开，尝试重新连接...")
                if not self.connect_to_ib():
                    log_error("重新连接IB失败，无法运行策略")
                    return False
                
                # 重新初始化策略
                if not self.initialize_strategy():
                    log_error("重新初始化策略失败")
                    return False
            
            return self.strategy.run()
                
        except Exception as e:
            log_error(f"运行策略时出错: {str(e)}")
            log_error(traceback.format_exc())
            return False
    
    def start_strategy_loop(self, interval_seconds=None):
        """
        启动策略循环
        
        参数:
            interval_seconds: 策略运行间隔（秒）
        """
        try:
            if interval_seconds is not None:
                self.run_interval = interval_seconds
            
            log_info(f"启动策略循环，间隔{self.run_interval}秒")
            self.is_running = True
            
            while self.is_running:
                # 运行一次策略
                success = self.run_strategy_once()
                
                if not success:
                    log_error("策略执行失败")
                
                # 等待下一次运行
                next_run_time = dt.datetime.now() + dt.timedelta(seconds=self.run_interval)
                log_info(f"等待{self.run_interval}秒后再次运行 (下次运行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')})")
                
                # 使用小间隔检查是否需要停止循环，便于及时响应中断
                wait_interval = 10  # 每10秒检查一次
                for _ in range(self.run_interval // wait_interval):
                    if not self.is_running:
                        break
                    time.sleep(wait_interval)
                
                # 处理剩余的等待时间
                remaining_seconds = self.run_interval % wait_interval
                if remaining_seconds > 0 and self.is_running:
                    time.sleep(remaining_seconds)
                
        except KeyboardInterrupt:
            log_info("收到中断信号，停止策略循环")
            self.is_running = False
            
        except Exception as e:
            log_error(f"策略循环出错: {str(e)}")
            log_error(traceback.format_exc())
            self.is_running = False
            
        finally:
            self.stop()
    
    def stop(self):
        """
        停止策略并断开连接
        """
        log_info("正在停止策略并断开连接...")
        self.is_running = False
        
        if self.ib is not None and self.ib.isConnected():
            try:
                self.ib.disconnect()
                log_info("已断开与Interactive Brokers的连接")
            except Exception as e:
                log_error(f"断开连接时出错: {str(e)}")
    
    def update_earnings_data(self):
        """
        更新收益公告数据 - 修复版，正确处理data_collector引用
        
        返回:
            bool: 是否成功更新数据
        """
        try:
            log_info("开始更新收益公告数据...")
            
            # 如果之前没有创建过数据收集器，先创建一个
            if self.data_collector is None:
                self.data_collector = EarningsDataCollector(
                    min_price=self.min_price, 
                    min_volume=self.min_volume, 
                    exclude_otc=self.exclude_otc
                )
            
            # 强制更新数据
            new_data = self.data_collector.get_earnings_data(force_update=True)
            
            if new_data is not None and not new_data.empty:
                # 更新数据
                self.earnings_data = new_data
                log_info(f"成功更新收益公告数据，共{len(new_data)}条记录")
                return True
            else:
                log_error("获取新数据失败")
                return False
                    
        except Exception as e:
            log_error(f"更新收益公告数据出错: {str(e)}")
            log_error(traceback.format_exc())
            return False

def initialize_strategy(self, earnings_data_path=None, cooldown_days=10, 
                       stop_loss_percent=0.05, min_price=5.0, min_volume=100000, exclude_otc=True):
    """
    初始化策略 - 增强数据加载检查
    
    参数:
        earnings_data_path: 收益公告数据文件路径
        cooldown_days: 冷却期（天）
        stop_loss_percent: 止损百分比
        min_price: 最低股票价格筛选
        min_volume: 最低日均交易量筛选
        exclude_otc: 是否排除场外交易股票
        
    返回:
        bool: 是否成功初始化策略
    """
    try:
        if self.ib is None or not self.ib.isConnected():
            log_error("未连接到IB，无法初始化策略")
            return False
        
        # 初始化策略
        self.strategy = EarningsReversalStrategy(
            ib_connection=self.ib,
            earnings_data_path=earnings_data_path,
            cooldown_days=cooldown_days,
            stop_loss_percent=stop_loss_percent,
            min_price=min_price,
            min_volume=min_volume,
            exclude_otc=exclude_otc
        )
        
        # 验证数据加载是否成功
        if self.strategy.earnings_data is None or self.strategy.earnings_data.empty:
            log_warning("初始化策略时未能加载收益公告数据，尝试强制更新数据")
            if self.update_earnings_data():
                log_info("强制更新数据成功")
            else:
                log_error("强制更新数据失败，策略可能无法正常运行")
                return False
        
        # 检查数据质量
        if self.strategy.earnings_data is not None and not self.strategy.earnings_data.empty:
            # 检查是否有未来的收益公告
            today = dt.datetime.now().date()
            if 'earnings_date' in self.strategy.earnings_data.columns:
                future_earnings = self.strategy.earnings_data[
                    self.strategy.earnings_data['earnings_date'].dt.date >= today
                ]
                
                if future_earnings.empty:
                    log_warning("加载的收益公告数据中没有未来的收益公告，尝试强制更新数据")
                    if not self.update_earnings_data():
                        log_error("更新数据失败，策略可能无法正常运行")
                        return False
                else:
                    log_info(f"成功加载数据，包含{len(future_earnings)}条未来的收益公告")
        
        log_info("策略初始化完成")
        return True
            
    except Exception as e:
        log_error(f"初始化策略时出错: {str(e)}")
        log_error(traceback.format_exc())
        return False


# 主程序示例
# 主程序示例
if __name__ == "__main__":
    try:
        # 创建应用
        app = EarningsReversalApp()
        
        # 解析命令行参数
        import argparse
        parser = argparse.ArgumentParser(description='收益公告反转策略')
        parser.add_argument('--force-update', action='store_true', help='强制更新收益公告数据')
        parser.add_argument('--port', type=int, default=7497, help='IB Gateway/TWS端口')
        parser.add_argument('--interval', type=int, default=4, help='运行间隔(小时)')
        args = parser.parse_args()
        
        # 连接到IB
        if not app.connect_to_ib(host='127.0.0.1', port=args.port, client_id=1):
            log_error("无法连接到IB，程序退出")
            exit(1)
        
        # 初始化策略
        if not app.initialize_strategy(
            earnings_data_path=None,  # 自动获取数据
            cooldown_days=10,
            stop_loss_percent=0.05,
            min_price=5.0,           # 最低股票价格
            min_volume=100000,       # 最低日交易量
            exclude_otc=True         # 排除OTC股票
        ):
            log_error("无法初始化策略，程序退出")
            app.stop()
            exit(1)
        
        # 如果指定了强制更新参数，则更新数据
        if args.force_update:
            log_info("执行强制更新收益公告数据...")
            if app.update_earnings_data():
                log_info("收益公告数据强制更新成功")
            else:
                log_warning("收益公告数据强制更新失败，将使用现有数据继续")
        
        # 启动策略循环，按指定间隔运行
        interval_hours = args.interval
        log_info(f"开始运行策略，每{interval_hours}小时运行一次")
        app.start_strategy_loop(interval_seconds=interval_hours * 60 * 60)
        
    except Exception as e:
        log_error(f"主程序出错: {str(e)}")
        log_error(traceback.format_exc())
        
    finally:
        # 确保断开连接
        if 'app' in locals():
            app.stop()
