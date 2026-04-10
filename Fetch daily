"""
A股恐惧与贪婪指数 - Phase 1: 当日数据采集验证脚本 (v4)

新增:
1. NO_PROXY 环境变量,绕过本地代理直连数据源
2. 全局 requests.get monkey patch: Connection: close + 随机 UA + 超时
3. 东财接口 3 秒节流
"""

import os

# 必须在 import requests/akshare 之前设置
os.environ['NO_PROXY'] = (
    'eastmoney.com,push2.eastmoney.com,push2his.eastmoney.com,'
    '80.push2.eastmoney.com,82.push2.eastmoney.com,'
    'sina.com.cn,finance.sina.com.cn,'
    'sse.com.cn,szse.cn,csindex.com.cn'
)

import akshare as ak
import pandas as pd
import numpy as np
import requests
import random
import time
from datetime import datetime, timedelta
from functools import partial
from unittest.mock import patch


# ============================================================
# 全局 HTTP 会话增强
# ============================================================

_UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
]

_original_get = requests.get


def enhanced_get(url, **kwargs):
    """
    增强版 requests.get:
    - 不继承系统环境代理 (trust_env=False)
    - 随机 UA
    - Connection: close 避免长连接被东财切断
    - 默认 15 秒超时
    """
    session = requests.Session()
    session.trust_env = False
    session.headers.update({
        'User-Agent': random.choice(_UA_LIST),
        'Accept': '*/*',
        'Connection': 'close',
    })
    kwargs.setdefault('timeout', 15)
    try:
        return session.get(url, **kwargs)
    finally:
        session.close()


# 全局 patch: akshare 内部所有 requests.get 都走 enhanced_get
requests.get = enhanced_get


# ============================================================
# 东财接口节流器
# ============================================================

_last_em_request_time = 0.0
_EM_MIN_INTERVAL = 3.0


def throttle_em():
    global _last_em_request_time
    now = time.time()
    elapsed = now - _last_em_request_time
    if elapsed < _EM_MIN_INTERVAL:
        time.sleep(_EM_MIN_INTERVAL - elapsed)
    _last_em_request_time = time.time()


# ============================================================
# 工具函数
# ============================================================

def section(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def ok(msg): print(f"  ✓ {msg}")
def warn(msg): print(f"  ⚠ {msg}")
def err(msg): print(f"  ✗ {msg}")


def try_sources(sources, label):
    """依次尝试多个数据源,东财源自动节流"""
    for name, fn in sources:
        try:
            if '东财' in name:
                throttle_em()
            result = fn()
            if result is not None and len(result) > 0:
                ok(f"{label} [来源: {name}]: {len(result)} 行")
                return result, name
        except Exception as e:
            warn(f"{label} 来源 '{name}' 失败: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(0.5)
    err(f"{label} 所有数据源都失败")
    return None, None


def filter_universe(df, name_col='名称'):
    if name_col not in df.columns:
        return df
    mask = (
        ~df[name_col].str.contains('ST', na=False) &
        ~df[name_col].str.contains(r'\*ST', na=False, regex=True) &
        ~df[name_col].str.contains('退', na=False)
    )
    return df[mask].copy()


def get_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def clean_ohlc(df, close_candidates=('close', '收盘')):
    if df is None or len(df) == 0:
        return df
    close_col = get_col(df, close_candidates)
    if close_col is None:
        return df
    df = df.copy()
    df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
    df = df.dropna(subset=[close_col]).reset_index(drop=True)
    return df


# ============================================================
# 指标 1: 市场动量
# ============================================================

def fetch_indicator_1():
    section("指标 1: 市场动量 (A500 + 中证全指)")

    a500_sources = [
        ("新浪 stock_zh_index_daily", lambda: ak.stock_zh_index_daily(symbol="sh000510")),
        ("东财 stock_zh_index_daily_em", lambda: ak.stock_zh_index_daily_em(symbol="sh000510")),
        ("东财 index_zh_a_hist", lambda: ak.index_zh_a_hist(
            symbol="000510", period="daily", start_date="20200101",
            end_date=datetime.now().strftime('%Y%m%d'))),
    ]
    a500, _ = try_sources(a500_sources, "A500")
    a500 = clean_ohlc(a500)

    allshare_sources = [
        ("东财 index_zh_a_hist", lambda: ak.index_zh_a_hist(
            symbol="000985", period="daily", start_date="20200101",
            end_date=datetime.now().strftime('%Y%m%d'))),
        ("东财 stock_zh_index_daily_em", lambda: ak.stock_zh_index_daily_em(symbol="sh000985")),
        ("新浪 stock_zh_index_daily", lambda: ak.stock_zh_index_daily(symbol="sh000985")),
    ]
    allshare, _ = try_sources(allshare_sources, "中证全指")
    allshare = clean_ohlc(allshare)

    def compute_dev(df, label):
        if df is None:
            return None
        cc = get_col(df, ['close', '收盘'])
        dc = get_col(df, ['date', '日期'])
        if cc is None or dc is None:
            err(f"{label} 列名未知: {list(df.columns)}")
            return None
        ok(f"{label} 最新 {df[dc].iloc[-1]}, 收盘 {df[cc].iloc[-1]:.2f}")
        if len(df) >= 60:
            ma60 = df[cc].rolling(60).mean()
            dev = (df[cc].iloc[-1] - ma60.iloc[-1]) / ma60.iloc[-1]
            ok(f"{label} 相对 60 日均线偏离: {dev * 100:.2f}%")
            return dev
        return None

    a500_dev = compute_dev(a500, "A500")
    allshare_dev = compute_dev(allshare, "中证全指")

    if a500_dev is not None and allshare_dev is not None:
        combined = 0.6 * a500_dev + 0.4 * allshare_dev
        ok(f"加权动量信号(0.6 A500 + 0.4 全指): {combined * 100:.2f}%")


# ============================================================
# 指标 2 + 3 + 8: 全市场快照
# ============================================================

def fetch_snapshot_indicators():
    section("全市场快照 (指标 2 / 3 / 8 依赖)")

    sources = [
        ("东财 stock_zh_a_spot_em", lambda: ak.stock_zh_a_spot_em()),
        ("新浪 stock_zh_a_spot", lambda: ak.stock_zh_a_spot()),
    ]
    df, source_name = try_sources(sources, "全市场快照")
    if df is None:
        return

    col_map = {
        'name': '名称', 'changepercent': '涨跌幅',
        'amount': '成交额', 'turnoverratio': '换手率',
        'trade': '最新价'
    }
    df = df.rename(columns=col_map)
    print(f"  可用字段: {[c for c in ['名称','涨跌幅','成交额','换手率'] if c in df.columns]}")

    df_f = filter_universe(df, name_col='名称')
    ok(f"剔除 ST/退市后: {len(df_f)} 只 (-{len(df) - len(df_f)})")

    # 指标 3
    section("指标 3: 资金广度 (成交额加权涨跌)")
    if '涨跌幅' in df_f.columns and '成交额' in df_f.columns:
        df_f['涨跌幅'] = pd.to_numeric(df_f['涨跌幅'], errors='coerce')
        df_f['成交额'] = pd.to_numeric(df_f['成交额'], errors='coerce')
        df_c = df_f.dropna(subset=['涨跌幅', '成交额'])
        up = df_c[df_c['涨跌幅'] > 0]
        down = df_c[df_c['涨跌幅'] < 0]
        v_up = up['成交额'].sum()
        v_down = down['成交额'].sum()
        ok(f"上涨家数 {len(up)}, 下跌家数 {len(down)}")
        ok(f"上涨成交额 {v_up/1e8:.1f} 亿, 下跌成交额 {v_down/1e8:.1f} 亿")
        ok(f"NAV (当日): {(v_up - v_down)/1e8:.1f} 亿")
        warn("EMA19/EMA39 差值需历史 NAV 序列")
    else:
        err(f"字段缺失: {list(df_f.columns)}")

    # 指标 8
    section("指标 8: 市场热度 (等权换手率)")
    if '换手率' in df_f.columns:
        df_f['换手率'] = pd.to_numeric(df_f['换手率'], errors='coerce')
        t = df_f['换手率'].dropna()
        t = t[t > 0]
        ok(f"当日等权平均换手率: {t.mean():.3f}%, 参与 {len(t)} 只")
        warn("5日/250日比值需历史数据")
    else:
        err("字段缺少 '换手率' (新浪源不含此字段,需东财源)")

    # 指标 2
    section("指标 2: 价格强度 (52 周新高/新低)")
    warn("快照不含 52 周高低字段,需 Phase 2 历史数据库")


# ============================================================
# 指标 4: 涨跌停情绪
# ============================================================

def fetch_indicator_4():
    section("指标 4: 涨跌停情绪")

    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    def fetch_with_fallback(fn, label):
        for date, tag in [(today, "今日"), (yesterday, "昨日")]:
            try:
                throttle_em()  # 东财接口节流
                r = fn(date=date)
                ok(f"{tag}{label}: {len(r)} 只")
                return r
            except Exception as e:
                warn(f"{tag}{label}失败: {type(e).__name__}")
        return None

    zt = fetch_with_fallback(ak.stock_zt_pool_em, "涨停股池")
    dt = fetch_with_fallback(ak.stock_zt_pool_dtgc_em, "跌停股池")

    if zt is not None and dt is not None and '名称' in zt.columns:
        zt_f = filter_universe(zt, name_col='名称')
        dt_f = filter_universe(dt, name_col='名称')
        ok(f"剔除 ST 后: 涨停 {len(zt_f)} 只, 跌停 {len(dt_f)} 只")
        raw = np.log((len(zt_f) + 1) / (len(dt_f) + 1))
        ok(f"对数比值 (当日): {raw:.3f}")
        warn("5 日平滑需历史数据")


# ============================================================
# 指标 5: 两融情绪
# ============================================================

def fetch_indicator_5():
    section("指标 5: 两融情绪")

    try:
        sse = ak.stock_margin_sse(
            start_date="20260101",
            end_date=datetime.now().strftime('%Y%m%d')
        )
        ok(f"沪市两融: {len(sse)} 行, 最新 {sse.iloc[0, 0] if len(sse) else 'N/A'}")
        print(f"  字段: {list(sse.columns)}")
        if len(sse) > 0:
            for col in sse.columns:
                if '融资买入' in col:
                    val = pd.to_numeric(sse.iloc[0][col], errors='coerce')
                    ok(f"  最新沪市融资买入额: {val/1e8:.1f} 亿")
                    break
    except Exception as e:
        err(f"沪市两融失败: {e}")

    for date_str, tag in [
        (datetime.now().strftime('%Y%m%d'), "今日"),
        ((datetime.now() - timedelta(days=1)).strftime('%Y%m%d'), "昨日"),
        ((datetime.now() - timedelta(days=2)).strftime('%Y%m%d'), "前日"),
    ]:
        try:
            szse = ak.stock_margin_szse(date=date_str)
            if len(szse) > 0:
                ok(f"深市两融 ({tag}): {len(szse)} 行")
                print(f"  字段: {list(szse.columns)}")
                break
        except Exception as e:
            warn(f"深市两融 {tag} 失败: {type(e).__name__}")


# ============================================================
# 指标 6: 波动率
# ============================================================

def fetch_indicator_6():
    section("指标 6: 波动率 (300ETF QVIX)")

    try:
        qvix = ak.index_option_300etf_qvix()
        qvix = clean_ohlc(qvix)
        ok(f"300ETF QVIX: {len(qvix)} 行, 最新 {qvix['date'].iloc[-1]}, "
           f"收盘 {qvix['close'].iloc[-1]:.2f}")
        if len(qvix) >= 50:
            ma50 = qvix['close'].rolling(50).mean()
            dev = (qvix['close'].iloc[-1] - ma50.iloc[-1]) / ma50.iloc[-1]
            ok(f"QVIX 相对 50 日均线偏离: {dev * 100:.2f}%")
            ok(f"反向信号: {-dev * 100:.2f}%")
    except Exception as e:
        err(f"300ETF QVIX 失败: {e}")

    try:
        q50 = ak.index_option_50etf_qvix()
        q50 = clean_ohlc(q50)
        ok(f"50ETF QVIX (对照): {len(q50)} 行, 收盘 {q50['close'].iloc[-1]:.2f}")
    except Exception as e:
        warn(f"50ETF QVIX 失败: {e}")


# ============================================================
# 指标 7: 避险需求
# ============================================================

def fetch_indicator_7():
    section("指标 7: 避险需求 (A500 vs 10年国债ETF)")

    a500_sources = [
        ("新浪", lambda: ak.stock_zh_index_daily(symbol="sh000510")),
        ("东财", lambda: ak.stock_zh_index_daily_em(symbol="sh000510")),
    ]
    a500, _ = try_sources(a500_sources, "A500")
    a500 = clean_ohlc(a500)

    r_a500 = None
    if a500 is not None:
        cc = get_col(a500, ['close', '收盘'])
        if len(a500) >= 21:
            r_a500 = (a500[cc].iloc[-1] - a500[cc].iloc[-21]) / a500[cc].iloc[-21]
            ok(f"A500 20 日收益: {r_a500 * 100:.2f}%")

    bond_sources = [
        ("东财 fund_etf_hist_em", lambda: ak.fund_etf_hist_em(
            symbol="511260", period="daily", start_date="20250101",
            end_date=datetime.now().strftime('%Y%m%d'), adjust="qfq")),
        ("新浪 fund_etf_hist_sina", lambda: ak.fund_etf_hist_sina(symbol="sh511260")),
        ("指数接口", lambda: ak.stock_zh_index_daily(symbol="sh511260")),
    ]
    bond, _ = try_sources(bond_sources, "10Y 国债 ETF")
    bond = clean_ohlc(bond)

    if bond is not None:
        cc = get_col(bond, ['收盘', 'close'])
        dc = get_col(bond, ['日期', 'date'])
        if cc:
            ok(f"国债 ETF 最新 {bond[dc].iloc[-1]}, 收盘 {bond[cc].iloc[-1]:.3f}")
            if len(bond) >= 21:
                r_bond = (bond[cc].iloc[-1] - bond[cc].iloc[-21]) / bond[cc].iloc[-21]
                ok(f"国债 ETF 20 日收益: {r_bond * 100:.2f}%")
                if r_a500 is not None:
                    ok(f"避险需求信号 (A500 - 债): {(r_a500 - r_bond) * 100:.2f}%")


# ============================================================
# 主流程
# ============================================================

def main():
    print(f"\n{'#' * 60}")
    print(f"#  A股恐惧与贪婪指数 - Phase 1 数据采集验证 (v4)")
    print(f"#  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"#  akshare 版本: {ak.__version__}")
    print(f"#  NO_PROXY 已启用, 东财节流 {_EM_MIN_INTERVAL}s")
    print(f"{'#' * 60}")

    for fn, label in [
        (fetch_indicator_1, "指标 1"),
        (fetch_snapshot_indicators, "快照指标"),
        (fetch_indicator_4, "指标 4"),
        (fetch_indicator_5, "指标 5"),
        (fetch_indicator_6, "指标 6"),
        (fetch_indicator_7, "指标 7"),
    ]:
        try:
            fn()
        except Exception as e:
            err(f"{label} 顶层异常: {e}")

    print(f"\n{'#' * 60}\n#  验证完成\n{'#' * 60}\n")


if __name__ == '__main__':
    main()
