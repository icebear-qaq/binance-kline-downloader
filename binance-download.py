import os
import csv
from binance.um_futures import UMFutures
from datetime import datetime
from tqdm import tqdm

# 初始化 Binance UM Futures 客户端
um_futures_client = UMFutures()

def save_data_to_csv(symbol, interval, data):
    """
    将 K 线数据保存到 CSV 文件
    :param symbol: 交易对，例如 BTCUSDT
    :param interval: K 线周期，例如 1m, 15m, 1h 等
    :param data: K 线数据列表
    """
    folder = f'kline/{symbol}/{interval}'
    if not os.path.exists(folder):
        os.makedirs(folder)
    file_path = f'{folder}/{symbol}_{interval}.csv'

    # 定义 CSV 文件表头
    header = [
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close Time', 'Quote Volume', 'Number of Trades',
        'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
    ]

    # 写入数据到 CSV 文件
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
    print(f"数据已保存至 {file_path}")


def load_existing_data(symbol, interval):
    """
    加载本地存储的 K 线数据
    :param symbol: 交易对，例如 BTCUSDT
    :param interval: K 线周期，例如 1m, 15m, 1h 等
    :return: 本地存储的 K 线数据列表
    """
    file_path = f'kline/{symbol}/{interval}/{symbol}_{interval}.csv'
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过表头
            data = []
            for row in reader:
                # 转换数据类型以匹配 API 返回格式
                converted = [
                    int(row[0]),  # Open Time (timestamp)
                    float(row[1]),  # Open
                    float(row[2]),  # High
                    float(row[3]),  # Low
                    float(row[4]),  # Close
                    float(row[5]),  # Volume
                    int(row[6]),  # Close Time (timestamp)
                    float(row[7]),  # Quote Volume
                    int(row[8]),  # Number of Trades
                    float(row[9]),  # Taker Buy Base
                    float(row[10]),  # Taker Buy Quote
                    float(row[11])  # Ignore
                ]
                data.append(converted)
        return data
    return []


def fetch_kline_data(symbol, interval, total_bars):
    """
    获取 K 线数据，自动合并本地与远程数据
    :param symbol: 交易对，例如 BTCUSDT
    :param interval: K 线周期，例如 1m, 15m, 1h 等
    :param total_bars: 需要获取的总 K 线数量
    :return: 合并后的 K 线数据列表
    """
    # 加载本地数据
    existing_data = load_existing_data(symbol, interval)

    # 如果本地数据充足，直接返回
    if len(existing_data) >= total_bars:
        print(f"本地数据充足（{len(existing_data)}条），直接使用缓存")
        return existing_data[-total_bars:]

    # 计算需要获取的数据量
    need_fetch = total_bars - len(existing_data)
    all_data = []
    end_time = None

    # 使用进度条显示下载进度
    with tqdm(total=need_fetch, desc=f"下载 {symbol} {interval} 数据", unit="条") as pbar:
        while need_fetch > 0:
            # 每次请求最多 1000 条数据
            limit = min(need_fetch, 1000)

            # 设置请求参数
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            if end_time:
                params['endTime'] = end_time

            # 从 Binance API 获取数据
            response = um_futures_client.klines(**params)
            if not response:
                print("未获取到更多数据，可能已达历史数据上限。")
                break

            # 更新数据列表和进度条
            all_data = response + all_data  # 保证时间顺序
            fetched = len(response)
            need_fetch -= fetched
            pbar.update(fetched)

            # 更新下一次请求的结束时间
            end_time = response[0][0] - 1  # 使用第一条数据的时间戳前移 1ms

    # 合并本地和远程数据，并按时间排序
    merged_data = all_data + existing_data
    merged_data.sort(key=lambda x: x[0])

    # 保存合并后的数据
    save_data_to_csv(symbol, interval, merged_data[-total_bars:])

    return merged_data[-total_bars:]


def get_user_input():
    """
    获取用户输入的交易对、周期和数据量
    :return: 用户输入的 symbol, interval, total_bars
    """
    symbol = input("请输入交易对（例如 BTCUSDT）：").strip().upper()
    interval = input("请输入 K 线周期（例如 1m, 15m, 1h）：").strip().lower()
    total_bars = int(input("请输入需要获取的 K 线数量：").strip())
    return symbol, interval, total_bars


if __name__ == "__main__":
    print("欢迎使用 K 线数据下载工具")
    symbol, interval, total_bars = get_user_input()
    print(f"开始获取 {symbol} {interval} 数据，共计 {total_bars} 条...")

    data = fetch_kline_data(symbol, interval, total_bars)
    print(f"数据获取完成，总条数：{len(data)}")