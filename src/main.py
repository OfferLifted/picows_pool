import asyncio

import uvloop

from data_streams import WSPool
from utils import init_logger


async def main_bybit():
    logger_task = asyncio.create_task(
        init_logger(log_level="DEBUG"), name="logger_task"
    )

    await asyncio.sleep(0)
    pool = WSPool(
        pool_size=3,
        url="wss://stream.bybit.com/v5/public/linear",
        eviction_policy="latency",
        eviction_interval=300,
        enable_ping=True,
        ping_interval=20,
        sub_payload=[
            {
                "op": "subscribe",
                "args": [
                    "orderbook.1.BTCUSDT",
                    "orderbook.1.ETHUSDT",
                    "orderbook.1.SOLUSDT",
                ],
            }
        ],
    )
    await pool.start()
    await asyncio.sleep(400)
    await pool.shutdown()
    if not logger_task.done():
        logger_task.cancel(msg="cancel on exit")


# uvloop.run(main_bybit())


async def main_blofin():
    logger_task = asyncio.create_task(
        init_logger(log_level="DEBUG"), name="logger_task"
    )
    await asyncio.sleep(0)
    pool = WSPool(
        pool_size=3,
        url="wss://openapi.blofin.com/ws/public",
        eviction_policy="msg_rate",
        eviction_interval=30,
        enable_ping=True,
        ping_interval=20,
        sub_payload=[
            {
                "op": "subscribe",
                "args": [
                    {"channel": "books5", "instId": "BTC-USDT"},
                    {"channel": "books5", "instId": "ETH-USDT"},
                    {"channel": "books5", "instId": "SOL-USDT"},
                ],
            }
        ],
    )
    await pool.start()
    await asyncio.sleep(90)
    await pool.shutdown()
    if not logger_task.done():
        logger_task.cancel(msg="cancel on exit")


uvloop.run(main_bybit())
