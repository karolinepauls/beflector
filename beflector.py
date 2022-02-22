import logging
import asyncio
import signal
import json
from decimal import Decimal
from typing import NamedTuple
import aiohttp
from websockets import connect

log = logging.getLogger(__name__)


class Offer(NamedTuple):

    price: Decimal
    quantity: Decimal


class OrderBookUpdate():

    def __init__(self, bids, asks):
        self._bids = bids
        self._asks = asks

    @staticmethod
    def _decode(str_offers):
        for str_price, str_quantity in str_offers:
            yield Offer(Decimal(str_price), Decimal(str_quantity))

    @property
    def asks(self):
        return self._decode(self._asks)

    @property
    def bids(self):
        return self._decode(self._bids)


async def fetch_order_book(symbol):
    http_uri = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000'
    log.info("Getting order book: %r", http_uri)
    async with aiohttp.ClientSession() as http_session:
        async with http_session.get(http_uri) as http_response:
            return await http_response.json()


async def watch(symbol):
    ws_uri = f'wss://stream.binance.com:9443/ws/{symbol.lower()}@depth'
    log.info("Listening to: %r", ws_uri)
    async with connect(ws_uri) as websocket:
        # Using build-in websocket message buffering to keep messages while we query for the order
        # book snapshot.

        order_book_snapshot = await fetch_order_book(symbol)
        yield OrderBookUpdate(bids=order_book_snapshot['bids'], asks=order_book_snapshot['asks'])

        order_book_update_id = order_book_snapshot['lastUpdateId']
        log.info("Got order book (update id %r)", order_book_update_id)

        prev_last_update = None
        while True:
            recv = await websocket.recv()
            msg = json.loads(recv)

            msg_type = msg.get('e')
            if msg_type != "depthUpdate":
                log.debug('Message is not depthUpdate. Skipping message, type: %s', msg_type)

            first_update = msg['U']
            last_update = msg['u']

            # The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
            # Drop any event where last_update is <= lastUpdateId in the snapshot.
            if last_update <= order_book_update_id:
                log.info("Ignoring pre-snapshot event: last_update=%r", last_update)

            # While listening to the stream, each new event's U should be equal to the previous
            # event's u+1.
            if prev_last_update is not None and first_update != prev_last_update + 1:
                raise Exception(
                    f"Update continuity error: {first_update=} != {prev_last_update=}")

            yield OrderBookUpdate(bids=msg['b'], asks=msg['a'])
            prev_last_update = last_update


async def watch_and_print():
    async for i in watch('BNBBTC'):
        print("Asks", list(i.asks))
        print("Bids", list(i.bids))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    task = loop.create_task(watch_and_print())

    def stop():
        log.info('Stopping')
        task.cancel()

    loop.add_signal_handler(signal.SIGINT, stop)
    loop.add_signal_handler(signal.SIGQUIT, stop)
    loop.add_signal_handler(signal.SIGTERM, stop)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
