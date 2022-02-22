import logging
import asyncio
import signal
import json
import socket
from decimal import Decimal
from typing import Sequence, NamedTuple, List, Iterator
import aiohttp
from sortedcontainers import SortedDict
from websockets import connect
from websockets.exceptions import ConnectionClosedError


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


class OrderBook():

    def __init__(self):
        self._bids = SortedDict()
        self._asks = SortedDict()

    @staticmethod
    def _update(offers_dict, updates: Sequence[Offer]):
        for update in updates:
            if update.quantity == 0:
                try:
                    del offers_dict[update.price]
                except KeyError:
                    pass
            else:
                offers_dict[update.price] = update.quantity

    def update(self, bids: Sequence[Offer], asks: Sequence[Offer]):
        self._update(self._bids, bids)
        self._update(self._asks, asks)

    @property
    def top_bid(self):
        return Offer(*self._bids.peekitem(index=-1))

    @property
    def top_ask(self):
        return Offer(*self._asks.peekitem(index=0))


class DepthUpdateEvent():

    def __init__(self, event):
        self._event = event

    @property
    def first_update(self) -> int:
        return self._event['U']

    @property
    def last_update(self) -> int:
        return self._event['u']

    @property
    def bids(self) -> List[str]:
        return self._event['b']

    @property
    def asks(self) -> List[str]:
        return self._event['a']


async def fetch_order_book(symbol):
    http_uri = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=100'
    log.info("Getting order book: %r", http_uri)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as http_session:
        async with http_session.get(http_uri) as http_response:
            return await http_response.json()


async def watch_ws(websocket) -> Iterator[DepthUpdateEvent]:
    while True:
        recv = await websocket.recv()
        msg = json.loads(recv)

        msg_type = msg.get('e')
        if msg_type == "depthUpdate":
            yield DepthUpdateEvent(msg)
        else:
            log.debug('Message is not depthUpdate. Skipping message, type: %s', msg_type)


async def watch(symbol):
    ws_uri = f'wss://stream.binance.com:9443/ws/{symbol.lower()}@depth'
    log.info("Listening to: %r", ws_uri)
    async with connect(ws_uri, ping_interval=1, ping_timeout=1, close_timeout=1) as websocket:
        # Using build-in websocket message buffering to keep messages while we query for the order
        # book snapshot.

        order_book_snapshot = await fetch_order_book(symbol)
        yield OrderBookUpdate(bids=order_book_snapshot['bids'], asks=order_book_snapshot['asks'])

        order_book_update_id = order_book_snapshot['lastUpdateId']
        log.info("Got order book (update id %r)", order_book_update_id)

        prev_last_update = None
        async for ev in watch_ws(websocket):
            # The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
            # Drop any event where last_update is <= lastUpdateId in the snapshot.
            if ev.last_update <= order_book_update_id:
                log.info("Ignoring pre-snapshot event: last_update=%r", ev.last_update)
                continue

            # While listening to the stream, each new event's U should be equal to the previous
            # event's u+1.
            if prev_last_update is not None and ev.first_update != prev_last_update + 1:
                raise Exception(
                    f"Update continuity error: {ev.first_update=} != {prev_last_update=}")

            yield OrderBookUpdate(bids=ev.bids, asks=ev.asks)
            prev_last_update = ev.last_update


async def watch_and_print():
    while True:
        try:
            book = OrderBook()
            async for update in watch('BNBUSDT'):
                book.update(bids=update.bids, asks=update.asks)

                print(f"Top bid price {book.top_bid.price}, quantity {book.top_bid.quantity}")
                print(f"Top ask price {book.top_ask.price}, quantity {book.top_ask.quantity}")
        except (ConnectionClosedError, socket.error, asyncio.TimeoutError):
            # Start from scratch - this will re-request the order book snapshot, since the stream
            # has been lost.
            log.exception('Connection lost - retrying')
            await asyncio.sleep(5)


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
