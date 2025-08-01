import websocket
import json
import pandas as pd
from datetime import datetime,timezone


class BinanceTradeStreamer:
    def __init__(self, symbol='btcusdt'):
        self.socket_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        self.data_per_sec = {}
        self.df = pd.DataFrame({
            'timestamp': pd.Series(dtype='str'),
            'buy_qty': pd.Series(dtype='float'),
            'sell_qty': pd.Series(dtype='float'),
            'buy_pct_change': pd.Series(dtype='float'),
            'sell_pct_change': pd.Series(dtype='float'),
            'price_pct_change': pd.Series(dtype='float')
        })
        self.prev_second = None
        self.prev_buy_qty = 0
        self.prev_sell_qty = 0
        self.prev_price = 0
        self.start_time = None
        self.duration = 120  # seconds

    def on_message(self, ws, message):
        current_time = datetime.now(timezone.utc).timestamp()
        if current_time - self.start_time > self.duration:
            print("10 seconds reached. Closing connection.")
            ws.close()
            return
        trade = json.loads(message)
        self._process_trade(trade)

    def _process_trade(self, trade):
        ts = trade['T']
        second = int(ts / (1000*60))
        qty = float(trade['q'])
        price = float(trade['p'])
        is_maker = trade['m']  # True = SELL, False = BUY

        if second != self.prev_second and self.prev_second is not None:
            self._record_second_data()

        if second not in self.data_per_sec:
            self.data_per_sec[second] = {'buy_qty': 0, 'sell_qty': 0, 'price': price}

        if is_maker:
            self.data_per_sec[second]['sell_qty'] += qty
        else:
            self.data_per_sec[second]['buy_qty'] += qty

        self.data_per_sec[second]['price'] = price
        self.prev_second = second

    def _record_second_data(self):
        curr = self.data_per_sec.get(self.prev_second, {})
        buy_qty = curr.get('buy_qty', 0)
        sell_qty = curr.get('sell_qty', 0)
        price = curr.get('price', self.prev_price)

        buy_change = ((buy_qty - self.prev_buy_qty) / self.prev_buy_qty * 100) if self.prev_buy_qty > 0 else 0
        sell_change = ((sell_qty - self.prev_sell_qty) / self.prev_sell_qty * 100) if self.prev_sell_qty > 0 else 0
        price_change = ((price - self.prev_price) / self.prev_price * 100) if self.prev_price > 0 else 0

        new_row = pd.DataFrame([{
            'timestamp': datetime.fromtimestamp(self.prev_second).strftime('%Y-%m-%d %H:%M:%S'),
            'buy_qty': buy_qty,
            'sell_qty': sell_qty,
            'buy_pct_change': round(buy_change, 2),
            'sell_pct_change': round(sell_change, 2),
            'price_pct_change': round(price_change, 2)
        }])
        self.df = pd.concat([self.df, new_row], ignore_index=True)


        self.prev_buy_qty = buy_qty
        self.prev_sell_qty = sell_qty
        self.prev_price = price

    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Connection closed. Code: {close_status_code}, Message: {close_msg}")
        filename = "btc_data.csv"
        self.df.to_csv(filename, index=False)
        print(f"Saved data to {filename}")


    def on_open(self, ws):
        print("Connection opened.")
        self.start_time = datetime.now(timezone.utc).timestamp()


    def run(self):
        ws = websocket.WebSocketApp(
            self.socket_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        ws.run_forever()

# Run it
if __name__ == "__main__":
    streamer = BinanceTradeStreamer('btcusdt')
    streamer.run()
