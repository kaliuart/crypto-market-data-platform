import websocket
import json

url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

def on_message(we, message):
    data = json.loads(message)

    print(data)

ws = websocket.WebSocketApp(url, on_message=on_message)
ws.run_forever()
