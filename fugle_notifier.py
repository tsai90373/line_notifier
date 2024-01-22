from fugle_marketdata import WebSocketClient
import json
import asyncio
import finlab
from finlab import data

# 載入 linetools 套件
import lineTool

line_token = 'mrkYS4guFl5CrgC6IKrT3pIGvNlsqZE7bUGeTL56RAh'
key = 'MjhiNWM2MjgtNmFlYy00NTc1LWJmOTEtNWUzMDNjZWJkNjA4IDM0ZTM0Y2I2LTQ4NGMtNDlhNS1iMDNhLWMxNzY1NmM1OWUwOA=='

finlab.login('6Xtx+++uDmYmnVF+rtrDhj+aZMXwMjrzxqDCgp3IRr/xwKeCLZN4J5uC/K6MqlTX#vip_m')
amount = data.get('price:成交金額')
market_amount = data.get('market_transaction_info:成交金額')

# 昨日成交金額前50名
stocks = amount.iloc[-1].sort_values(ascending=False).head(50)
stock_ids = stocks.index.tolist()

# 昨天市場總成交量的0.5%為進場門檻
market_amount = data.get('market_transaction_info:成交金額')
market_amount = market_amount['OTC'] + market_amount['TAIEX']
THRESHOLD = market_amount.iloc[-1] * 0.005

# 記錄內外盤（第一盤不要算）
cum_io = 0
is_open = {}
for stock_id in stock_ids:
    is_open[stock_id] = False

high = 0
low = 0
long = 0
short = 0
pos = None
high_reverse = 0
low_reverse = 0
long_enter = []
short_enter = []


def _on_new_price(message):
    global cum_io, high, low, long, short, pos, high_reverse, low_reverse, long_enter, short_enter

    json_data = json.loads(message)
    # print(json_data)
    # print(cum_io) 

    if json_data['event'] == "data":

        # 取最新成交價
        try:
            now_price = json_data['data']['price']
            symbol = json_data['data']['symbol']
            size = json_data['data']['size']
            bid = json_data['data']['bid']
            ask = json_data['data']['ask']

            try:  
                isTrial = json_data['data']['isTrial']
                if isTrial:
                    return

            except:
                # 如果是第一盤，不要算到內外盤裡面
                if is_open[symbol] == False:
                    is_open[symbol] = True
                    return 

                if now_price > (bid + ask) / 2:
                    cum_io += (size * 1000 * now_price)
                elif now_price < (bid + ask) / 2:
                    cum_io -= (size * 1000 * now_price)

                high = max(cum_io, high)
                low = min(cum_io, low)

                if pos == None:
                    low_reverse = low
                    high_reverse = high

                    # 做空狀況1: 從0往下 threshold 做空
                    if short - cum_io >= THRESHOLD:
                        pos = 'S'
                        short = cum_io
                        # short_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 第一筆空'
                        lineTool.lineNotify(line_token, msg) 
                        print('第一筆空')

                    # 做空狀況2: 高點回檔做空
                    if high - cum_io >= THRESHOLD:
                        pos = 'S'
                        short = cum_io
                        # short_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 第一筆空'
                        lineTool.lineNotify(line_token, msg)  
                        print('第一筆空')

                    # 做多狀況1: 從0往上 threshold 做多
                    if cum_io - long >= THRESHOLD:
                        pos = 'B'
                        long = cum_io
                        # long_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 第一筆多'
                        lineTool.lineNotify(line_token, msg) 
                        print('第一筆多')

                    # 做多狀況2: 低點往上做多
                    if cum_io - low >= THRESHOLD:
                        pos = 'B'
                        long = cum_io
                        # long_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 第一筆多'
                        lineTool.lineNotify(line_token, msg) 
                        print('第一筆多')

                elif pos == 'B':
                    # 加碼做多
                    if cum_io - long >= THRESHOLD:
                        long = cum_io
                        # long_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 加碼做多'
                        lineTool.lineNotify(line_token, msg)

                    # 反轉1: 比前一筆空單再往下 threshold
                    elif short - cum_io >= THRESHOLD:
                        pos = 'S'
                        short = cum_io
                        # short_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 多 -> 空: 比前一筆空單再往下 threshold'
                        lineTool.lineNotify(line_token, msg)

                        print('多 -> 空: 比前一筆空單再往下 threshold')

                    # 反轉2: 從高點往下做空
                    elif high - cum_io >= THRESHOLD and high_reverse != high:
                        pos = 'S'
                        short = cum_io
                        # short_enter.append(i)
                        high_reverse = high

                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 多 -> 空: 從高點往下'
                        lineTool.lineNotify(line_token, msg)
                        print('多 -> 空: 從高點往下')

                elif pos == 'S':
                    # 加碼做空
                    if short - cum_io >= THRESHOLD:
                        short = cum_io
                        # short_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 加碼做空'
                        lineTool.lineNotify(line_token, msg) 
                        print('加碼做空')

                    # 反轉1: 比前一筆多單再往上 threshold
                    elif cum_io - long >= THRESHOLD:
                        pos = 'B'
                        long = cum_io
                        # long_enter.append(i)
                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 空 -> 多: 比前一筆多單再往上 threshold'
                        lineTool.lineNotify(line_token, msg) 
                        print('空 -> 多: 比前一筆多單再往上 threshold')

                    # 反轉2: 從低點往上
                    elif cum_io - low >= THRESHOLD and low_reverse != low:
                        pos = 'B'
                        long = cum_io
                        # long_enter.append(i)
                        low_reverse = low

                        msg = '\n' + f'\n 現在累積內外盤金額 {cum_io}, 空 -> 多: 空 -> 多: 從低點往上'
                        lineTool.lineNotify(line_token, msg) 
                        print('空 -> 多: 從低點往上')

        except:
            print(json_data)
            return
     
async def main():
    client = WebSocketClient(api_key=key)
    stock = client.stock
    stock.on('message', _on_new_price)
    
    await stock.connect()
    
    stock.subscribe({
        'channel': 'trades',
        'symbols': stock_ids
    })

if __name__ == '__main__':
    asyncio.run(main()) # command line 等環境用這個
    # await main() # notebook 用這個！
