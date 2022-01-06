import time
from time import sleep
from Functions.scraping_tools import get_response
from Functions.file_handler import save_pickle, load_pickle
from Functions.telegrambot import telegram_bot_sendtext, telegram_bot_sendphoto_url, bot_chatID_private

COLLECTION  = 'clonex'
PICKLE_FILE = '../Data/clonex_last_sale.pickle'
SLEEP       = 10


def get_asset_list(limit=20, collection='', order_by='', order_direction='desc', owner=''):
    url = f"https://api.opensea.io/api/v1/assets?order_direction={order_direction}&limit={limit}"
    if collection:
        url += f"&collection={collection}"
    if order_by:
        url += f"&order_by={order_by}"
    if owner:
        url += f"&owner={owner}"

    res  = get_response(url)
    data = res.json()
    return data['assets']


def get_last_message(file=PICKLE_FILE):
    try:
        last_messages = load_pickle(file)
        if 'Error' in last_messages:
            last_messages = []
        return last_messages
    except:
        last_messages = []
        return last_messages


def get_last_sale():
    check_num = 10
    sale_list = get_asset_list(limit=check_num, collection=COLLECTION, order_by='sale_date')
    for item in reversed(sale_list):
        name      = item['name'].replace('#', '')
        timestamp = item['last_sale']['event_timestamp'].replace('T', ' ')
        os_link   = item['permalink']
        price     = float(item['last_sale']['total_price'])/1000000000000000000
        message   = f"{timestamp} - *{name}*\nSold for: *{price} ETH*\nView on [OpenSea]({os_link})"
        message  += '\n\n-----\nIf you have any issues or feedback, feel free to [contact me](tg://user?id=383615621) :)'
        message  += '\nCheck out my other [Telegram-Bots](https://linktr.ee/v1et4nh)'

        last_messages = get_last_message()
        if len(last_messages) > check_num + 5:
            last_messages.pop(0)
        if message not in last_messages:
            telegram_bot_sendtext(message, bot_chatID='-1001661074217')
            last_messages.append(message)
            save_pickle(last_messages, PICKLE_FILE)


def main(time_intervall=SLEEP):
    while True:
        try:
            print(time.strftime('%X %x %Z'))
            get_last_sale()
            sleep(time_intervall)
        except:
            print('Restart...')
            sleep(time_intervall)


if __name__ == '__main__':
    main()
