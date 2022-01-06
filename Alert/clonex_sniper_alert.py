import requests
import time
import lxml.html as lh
import pandas as pd
from time import sleep
from Functions.file_handler import save_pickle, load_pickle
from Functions.telegrambot import telegram_bot_sendtext, bot_chatID_private


NAME        = 'CloneX'
OPENSEA     = 'clonex'
SLEEP       = 60
PRICE_ALARM = 1000000  # ETH
PICKLE_FILE = '../Data/clonex_sniper.pickle'


def getSniperStats(limit=7):
    url = 'https://nft.wuestenigel.com/sniper/'
    if limit > 0:
        url += '&limit=' + str(limit)
    res = requests.get(url)
    if res.status_code != 200:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code != 200:
            telegram_bot_sendtext('Error: Cannot get data from wuestenigel', bot_chatID=bot_chatID_private)
            return 'RequestsError'
    doc = lh.fromstring(res.content)
    tr_elements = doc.xpath('//tr')

    # Get Header
    col = []
    for t in tr_elements[0]:
        name_col = t.text_content()
        col.append((name_col, []))

    # Get Table
    for idx, (thumb, price, score, rank, url, date) in enumerate(tr_elements):
        if idx == 0:
            continue
        thumb = ''
        price = round(float(price.text_content()), 2)
        score = int(score.text_content())
        rank  = int(rank.text_content().split()[0])
        url   = url.getchildren()[0].attrib['href']
        date  = date.text_content()

        if price < 5 or score > 500:
            col[0][1].append(thumb)
            col[1][1].append(price)
            col[2][1].append(score)
            col[3][1].append(rank)
            col[4][1].append(url)
            col[5][1].append(date)

    tmp_dict = {title:column for (title, column) in col}
    df = pd.DataFrame(tmp_dict)
    df = df.drop(columns=['Thumb'])
    df = df.drop(columns=['Rang'])
    df = df.sort_values(['Preis', 'Score'])
    stats = getOSstats()
    floor_price = float(stats['floor_price'])
    print(f"{NAME}: {floor_price}")
    last_df = get_last_message()
    if not df.equals(last_df):
        stats = getOSstats()
        floor_price = float(stats['floor_price'])
        message = f"*Floor Price: {format(floor_price, '.2f')}*\n"
        message += '\nURL | Price | Score'
        for row in df.itertuples():
            message += f"\n[link]({row.Traits})  |  {format(row.Preis, '.2f')}  |  {row.Score}"
        telegram_bot_sendtext(message, bot_chatID='-1001766067718', disable_web_page_preview=True)
        save_pickle(df, PICKLE_FILE)


def get_last_message():
    dict_last_messages = load_pickle(PICKLE_FILE)
    try:
        return dict_last_messages
    except:
        return pd.DataFrame({'A': []})


def getData(url):
    res = requests.get(url)
    if res.status_code != 200:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code != 200:
            return 'RequestsError'
    data = res.json()
    return data


def getETHprice():
    url_eur = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=eur%2Cbtc&include_market_cap=true&include_24hr_change=true"
    url_usd = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd%2Cbtc&include_market_cap=true&include_24hr_change=true"
    data_eur = requests.get(url_eur).json()
    peur = round(data_eur["ethereum"]["eur"], 2)
    peur = format(peur, ",")
    peur_val = float(peur.replace(',', ''))
    data = requests.get(url_usd).json()
    pusd = round(data["ethereum"]["usd"], 2)
    pusd = format(pusd, ",")
    pusd_val = float(pusd.replace(',', ''))

    return peur_val, pusd_val


def getOSstats(collection=OPENSEA):
    url = "https://api.opensea.io/api/v1/collection/" + collection
    data = getData(url)
    stats = data['collection']['stats']

    return stats


def run_os_stats():
    stats = getOSstats()
    last_floor = get_last_message()
    floor_price = float(stats['floor_price'])
    message  = NAME + ': ' + str(floor_price)
    print(message)
    if floor_price < PRICE_ALARM and abs(floor_price - last_floor) > 0:
        eur, usd, = getETHprice()
        eur_price = int(eur * floor_price)
        usd_price = int(usd * floor_price)
        url       = 'https://opensea.io/collection/' + OPENSEA
        message   = '\n\nFloor Price: *' + str(stats['floor_price']) + ' ETH* (*' + str(eur_price) + ' EUR* | *' + str(usd_price) + ' USD*)'
        message  += '\nVolume traded: *' + str(int(stats['total_volume'])) + ' ETH*'
        message  += '\nHolders: *' + str(stats['num_owners']) + '*'
        message  += '\n\nView in [Opensea](' + url + ')'
        message  += '\n\n-----\nIf you have any issues or feedback, feel free to [contact me](tg://user?id=383615621) :)'
        message  += '\nCheck out my other [Telegram-Bots](https://linktr.ee/v1et4nh)'
        telegram_bot_sendtext(message, bot_chatID='-1001566937584', disable_web_page_preview=True)
        dict_floor = {'floor': floor_price}
        save_pickle(dict_floor, PICKLE_FILE)


def main(time_intervall=SLEEP):
    while True:
        try:
            print(time.strftime('%X %x %Z'))
            getSniperStats()
            sleep(time_intervall)
        except:
            print('Restart...')
            sleep(time_intervall)


if __name__ == '__main__':
    main()
