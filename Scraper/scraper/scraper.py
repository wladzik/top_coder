import grequests
import requests
import re
import time
import json
from threading import Thread
from openpyxl import Workbook

init_time = time.time()
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                 "Chrome/81.0.4044.122 Safari/537.36"
headers = {'User-Agent': user_agent}
url_root = "https://store.hp.com"
url = "https://store.hp.com/us/en/Finder?storeId=10151&catalogId=10051&categoryId=88340&" \
      "isAjax=false&pageSize=50&beginIndex={arg}&orderBy=6"
prices = "https://store.hp.com/us/en/HPServices?langId=-1&storeId=10151&catalogId=10051&" \
         "=1588483268236&action=cupids&catentryId={arg}&modelId="

# locators based on regex for search page
total_amount_regex = r'totalProducts": ([0-9]*),'
printer_wrapper_locator = '<div class="productWrapper">'
long_id_regex = r'class="productCard" id="p_(\S*)">'
product_name_regex = r'class="productHdr".*>(.*)</a'
product_id_regex = r'div class="partNo".*>(\S*)</div'
url_regex = r'class="productHdr".*href="(\S*)"'

# locators based on regex for separate pages
name_regex = r'class="product-detail.*<h1>(.*)</h1'
name_regex_2 = r'class="puf-product-detail.*<h1>(.*)</h1'
id_from_page_regex = r'class="pdp-sku">(\S*)</span'
id_from_title_regex = r'\((\S*)\)</title'

# for logger
search_page = []
sep_pages = []
save_time = []

# threads and prepared data
threads = []
sheet_1 = []
sheet_2 = []


def starter():
    search_start = time.time()
    max_request = 50
    index = 50
    document, total_amount = first_request()
    threading(document)
    url_list = [url.format(arg=index) for index in range(index, int(total_amount), max_request)]
    results = grequests.map((grequests.get(u, headers=headers) for u in url_list), size=3)
    for resp in results:
        threading(resp.text)
    search_end = time.time()
    search_page.append(search_end-search_start)
    for i in threads:
        i.join()
    save_file(sheet_1, sheet_2)


def threading(doc):
    t = Thread(target=prepare_data_from_raw_response, args=[doc])
    t.start()
    threads.append(t)


def first_request():
    response = requests.get(url.format(arg=0), headers=headers)
    if response.status_code == 200:
        document = response.text
        total_amount = re.search(total_amount_regex, document).group(1)
        return document, total_amount
    else:
        with open("logs.log", "a") as log_file:
            log_file.write("\n!!! Error 403 !!!\n")
        raise Exception(str(response))


def prepare_data_from_raw_response(html):
    start_raw = time.time()
    html_list_of_products = html.split(printer_wrapper_locator)[1:]
    long_ids = [re.search(long_id_regex, i).group(1) for i in html_list_of_products]
    price = requests.get(prices.format(arg="%2C".join(long_ids)), headers=headers).text
    prices_json = json.loads(price)
    price_list = prices_json["priceData"]
    dictionary = {}
    for i in price_list:
        dictionary[i["productId"]] = i["lPrice"]
    price_list_sorted = [dictionary[j] for j in long_ids]
    currency = {"USD": "$"}[prices_json["storeData"]["currency"]]
    parsed_data = [[re.search(product_name_regex, i).group(1).split("&")[0],
                    re.search(product_id_regex, i).group(1),
                    currency + a,
                    url_root + re.search(url_regex, i).group(1)]
                   for i, a in zip(html_list_of_products, price_list_sorted)]
    sheet_1.extend(parsed_data)
    end_raw = time.time()
    get_separate_pages(parsed_data, price_list, currency)
    search_page.append(end_raw-start_raw)


def get_separate_pages(parsed_data, price_list, currency):
    start_sep = time.time()
    rs = (grequests.get(u[3], headers=headers, allow_redirects=False, timeout=7) for u in parsed_data)
    single_resp = grequests.map(rs, size=2, gtimeout=0.7)  # set gtimeout to avoid 403
    for i, k in zip(single_resp, price_list):
        if i.status_code in [200]:
            doc = i.text.split('<div class="pdp-right')
            try:
                name = re.findall(name_regex, doc[1])[0]
            except IndexError:
                name = re.findall(name_regex_2, doc[1])[0]
            try:
                pr_id = re.findall(id_from_page_regex, doc[1])[0]
            except IndexError:
                pr_id = re.findall(id_from_title_regex, doc[0])[0]
            pr_price = currency + k["lPrice"]
            pr_url = i.url
            sheet_2.append([name, pr_id, pr_price, pr_url])
        elif i.status_code == 403:
            print("Response :", i.status_code, "increase gtimeout if you get 403 response")
            with open("logs.log", "a") as log_file:
                log_file.write("\n!!! Error 403 !!!\n")
            raise Exception(str(i.status_code))

    stop_sep = time.time()
    sep_pages.append(stop_sep - start_sep)


column_headers = [["Product Name", "ID", "Price", "URL"]]


def insert_data_into_sheet(book, sheet_name, data):
    ws = book.create_sheet(title=sheet_name)
    for row in column_headers + data:
        ws.append([z for z in row])


def save_file(sheet1, sheet2):
    save_start = time.time()
    wb = Workbook(write_only=True)
    insert_data_into_sheet(wb, "Product listing", sheet1)
    insert_data_into_sheet(wb, "Products", sheet2)
    wb.save(filename="output_{arg}.xlsx".format(arg=time.time()))
    save_end = time.time()
    save_time.append(save_end - save_start)


print("Scraping started...")

starter()  # starts the process

end_time = time.time()
total_time = end_time - init_time
print("Preparing data from search page took: {arg1} s\nPreparing data from separate pages took: {arg2} s"
      "\nData saved: {arg3} s\nTask finished! Total time: {arg4} s\n".format(
        arg1=str(max(search_page)), arg2=str(max(sep_pages)), arg3=str(save_time[0]), arg4=str(total_time)))
with open("logs.log", "a") as file:
    file.write("\n\n Start: {arg}\n Preparing data from search page took: {arg1} s\n Preparing data from separate "
               "pages took: {arg2} s\n Data saved: {arg3} s\n Task finished! Total time: {arg4} s\n".format(
                arg=str(init_time), arg1=str(max(search_page)), arg2=str(max(sep_pages)), arg3=str(save_time[0]),
                arg4=str(total_time)))

if __name__ == "__main__":
    pass
