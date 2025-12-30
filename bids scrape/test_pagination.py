import requests
import json

url = "https://bidplus.gem.gov.in/all-bids-data"
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "origin": "https://bidplus.gem.gov.in",
    "referer": "https://bidplus.gem.gov.in/all-bids"
}

def get_page(start):
    payload_dict = {
        "param": {"searchBid": "", "searchType": "fullText", "start": start},
        "filter": {
            "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
            "byEndDate": {"from": "", "to": ""},
            "sort": "Bid-End-Date-Latest", "byStatus": ""
        }
    }
    data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": "8cc1b7dd8d2b7a3bf24c8d202e66efba"}
    resp = requests.post(url, headers=headers, data=data)
    j = resp.json()
    return [d['id'] for d in j['response']['response']['docs']]

ids_0 = get_page(0)
print(f"Page 0 IDs: {ids_0}")

ids_10 = get_page(10)
print(f"Page 10 IDs: {ids_10}")

intersection = set(ids_0).intersection(ids_10)
print(f"Intersection: {intersection}")
