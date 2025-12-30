import requests
import json

headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "origin": "https://bidplus.gem.gov.in",
        "referer": "https://bidplus.gem.gov.in/all-bids",
        "cookie": "csrf_gem_cookie=e331d254a1625325352a675d7eff471e; ci_session=e72373b6e945b9800026970b0837c53951942c57; TS0174a79d=01e393167d4a963be55d4c4af088a2d434fbd183521b8287cee50260d2dd642cd6618da0b8c76ab225e315b68566bd19fe820be4947b8f7ac675ef7912a451446b2133190d32eafe14ead8175e1d7dec7eeeff542e9388afc22660f2eec96b049cd2932333; GeM=1474969956.20480.0000; _ga=GA1.3.484596475.1761793171; _gid=GA1.3.2012138776.1767016991"
    }

url = "https://bidplus.gem.gov.in/all-bids-data"

def get_page(page_num):
    payload_dict = {
        "page": page_num,
        "param": {"searchBid": "", "searchType": "fullText"},
        "filter": {
            "bidStatusType": "bidrastatus", "byType": "all", "highBidValue": "",
            "byEndDate": {"from": "", "to": ""},
            "sort": "Bid-End-Date-Latest", "byStatus": ""
        }
    }
    data = {"payload": json.dumps(payload_dict), "csrf_bd_gem_nk": "e331d254a1625325352a675d7eff471e"}
    try:
        resp = requests.post(url, headers=headers, data=data) 
        j = resp.json()
        if "response" in j and "response" in j["response"] and "docs" in j["response"]["response"]:
            return [d['id'] for d in j['response']['response']['docs']]
        else:
            return f"Error/Empty: {j}"
    except Exception as e:
        return f"Exception: {e}"

print("Page 1:", get_page(1))
print("Page 2:", get_page(2))
print("Page 3:", get_page(3))
print("Page 4:", get_page(4))
