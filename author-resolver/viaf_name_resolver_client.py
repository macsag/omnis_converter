import json
import requests

r = requests.post('http://192.168.40.99:5000/omnis/personal/viaf/by_fields',
                  json={"full_name": "Gombrowicz, Witold",
                        "titles": ["Ferdydurke", "Kosmos"],
                        "params": "only"})

print(r.status_code)
print(json.dumps(r.json(), indent=2, ensure_ascii=False))