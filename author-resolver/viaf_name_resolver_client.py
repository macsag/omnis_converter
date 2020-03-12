import requests

r = requests.post('http://192.168.40.99:5000/omnis/personal/viaf/by_fields',
                  json={"full_name": "Rutkowska, Agnieszka",
                        "titles": ["Pedagog tańca w świetle aktów prawnych"],
                        "params": "best_score"})

print(r.status_code)
print(r.json())