import requests 


response = requests.get("https://quotes.toscrape.com/")
print(response.status_code)
print(response.content)

