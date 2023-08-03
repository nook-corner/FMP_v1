# Import necessary modules
import requests
import random
import config

def test_api_connection(api_url):
    try:
        response = requests.get(api_url)

        # Check if the response status code is 200 (OK)
        if response.status_code == 200:
            print("Connection successful. API is accessible.")
        else:
            print(f"Connection failed. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Connection failed. Error: {e}")

# Get the API key from config
api_key = config.api_key


# Function to fetch the list of companies from the API
def fetch_list_of_companies():
    url_list_company = f'https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey={api_key}'
    try:
        re_list_company = requests.get(url_list_company)
        list_company = re_list_company.json()
        return list_company
    except Exception as e:
        print(f"Error fetching the list of companies: {str(e)}")
        return []

# Function to test fetching the list of companies
def test_fetch_list_of_companies():
    list_company = fetch_list_of_companies()
    return len(list_company) > 0

# Function to fetch dividend data for randomly selected companies
def fetch_random_data(num_companies=10):
    list_company = fetch_list_of_companies()
    random_symbols = random.sample(list_company, num_companies)
    return random_symbols

# Function to test fetching random dividend data
def test_fetch_random_data():
    random_symbols = fetch_random_data()
    return len(random_symbols) > 0

# Main function to run all the tests
def main():
    # Test the API connection
    api_key = config.api_key
    url_list_company = f'https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey={api_key}'
    test_api_connection(url_list_company)

    # Test fetching the list of companies
    print("Testing fetch_list_of_companies...")
    if test_fetch_list_of_companies():
        print("Fetch list of companies test: Success")
    else:
        print("Fetch list of companies test: Error")

    # Test fetching random dividend data
    print("Testing fetch_random_data...")
    if test_fetch_random_data():
        print("Fetch random data test: Success")
    else:
        print("Fetch random data test: Error")

if __name__ == "__main__":
    main()
