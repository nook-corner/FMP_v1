import requests
import random
import config

class FinancialModelingPrepConnection:
    
    
    def __init__(self):
        """
        Initialize the connection to the FinancialModelingPrep API with the API key.

        Args:
            api_key (str): The API key to access the financialmodelingprep API.
        """
        self.api_key = config.api_key
        self.url_list_company = 'https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey=' + self.api_key

    def fetch_list_of_companies(self):
        """
        Fetch the list of companies from the FinancialModelingPrep API.

        Returns:
            list: A list of companies' data.
        """
        try:
            re_list_company = requests.get(self.url_list_company)
            list_company = re_list_company.json()
            return list_company
        except Exception as e:
            print(f"Error fetching the list of companies: {str(e)}")
            return []
            
            
    def fetch_random_data(self , num_companies=15):
        """
        Fetch dividend data for randomly selected companies.

        Args:
            num_companies (int): Number of companies to fetch dividend data for (default is 15).

        Returns:
            list: A list of tuples containing the processed dividend data.
        """
        try:
            list_company = self.fetch_list_of_companies()

            #Random symbol because API have limit per day
            random_symbols = random.sample(list_company, num_companies)
            
            
            return random_symbols
        except Exception as e:
            print(f"Error fetching dividend data: {str(e)}")
            return []          
   
        
    def fetch_dividend_data(self, symbol):
        """
        Fetch dividend data for a specific company symbol from the FinancialModelingPrep API.

        Args:
            symbol (str): The company symbol for which to fetch dividend data.

        Returns:
            dict: A dictionary containing the dividend data for the company.
        """
        self.api_key = config.api_key
        #url = f'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{symbol}?apikey={self.api_key}'
        url = f'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{symbol}?apikey='  + self.api_key
        try:
            re_dividend_data = requests.get(url)
            return re_dividend_data.json()
        except Exception as e:
            print(f"Error fetching dividend data for {symbol}: {str(e)}")
            return {}
            



