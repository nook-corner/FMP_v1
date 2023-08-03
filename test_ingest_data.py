#Test ingest data
#from connection import FinancialModelingPrepConnection
import config
import requests
#connection = FinancialModelingPrepConnection()

class DividendDataProcessor:
    def fetch_fixed_dividend_data(self):
        # Get the API key from config
        api_key = config.api_key

        """
        Fetch dividend data for specific company symbols.

        Returns:
            list: A list of tuples containing the processed dividend data.
        """
        try:
            fixed_symbols = ["AAPL", "CFVI"]
            #fixed_symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "FB" , "CFVI"]
            dividend_data = []

            for symbol in fixed_symbols:
                company_re_dividend = f'https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{symbol}?apikey='  + api_key
                re_dividend = requests.get(company_re_dividend)
                company_re_dividend = re_dividend.json()
                symbol_company = company_re_dividend.get('symbol')
                historical_company = company_re_dividend.get('historical')

                if historical_company is not None:
                    if len(historical_company) > 0:
                        for value in historical_company:
                            date = value['date']
                            label = value['label']
                            adj_dividend = value['adjDividend']
                            dividend = value['dividend']
                            record_date = value['recordDate']
                            payment_date = value['paymentDate']
                            declaration_date = value['declarationDate']

                            new_row = (symbol_company, date, label, adj_dividend, dividend,
                                       record_date, payment_date, declaration_date, '-')
                            dividend_data.append(new_row)
                    else:
                        new_row = (symbol_company, '-', '-', '-', '-', '-', '-', '-', 'No Data')
                        dividend_data.append(new_row)
                else:
                    new_row = (symbol, '-', '-', '-', '-', '-', '-', '-', 'Can not load data')
                    dividend_data.append(new_row)

            return dividend_data
        except Exception as e:
            print(f"Error fetching dividend data: {str(e)}")
            return []
