from connection import FinancialModelingPrepConnection
connection = FinancialModelingPrepConnection()

class DividendDataProcessor:

    def fetch_random_dividend_data(self):
        """
        Fetch dividend data for randomly selected companies.

        Args:
            num_companies (int): Number of companies to fetch dividend data for (default is 15).

        Returns:
            list: A list of tuples containing the processed dividend data.
        """
        try:
            random_symbols = connection.fetch_random_data()
            dividend_data = []
            for symbol in random_symbols: 
                company_re_dividend = connection.fetch_dividend_data(symbol)
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
