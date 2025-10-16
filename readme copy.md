This file explains the format of the csv files you should have received alongside it, **orderbooks-1000.csv**, **orderbooks-10.csv**, **trades.csv** and **refs.csv**. It also contains some additional information related to the case study.

### orderbooks-1000.csv and orderbooks-10.csv format
The file is a standard csv file, using ',' as the separator and '\n' as the line terminator. It contains a header row with the column names. Each row after the first headear contains a timestamp in seconds, then a short string for symbol name, then a string containing a python list each for both the bids column and the asks column. Each element of this list is another element, containing precisely two elements. Each two-element list represents an update to a different price level in the orderbook. The first element is the price, and the second element is the updated total amount of orders at that price.

The data contains orderbook updates for 8 different symbols. The first message for each symbol is the complete orderbook snapshot of up to depth 1000 and 10, each message after that for that symbol is an orderbook update. Refer to the "Case Studies" and "Definitions and Glossary" documents for more information.

### trades.csv format
The file is a standard csv file, using ',' as the separator and '\n' as the line terminator. It contains a header row with the column names. Fields are the symbol the trade, the side of the taker of that trade (see the definitions and glossary document for what maker/taker is), the trade price and amount, the time trade has happened, the reference price for bid and ask sides (see the definitions and glossary document for what the reference price is) and the exchange value for USDT/USD.

### refs.csv format
symbol,time,bid_price,ask_price
The file is a standard csv file, using ',' as the separator and '\n' as the line terminator. It contains a header row with the column names. Fields are the bid and ask reference price for the given symbol and the time that reference price update was received at. See the definitions and glossary document for the definition of the reference price.
