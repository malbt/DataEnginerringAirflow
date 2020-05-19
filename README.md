# DataEnginerringAirflow
![description_if_image_fails_to_load](Dags/airflowpipe.jpg.png)

SEC data source : https://www.sec.gov/cgi-bin/browse-edgar?CIK=tsla&owner=exclude&action=getcompany <br>
Stock price data source : yahoo finance <br>

**SEC filings and stock price**<br>
Example: Tesla stock "TSLA"<br>

The idea of stock trading is to capitalize on short-term market to sell stocks for profit, or buy at a low.<br>
There is day trading i.e buy and sell several times throughout the day; and there is active trading one or more <br>
trades per month.<br>

Traders do extensive research, spending hours following the market, technical analysis, trends etc. Investors can <br>
dig deep and discover why results ended up the way they did -- good or bad. But it's hard to get reliable source to <br>
find out the 'real story' behind what's going on with a company or moves that it is making.<br>

What are **SEC** Fillings?<br>
SEC Filings are formal documents that are submitted to the U.S. Securities and Exchange Commission (SEC). All public <br>
companies are required to make regular SEC filings. Financial professionals and investors rely on SEC filings for investment <br>
purposes. SEC Filings contain important financial data, disclosures, and events that impact the company.
Because the data contained within SEC filings is sworn to accuracy or audited, it is a much more reliable source. But at the same time <br>
unless they are a professional traders/investors most people don't really know about sec fillings and important phrases that <br>
are needed to know to recognize red flags.<br>

Based on my interest i created an **airflow pipeline**:<br>
I picked one ticker(**TSLA/Tesla**) and took out 10 years stock price from yahoo finance and it's SEC filling from EDGAR,<br>
cleaned and stored the data into postgres database, merged the tables in to one and then created tables for each year, <br>
created a dataframe and a candlestick chart so that they can see when and where the fillings occurred how it impacted <br>
the price and how long it stayed. 

