# datawarehouse_project
In this repository, you will find a data-warehouse project that combines Python, MySQL and GoogleSheets API tools. The objective of this project was to retrieve a spreadsheet's data that was in wide format, transform it into long format, give it structure and then upload it to another spreadsheet.

As you will see, there are four files. ausentismo_base.py and ausentismo.py are the main scripts. The first one takes the needed information and creates all the dim tables and the facts table. the other one, keeps the database updated, uploading the new values only.

On the other hand, there are two auxiliary files dim_tables_base.py that contains the needed code to create the dim tables based in the particular characteristics of the information. The second file, exec_queries.py, stores the queries that will be used along the process. In order to make the code more readable these parts of code were cut and these functions were created. 
