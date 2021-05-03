# Postman_Assignment
Assignment for Postman

Concept :
To complete the assignment the concept designed is a bit different. The following steps has been performed:

1> The first thing is to insert large data into sql table within very less time. In the program the function 'table_creation(data)' does the work. In the function it will first check if the table is already present or not. If already present then it will drop the table and then create the 'Product' table in sql database. This code has the capability to put a huge amount of data i.e. .csv file into the database table too fast.

2> The second thing to do is to get the index of data because queries like update takes the most time in searching the index into the table and then to perform updation.
   So, my concept was to get the data from the sql table and store it into a dataframe. In the programe the function '_index_dict' will find the indices of the data according to      primary key as specified in assignment i.e. sku. The foormat to store will be _index_list = {'sku data': [indices]} . This takes a bit time and as per assignment this section's    time complexity shouldnot be considered.

3> Third thing is to 
   
