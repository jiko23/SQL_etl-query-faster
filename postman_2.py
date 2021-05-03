import pandas as pd
import pyodbc



class Table_Queries:


	def __init__(self, conn, cursor):


		self.data = 1

		self._index = {}

		self.conn = conn

		self._cursor = cursor

 



	'''
		Here table will be created and bulk data can be inserted into the table with ease.
	'''
	def table_creation(self, data):


		try:

			self._cursor.execute('DROP TABLE JAYANTA.dbo.Products')


			self._cursor.execute(''' CREATE TABLE Products(name nvarchar(50), sku varchar(255) NOT NULL, description TEXT)''')


		except:

			self._cursor.execute(''' CREATE TABLE Products(name nvarchar(50), sku varchar(255) NOT NULL, description TEXT)''')


		query = "BULK INSERT " + 'JAYANTA.dbo.Products' + " FROM '" + data + "' WITH (FORMAT = 'CSV',FIRSTROW = 2)"

		success = self._cursor.execute(query)

		self.data = pd.read_sql('SELECT * FROM JAYANTA.dbo.Products', con = self.conn)


		self.conn.commit()


		return "Table created with data"



	'''
		To get instant access to the index to perform update.
	'''
	def _index_dict(self):


		for i in self.data[0:20].sku.unique():

			_index_list = self.data[0:20].index[self.data[0:20]["sku"] == i].tolist()

			self._index[i] = _index_list

			#print(self._index)

		print("Index Ready")

		return self._index



	'''
		To perform aggregation and create a table with aggregated data.
	'''
	
	def aggregate_query(self, data_link):

		
		query = ''' SELECT COUNT(sku) as Product_count, name From Products GROUP BY name;'''

		data_ = pd.read_sql(query, self.conn).to_csv(input("Enter the aggregated file name to be saved:"), index = False)


		try:

			self._cursor.execute('DROP TABLE JAYANTA.dbo.Aggregated_Products')


			self._cursor.execute(''' CREATE TABLE Aggregated_Products(Product_count int, name nvarchar(50))''')


		except:

			self._cursor.execute(''' CREATE TABLE Aggregated_Products(Product_count int, name nvarchar(50))''')


		self._cursor.execute("BULK INSERT " + 'JAYANTA.dbo.Aggregated_Products' + " FROM '" + data_link + "' WITH (FORMAT = 'CSV', FIRSTROW = 2)")

		self.conn.commit()


		return "Aggregated Table Created!!!"




	'''
		Sql table update.
	'''
	def update_query(self, data_link, aggregated_link):


		for k, v in self._index.items():

			print(k,v)

			if len(v) >= 1 :


				for i in v :

					print(self.data.iloc[i])

					choice = input("Do you want to update yes/no:")

					if choice == 'yes':

						_column_name = input("Kindly type the column you want to be updated at a certain index: ")

						_value = input("Kindly enter the value to be set: ")

						self.data.loc[i, _column_name] = _value


					elif choice == 'no':

						continue


					else:

						self._cursor.execute('DROP TABLE JAYANTA.dbo.Products')

						self.conn.commit()

						#self._cursor.close()

						_data = self.data.to_csv(input("Enter the file name to be saved:"), index = False)

						self.table_creation(data_link)

						self.aggregate_query(aggregated_link)

						#self._cursor.close()

						print("Success2!!!")

						exit()

			else:

				continue


		self._cursor.execute('DROP TABLE JAYANTA.dbo.Products')

		_data = self.data.to_csv(input("Enter the file name to be saved:"), index = False)

		self.table_creation(data_link)

		self.aggregate_query(aggregated_link)

		self._cursor.close()

		return 'success!!!'




if __name__ == '__main__':

	data_link = 'E:\prog\products.csv' #Original data to be send to file

	updated_data_link = 'E:\prog\products1.csv' #updated data

	aggregated_data_link = 'E:\prog\products2.csv' #aggregated data

	conn = pyodbc.connect('Driver={SQL Server};' 'Server=WINDOWS-ASIROT3\SQLEXPRESS;' 'Database=JAYANTA;' 'Trusted_Connection=yes;')

	cursor = conn.cursor()


	_call = Table_Queries(conn, cursor)



	print(_call.table_creation(data_link))

	print(_call._index_dict())

	print(_call.update_query(updated_data_link, aggregated_data_link))
			
	#print(_call.aggregate_query())
		
		