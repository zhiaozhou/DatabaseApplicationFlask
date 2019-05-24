import pandas as pd
import numpy as np
import os
import pymysql
import datetime as dt


def createFolder(dir):
    """ Create a folder with directory assigned as dir

    :param:
        dir (String): directory
    :return:
        None.
    """

    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except OSError:
        print('Error: Creating directory: {}'.format(dir))


def tables_preparation(input_dir=r'C:\Users\chu\Downloads\DB6400_Project_Phase_3\From_Phase2\Sample_Data',
                       input_excel_file=r'Sample Data.xlsx', output_excel_filename='Sample_data_cleaned.xlsx',
                       verbose=False):

    file = pd.ExcelFile(os.path.join(input_dir, input_excel_file))
    try:
        df_stores = file.parse('Stores')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Stores'))
        return
    try:
        df_products = file.parse('Products')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Products'))
        return
    try:
        df_saleprices = file.parse('Sale Prices')   # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Sale Prices'))
        return
    try:
        df_sales = file.parse('Sales')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Sales'))
        return
    try:
        df_holidays = file.parse('Holidays')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Holidays'))
        return

    output_dir = os.path.join(input_dir, 'output')
    createFolder(output_dir)
    writer = pd.ExcelWriter(os.path.join(output_dir, output_excel_filename), engine='xlsxwriter', datetime_format='yyyy-mm-dd', date_format='yyyy-mm-dd')

    table_City = df_stores[['name', 'state', 'population']]
    table_City = table_City.rename(columns={'name': 'CityName', 'state': 'State', 'population': 'population'})
    table_City = table_City.dropna()
    table_City.drop_duplicates(subset=['CityName', 'State'], inplace=True)
    df_CityState = pd.DataFrame({'CityState': table_City['CityName'] + ', ' + table_City['State']})
    table_City = pd.concat([df_CityState, table_City], axis=1)
    city_size_list = []
    for i in range(table_City.shape[0]):
        population_temp = int(table_City['population'].iloc[i])
        if population_temp < 3700000:
            city_size_list.append('Small')
        elif 3700000 <= population_temp < 6700000:
            city_size_list.append('Medium')
        elif 6700000 <= population_temp < 9000000:
            city_size_list.append('Large')
        elif population_temp >= 9000000:
            city_size_list.append('Extra Large')
    city_size_df = pd.DataFrame({'city_size': city_size_list}, index=table_City.index)
    table_City = pd.concat([table_City, city_size_df], axis=1)
    table_City = table_City[table_City['population'] > 0]
    table_City.to_excel(writer, sheet_name='City', index=False)

    table_Store = df_stores[['storeid', 'phone', 'address']]
    table_Store = table_Store.rename(columns={'storeid': 'StoreNumber', 'phone': 'phone_number', 'address': 'street_address'})
    store_citystate = pd.DataFrame({'CityState': df_stores['name'] + ', ' + df_stores['state']})
    table_Store = pd.concat([table_Store, store_citystate], axis=1)
    table_Store = table_Store.dropna()
    table_Store.drop_duplicates(subset=['StoreNumber'], inplace=True)
    table_Store.to_excel(writer, sheet_name='Store', index=False)

    '''
    table_LocatedIn = df_stores[['storeid', 'name', 'state']]
    table_LocatedIn = table_LocatedIn.rename(columns={'storeid': 'StoreNumber', 'name': 'CityName', 'state': 'State'})
    table_LocatedIn = table_LocatedIn.dropna()
    table_LocatedIn.drop_duplicates(subset=['StoreNumber', 'CityName', 'State'], inplace=True)
    table_LocatedIn.to_excel(writer, sheet_name='LocatedIn', index=False)
    '''

    table_Manufacturer = df_products[['manufacturer', 'maxdiscount']]
    table_Manufacturer = table_Manufacturer.rename(columns={'manufacturer': 'ManufacturerName', 'maxdiscount': 'max_discount'})
    table_Manufacturer = table_Manufacturer.dropna(subset=['ManufacturerName'])
    table_Manufacturer.drop_duplicates(subset=['ManufacturerName'], inplace=True)
    for i in range(table_Manufacturer.shape[0]):
        if table_Manufacturer['max_discount'].iloc[i] < 0.0 or table_Manufacturer['max_discount'].iloc[i] > 0.9:
            table_Manufacturer['max_discount'].iloc[i] = np.nan
    table_Manufacturer = table_Manufacturer.fillna(value=0.90)  # A a general rule, non-specified max_discount is defaulted to 0.90
    table_Manufacturer.to_excel(writer, sheet_name='Manufacturer', index=False)

    table_Product = df_products[['productid', 'name', 'retailprice', 'manufacturer']]
    table_Product = table_Product.rename(columns={'productid': 'PID', 'name': 'product_name',
                                                  'retailprice': 'retail_price', 'manufacturer': 'ManufacturerName'})
    table_Product = table_Product.dropna()
    table_Product.drop_duplicates(subset=['PID'], inplace=True)
    table_Product = table_Product[table_Product['retail_price'] >= 0.0]
    table_Product.to_excel(writer, sheet_name='Product', index=False)

    '''
    table_Produces = df_products[['manufacturer', 'productid']]
    table_Produces = table_Produces.rename(columns={'manufacturer': 'ManufacturerName', 'productid': 'PID'})
    table_Produces = table_Produces.dropna()
    table_Produces.drop_duplicates(subset=['ManufacturerName', 'PID'], inplace=True)
    table_Produces.to_excel(writer, sheet_name='Produces', index=False)
    '''

    table_Category = df_products[['categoryname']]
    table_Category = table_Category.rename(columns={'categoryname': 'CategoryName'})
    table_Category = table_Category.dropna()
    table_Category.drop_duplicates(inplace=True)
    table_Category.to_excel(writer, sheet_name='Category', index=False)

    table_BelongTo = df_products[['productid', 'categoryname']]
    table_BelongTo = table_BelongTo.rename(columns={'productid': 'PID', 'categoryname': 'CategoryName'})
    table_BelongTo = table_BelongTo.dropna()
    table_BelongTo.drop_duplicates(subset=['PID', 'CategoryName'], inplace=True)
    table_BelongTo.to_excel(writer, sheet_name='BelongTo', index=False)

    table_Cdate = df_sales[['date']]
    table_Cdate = table_Cdate.rename(columns={'date': 'CalendarDate'})
    table_Cdate = table_Cdate.dropna()
    table_Cdate.drop_duplicates(inplace=True)
    table_Cdate.to_excel(writer, sheet_name='Cdate', index=False)

    table_Holiday = df_holidays[['date', 'holidayname']]
    table_Holiday = table_Holiday.rename(columns={'date': 'CalendarDate', 'holidayname': 'HolidayName'})
    table_Holiday = table_Holiday.dropna()
    table_Holiday.drop_duplicates(subset=['CalendarDate', 'HolidayName'], inplace=True)
    table_Holiday.to_excel(writer, sheet_name='Holiday', index=False)

    table_Sold = df_sales[['storeid', 'productid', 'date', 'quantity']]
    table_Sold = table_Sold.rename(columns={'storeid': 'StoreNumber', 'productid': 'PID', 'date': 'CalendarDate', 'quantity': 'quantity'})
    table_Sold = table_Sold.dropna()
    table_Sold.drop_duplicates(subset=['StoreNumber', 'PID', 'CalendarDate'], inplace=True)
    table_Sold = table_Sold[table_Sold['quantity'] > 0]
    table_Sold.to_excel(writer, sheet_name='Sold', index=False)

    table_OnSale = df_saleprices[['productid', 'date', 'saleprice']]
    table_OnSale = table_OnSale.rename(columns={'productid': 'PID', 'date': 'CalendarDate', 'saleprice': 'SalePrice'})
    table_OnSale = table_OnSale.dropna()
    table_OnSale.drop_duplicates(subset=['PID', 'CalendarDate'], inplace=True)
    if verbose:
        print('table_OnSale, Original number of items: {}'.format(table_OnSale.shape[0]))
    for i in range(table_OnSale.shape[0]):
        pid_i = table_OnSale['PID'].iloc[i]
        saleprice_i = table_OnSale['SalePrice'].iloc[i]
        retailprice_i = table_Product[table_Product['PID'] == pid_i]['retail_price'].iloc[0]
        manufacturer_i = table_Product[table_Product['PID'] == pid_i]['ManufacturerName'].iloc[0]
        max_discount_i = table_Manufacturer[table_Manufacturer['ManufacturerName'] == manufacturer_i]['max_discount'].iloc[0]
        if saleprice_i > retailprice_i or 1. * (retailprice_i - saleprice_i) / retailprice_i > max_discount_i:
            table_OnSale['SalePrice'].iloc[i] = np.nan
    table_OnSale = table_OnSale.dropna()
    if verbose:
        print('table_OnSale, Updated number of items: {}'.format(table_OnSale.shape[0]))
    table_OnSale.to_excel(writer, sheet_name='OnSale', index=False)

    ManagerInfo = df_stores[['managerfirstname', 'managerlastname', 'manageremail']]
    ManagerFullName = pd.DataFrame({'manager_name': ManagerInfo['managerfirstname'] + ' ' + ManagerInfo['managerlastname']})
    table_Manager = pd.concat([ManagerInfo[['manageremail']], ManagerFullName], axis=1)
    table_Manager = table_Manager.rename(columns={'manageremail': 'Email'})
    table_Manager = table_Manager.dropna()
    table_Manager.drop_duplicates(subset=['Email'], inplace=True)
    employment_status = pd.DataFrame({'EmploymentStatus': np.ones(table_Manager.shape[0]).astype(np.int)}, index=table_Manager.index)
    table_Manager = pd.concat([table_Manager, employment_status], axis=1)
    table_Manager.to_excel(writer, sheet_name='Manager', index=False)

    table_AssignedTo = df_stores[['manageremail', 'storeid']]
    table_AssignedTo = table_AssignedTo.rename(columns={'manageremail': 'Email', 'storeid': 'StoreNumber'})
    table_AssignedTo = table_AssignedTo.dropna()
    table_AssignedTo.drop_duplicates(subset=['Email', 'StoreNumber'], inplace=True)
    table_AssignedTo.to_excel(writer, sheet_name='AssignedTo', index=False)

    writer.save()


def tables_creation_db(dbuser='root', dbpw='db6400', dbname='testdb', verbose=False):

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
    except:
        my_connection = pymysql.connect(host="localhost", user=dbuser, password=dbpw)
        my_cursor = my_connection.cursor()
        my_cursor.execute("CREATE DATABASE {}".format(dbname))
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
    my_cursor = mydb.cursor()

    table_if_exists = "SHOW TABLES LIKE %s"

    if my_cursor.execute(table_if_exists, 'City') == 0:
        my_cursor.execute("CREATE TABLE City" + "\n"
                          + "(CityState VARCHAR(50) NOT NULL," + "\n"
                          + "CityName VARCHAR(50) NOT NULL," + "\n"
                          + "State VARCHAR(50) NOT NULL," + "\n"
                          + "population INT NOT NULL," + "\n"
                          + "city_size VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (CityState)," + "\n"
                          + "CONSTRAINT chk_population_nonegative CHECK (population >= 0)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Store') == 0:
        my_cursor.execute("CREATE TABLE Store" + "\n"
                          + "(StoreNumber INT NOT NULL," + "\n"
                          + "phone_number VARCHAR(50) NOT NULL," + "\n"
                          + "street_address VARCHAR(50) NOT NULL," + "\n"
                          + "CityState VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (StoreNumber)," + "\n"
                          + "CONSTRAINT city_state FOREIGN KEY (CityState) REFERENCES City(CityState)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    '''
    if my_cursor.execute(table_if_exists, 'LocatedIn') == 0 and my_cursor.execute(table_if_exists, 'City') == 1:
        my_cursor.execute("CREATE TABLE LocatedIn" + "\n"
                          + "(StoreNumber INT NOT NULL," + "\n"
                          + "CityName VARCHAR(50) NOT NULL," + "\n"
                          + "State VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (StoreNumber, CityName, State)," + "\n"
                          + "FOREIGN KEY (StoreNumber) REFERENCES Store(StoreNumber)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "CONSTRAINT city_state FOREIGN KEY (CityName, State) REFERENCES City(CityName, State)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    '''
    if my_cursor.execute(table_if_exists, 'Manufacturer') == 0:
        my_cursor.execute("CREATE TABLE Manufacturer" + "\n"
                          + "(ManufacturerName VARCHAR(50) NOT NULL," + "\n"
                          + "max_discount FLOAT NOT NULL," + "\n"
                          + "PRIMARY KEY (ManufacturerName)," + "\n"
                          + "CONSTRAINT chk_max_discount_range CHECK (max_discount >= 0.0 AND max_discount <= 0.9)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Product') == 0:
        my_cursor.execute("CREATE TABLE Product" + "\n"
                          + "(PID INT NOT NULL," + "\n"
                          + "product_name VARCHAR(50) NOT NULL," + "\n"
                          + "retail_price FLOAT NOT NULL," + "\n"
                          + "ManufacturerName VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (PID)," + "\n"
                          + "FOREIGN KEY (ManufacturerName) REFERENCES Manufacturer(ManufacturerName)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "CONSTRAINT chk_retail_price_nonegative CHECK (retail_price >= 0.0)" + "\n"
                          + ");")
    '''
    if my_cursor.execute(table_if_exists, 'Produces') == 0:
        my_cursor.execute("CREATE TABLE Produces" + "\n"
                          + "(ManufacturerName VARCHAR(50) NOT NULL," + "\n"
                          + "PID INT NOT NULL," + "\n"
                          + "PRIMARY KEY (ManufacturerName, PID)," + "\n"
                          + "FOREIGN KEY (ManufacturerName) REFERENCES Manufacturer(ManufacturerName)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (PID) REFERENCES Product(PID)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    '''
    if my_cursor.execute(table_if_exists, 'Category') == 0:
        my_cursor.execute("CREATE TABLE Category" + "\n"
                          + "(CategoryName VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (CategoryName)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'BelongTo') == 0:
        my_cursor.execute("CREATE TABLE BelongTo" + "\n"
                          + "(PID INT NOT NULL," + "\n"
                          + "CategoryName VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (PID, CategoryName)," + "\n"
                          + "FOREIGN KEY (PID) REFERENCES Product(PID)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (CategoryName) REFERENCES Category(CategoryName)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Cdate') == 0:
        my_cursor.execute("CREATE TABLE Cdate" + "\n"
                          + "(CalendarDate DATE NOT NULL," + "\n"
                          + "PRIMARY KEY(CalendarDate)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Holiday') == 0:
        my_cursor.execute("CREATE TABLE Holiday" + "\n"
                          + "(CalendarDate DATE NOT NULL," + "\n"
                          + "HolidayName VARCHAR(50) NOT NULL," + "\n"
                          + "PRIMARY KEY (CalendarDate, HolidayName)," + "\n"
                          + "FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Sold') == 0:
        my_cursor.execute("CREATE TABLE Sold" + "\n"
                          + "(StoreNumber INT NOT NULL," + "\n"
                          + "PID INT NOT NULL," + "\n"
                          + "CalendarDate DATE NOT NULL," + "\n"
                          + "quantity INT NOT NULL," + "\n"
                          + "PRIMARY KEY (StoreNumber, PID, CalendarDate)," + "\n"
                          + "FOREIGN KEY (StoreNumber) REFERENCES Store(StoreNumber)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (PID) REFERENCES Product(PID)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "CONSTRAINT chk_quantity_nonegative CHECK (quantity >= 0)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'OnSale') == 0:
        my_cursor.execute("CREATE TABLE OnSale" + "\n"
                          + "(PID INT NOT NULL," + "\n"
                          + "CalendarDate DATE NOT NULL," + "\n"
                          + "SalePrice FLOAT NOT NULL," + "\n"
                          + "PRIMARY KEY (PID, CalendarDate)," + "\n"
                          + "FOREIGN KEY (PID) REFERENCES Product(PID)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'Manager') == 0:
        my_cursor.execute("CREATE TABLE Manager" + "\n"
                          + "(Email VARCHAR(50) NOT NULL," + "\n"
                          + "manager_name VARCHAR(50) NOT NULL," + "\n"
                          + "EmploymentStatus TINYINT(1) NOT NULL," + "\n"
                          + "PRIMARY KEY (Email)" + "\n"
                          + ");")
    if my_cursor.execute(table_if_exists, 'AssignedTo') == 0:
        my_cursor.execute("CREATE TABLE AssignedTo" + "\n"
                          + "(Email VARCHAR(50) NOT NULL," + "\n"
                          + "StoreNumber INT NOT NULL," + "\n"
                          + "PRIMARY KEY (Email, StoreNumber)," + "\n"
                          + "FOREIGN KEY (Email) REFERENCES Manager(Email)" + "\n"
                          + "ON UPDATE CASCADE," + "\n"
                          + "FOREIGN KEY (StoreNumber) REFERENCES Store(StoreNumber)" + "\n"
                          + "ON DELETE CASCADE ON UPDATE CASCADE" + "\n"
                          + ");")


def views_creation_db(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()

        # Report 4 dropdown box for listing states
        my_cursor.execute("CREATE OR REPLACE VIEW States\n"
                          + "AS SELECT DISTINCT State\n"
                          + "FROM City\n"
                          + "ORDER BY State ASC;")

        # Report 5 - Air Conditioners on Groundhog Day
        my_cursor.execute("CREATE OR REPLACE VIEW Report5_ACGroundhog\n"
                          + "AS SELECT General.Year AS Year, General.ItemsSold_Annually AS ItemsSold_Annually, \n"
                          + "General.ItemsSold_DailyOnAverage AS ItemsSold_DailyOnAverage, Groundhog.ItemsSold_OnGroundhog AS ItemsSold_OnGroundhog\n"
                          + "FROM\n"
                          + "(\n"
                          + "SELECT YEAR(Sold.CalendarDate) AS Year, SUM(Sold.quantity) AS ItemsSold_Annually, SUM(Sold.quantity)/365.0 AS ItemsSold_DailyOnAverage\n"
                          + "FROM Sold\n"
                          + "LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID\n"
                          + "WHERE BelongTo.CategoryName = 'Air Conditioner'\n"
                          + "GROUP BY YEAR(Sold.CalendarDate)\n"
                          + ") AS General\n"
                          + "LEFT OUTER JOIN\n"
                          + "(\n"
                          + "SELECT YEAR(Sold.CalendarDate) AS Year, SUM(Sold.quantity) AS ItemsSold_OnGroundhog\n"
                          + "FROM Sold\n"
                          + "LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID\n"
                          + "WHERE BelongTo.CategoryName = 'Air Conditioner' AND MONTH(Sold.CalendarDate) = 2 AND DAY(Sold.CalendarDate) = 2\n"
                          + "GROUP BY YEAR(Sold.CalendarDate)\n"
                          + ") AS Groundhog\n"
                          + "ON General.Year = Groundhog.Year\n"
                          + "ORDER BY Year ASC;")

        # Report 7 - Revenue by Population (before pivot)
        my_cursor.execute("CREATE OR REPLACE VIEW Report7_Before_Pivot\n"
                          + "AS SELECT Year, city_size AS Population_Category, 1.0*SUM(CityAnnualRevenue)/COUNT(*) AS Average_Revenue\n"
                          + "FROM\n"
                          + "(\n"
                          + "SELECT Year, city_size, CityState, SUM(retail_price * RetailQuantity + SalePrice * SaleQuantity) AS CityAnnualRevenue\n"
                          + "FROM\n"
                          + "(\n"
                          + "SELECT YEAR(Sold.CalendarDate) AS Year, City.CityState AS CityState, \n"
                          + "City.city_size AS city_size, Product.retail_price AS retail_price, \n"
                          + "(CASE\n"
                          + "WHEN OnSale.SalePrice is NULL THEN 0\n"
                          + "ELSE OnSale.SalePrice\n"
                          + "END) AS SalePrice,\n"
                          + "(CASE\n"
                          + "WHEN OnSale.SalePrice is NULL THEN Sold.quantity\n"
                          + "ELSE 0\n"
                          + "END) AS RetailQuantity,\n"
                          + "(CASE\n"
                          + "WHEN OnSale.SalePrice is NULL THEN 0\n"
                          + "ELSE Sold.quantity\n"
                          + "END) AS SaleQuantity\n"
                          + "FROM Sold\n"
                          + "LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber\n"
                          + "LEFT OUTER JOIN City ON Store.CityState = City.CityState\n"
                          + "LEFT OUTER JOIN Product ON Sold.PID = Product.PID\n"
                          + "LEFT OUTER JOIN OnSale ON Sold.PID = OnSale.PID AND Sold.CalendarDate = OnSale.CalendarDate\n"
                          + ") AS Report7_prelim\n"
                          + "GROUP BY Year, CityState\n"
                          + ") AS Report7_almostready\n"
                          + "GROUP BY Year, city_size\n"
                          + "ORDER BY Year ASC, Population_Category DESC;")
        mydb.commit()
    except:
        print("Error occurred when creating views in the database: {}.".format(dbname))
        return


def write_tables_to_db(input_dir=r'C:\Users\chu\Downloads\DB6400_Project_Phase_3\From_Phase2\Sample_Data\output',
                         cleaned_excel_file='Sample_data_cleaned.xlsx', dbuser='root', dbpw='db6400', dbname='testdb', verbose=False):

    mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
    my_cursor = mydb.cursor()

    file = pd.ExcelFile(os.path.join(input_dir, cleaned_excel_file))

    sql_stmt = "INSERT INTO City\n" \
               "VALUES (%s, %s, %s, %s, %s)"
    try:
        table_City = file.parse('City').astype('object')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('City'))
        return
    for i in range(table_City.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_City.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Store\n" \
               "VALUES (%s, %s, %s, %s)"
    try:
        table_Store = file.parse('Store').astype('object')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Store'))
        return
    for i in range(table_Store.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_Store.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Manufacturer\n" \
               "VALUES (%s, %s)"
    try:
        table_Manufacturer = file.parse('Manufacturer').astype('object')  # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Manufacturer'))
        return
    for i in range(table_Manufacturer.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_Manufacturer.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Product\n" \
               "VALUES (%s, %s, %s, %s)"
    try:
        table_Product = file.parse('Product').astype('object')   # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Product'))
        return
    for i in range(table_Product.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_Product.iloc[i, :]))
            mydb.commit()
        except:
            pass

    #sql_stmt = "INSERT INTO Produces\n" \
    #           "VALUES (%s, %s)"
    #try:
    #    table_Produces = file.parse('Produces').astype('object')  # pd.DataFrame
    #except OSError:
    #    print('Needed worksheet {} not found.'.format('Produces'))
    #    return
    #for i in range(table_Produces.shape[0]):
    #    try:
    #        my_cursor.execute(sql_stmt, tuple(table_Produces.iloc[i, :]))
    #        mydb.commit()
    #    except:
    #        pass

    sql_stmt = "INSERT INTO Category\n" \
               "VALUES (%s)"
    try:
        table_Category = file.parse('Category').astype('object')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Category'))
        return
    for i in range(table_Category.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_Category.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO BelongTo\n" \
               "VALUES (%s, %s)"
    try:
        table_BelongTo = file.parse('BelongTo').astype('object')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('BelongTo'))
        return
    for i in range(table_BelongTo.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_BelongTo.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Cdate\n" \
               "VALUES (%s)"
    try:
        table_Cdate = file.parse('Cdate')    # pd.DataFrame
        #print('date example: {}'.format(table_Cdate.iloc[0, 0]))
        #print('date type: {}'.format(type(table_Cdate.iloc[0, 0])))
    except OSError:
        print('Needed worksheet {} not found.'.format('Cdate'))
        return
    for i in range(table_Cdate.shape[0]):
        try:
            my_cursor.execute(sql_stmt, (table_Cdate.iloc[i, 0].date()))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Holiday\n" \
               "VALUES (%s, %s)"
    try:
        table_Holiday = file.parse('Holiday')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Holiday'))
        return
    for i in range(table_Holiday.shape[0]):
        try:
            my_cursor.execute(sql_stmt, (table_Holiday.iloc[i, 0].date(), table_Holiday.iloc[i, 1]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Sold\n" \
               "VALUES (%s, %s, %s, %s)"
    try:
        table_Sold = file.parse('Sold')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Sold'))
        return
    for i in range(table_Sold.shape[0]):
        try:
            my_cursor.execute(sql_stmt, (int(table_Sold.iloc[i, 0]), int(table_Sold.iloc[i, 1]), table_Sold.iloc[i, 2].date(), int(table_Sold.iloc[i, 3])))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO OnSale\n" \
               "VALUES (%s, %s, %s)"
    try:
        table_OnSale = file.parse('OnSale')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('OnSale'))
        return
    for i in range(table_OnSale.shape[0]):
        try:
            my_cursor.execute(sql_stmt, (int(table_OnSale.iloc[i, 0]), table_OnSale.iloc[i, 1].date(), float(table_OnSale.iloc[i, 2])))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO Manager\n" \
               "VALUES (%s, %s, %s)"
    try:
        table_Manager = file.parse('Manager').astype('object')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('Manager'))
        return
    for i in range(table_Manager.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_Manager.iloc[i, :]))
            mydb.commit()
        except:
            pass

    sql_stmt = "INSERT INTO AssignedTo\n" \
               "VALUES (%s, %s)"
    try:
        table_AssignedTo = file.parse('AssignedTo').astype('object')    # pd.DataFrame
    except OSError:
        print('Needed worksheet {} not found.'.format('AssignedTo'))
        return
    for i in range(table_AssignedTo.shape[0]):
        try:
            my_cursor.execute(sql_stmt, tuple(table_AssignedTo.iloc[i, :]))
            mydb.commit()
        except:
            pass


def simple_count(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', table='Store', verbose=False):
    """
    Return count of rows for a given table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    table: str
    verbose: boolean

    :return
    count: int

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "SELECT COUNT(*) FROM `%s`;" %table
        my_cursor.execute(sql_stmt)#, (table))
        count = my_cursor.fetchone()[0]   # int
        if verbose:
            print('{} count: {}'.format(table, count))
        return count    # int
    except:
        print("Error occurred at simple_count SQL query for Table {}.".format(table))
        return


def view_holidays(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return the full content of Holiday table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    table_Holiday: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT * FROM `Holiday`;"
        table_Holiday = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('Holidays:\n{}'.format(table_Holiday))
        return table_Holiday
    except:
        print("Error occurred at retrieving Table Holiday data.")
        return


def add_a_holiday(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', year=2019, month=1, day=1, holiday_name='New Year', verbose=False):
    """
    Add one holiday (one row) to the Holiday table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    year: int
    month: int
    day: int
    holiday_name: str
    verbose: boolean

    :return
    success: boolean (True: successfully added, False: failed to be added)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "INSERT INTO `Holiday`(CalendarDate, HolidayName)\n" \
                   "VALUES (%s, %s);"
        my_cursor.execute(sql_stmt, (dt.date(year, month, day), holiday_name))
        mydb.commit()
        success = True
    except:
        print("Error occurred when adding a new holiday (a new row into Table Holiday).")
        success = False

    return success


def view_managers(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return the full info of managers

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    view_managers: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT M.Email AS Email, M.manager_name AS manager_name, M.EmploymentStatus AS EmploymentStatus, AT.StoreNumber AS StoreNumber\n" \
                   "FROM (Manager AS M LEFT OUTER JOIN AssignedTo AS AT\n" \
                   "ON M.Email = AT.Email);"
        view_managers = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('Managers:\n{}'.format(view_managers))
        return view_managers
    except:
        print("Error occurred at retrieving full info of managers.")
        return


def add_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                  manager_name='John Welch', employment_status=1, verbose=False):
    """
    Add one holiday (one row) to the Holiday table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    manager_name: str
    employment_status: int
    verbose: boolean

    :return
    success: boolean (True: successfully added, False: failed to be added)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "INSERT INTO `Manager`(Email, manager_name, EmploymentStatus)\n" \
                   "VALUES (%s, %s, %s);"
        my_cursor.execute(sql_stmt, (email, manager_name, employment_status))
        mydb.commit()
        success = True
    except:
        print("Error occurred when adding a new manager (a new row into Table Manager).")
        success = False

    return success


def remove_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                     verbose=False):
    """
    Remove the specified manager (one row) from the Manager table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "DELETE FROM `Manager`\n" \
                   "WHERE Email = %s;"
        my_cursor.execute(sql_stmt, email)
        mydb.commit()
        success = True
    except:
        print("Error occurred when removing a manager (a row) from Table Manager).")
        success = False

    return success


def activate_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                       verbose=False):
    """
    Activate the specified manager in the Manager table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "UPDATE `Manager`\n" \
                   "SET EmploymentStatus = 1\n" \
                   "WHERE Email = %s;"
        my_cursor.execute(sql_stmt, email)
        mydb.commit()
        success = True
    except:
        print("Error occurred when activating a manager in Table Manager).")
        success = False

    return success


def deactivate_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                         verbose=False):
    """
    Deactivate the specified manager in the Manager table

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "UPDATE `Manager`\n" \
                   "SET EmploymentStatus = 0\n" \
                   "WHERE Email = %s;"
        my_cursor.execute(sql_stmt, email)
        mydb.commit()
        success = True
    except:
        print("Error occurred when deactivating a manager in Table Manager).")
        success = False

    return success


def assign_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                     store_number=111222333, verbose=False):
    """
    Assign a manager to a store (add a row in AssignedTo table)

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    store_number: int
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "INSERT INTO `AssignedTo`(Email, StoreNumber)\n" \
                   "VALUES (%s, %s);"
        my_cursor.execute(sql_stmt, (email, store_number))
        mydb.commit()
        success = True
    except:
        print("Error occurred when assigning a manager to a store (a new row into Table AssignedTo).")
        success = False

    return success


def unassign_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='john.welch@setechstores.com',
                       store_number=111222333, verbose=False):
    """
    Unassign a manager from a store (remove the row from AssignedTo table)

    :param
    dbuser: str
    dbpw: str
    dbname: str
    email: str
    store_number: int
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "DELETE FROM `AssignedTo`\n" \
                   "WHERE Email = %s AND StoreNumber = %s;"
        my_cursor.execute(sql_stmt, (email, store_number))
        mydb.commit()
        success = True
    except:
        print("Error occurred when unassigning a manager from a store (removing a row from Table AssignedTo).")
        success = False

    return success


def view_cities(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return the info of all cities

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    cities: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT CityName, State, population, city_size\n" \
                   "FROM City;"
        cities = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('Cities:\n{}'.format(cities))
        return cities
    except:
        print("Error occurred at retrieving info of all cities.")
        return


def update_citypopulation(dbuser='root', dbpw='db6400', dbname='se_datawarehouse',
                          city_name='Akron', state='MO', population=7684915, verbose=False):
    """
    Update the city population and city_size for that city

    :param
    dbuser: str
    dbpw: str
    dbname: str
    city_name: str
    state: str
    population: int
    verbose: boolean

    :return
    success: boolean (True: successful, False: failed)

    """
    population_int = int(population)
    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()
        sql_stmt = "UPDATE `City`\n" \
                   "SET population = %s, city_size = %s \n" \
                   "WHERE CityName = %s AND State = %s;"
        if population_int < 3700000:
            city_size = 'Small'
        elif 3700000 <= population_int < 6700000:
            city_size = 'Medium'
        elif 6700000 <= population_int < 9000000:
            city_size = 'Large'
        elif population_int >= 9000000:
            city_size = 'Extra Large'
        my_cursor.execute(sql_stmt, (population_int, city_size, city_name, state))
        mydb.commit()
        success = True
    except:
        print("Error occurred when updating city population and city_size.")
        success = False

    return success


def view_states(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return a list (pd.DataFrame) of all states

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    states: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        #sql_stmt = "SELECT DISTINCT State\n" \
        #           "FROM City\n" \
        #           "ORDER BY State ASC"
        sql_stmt = "SELECT *\n" \
                   "FROM States;"
        states = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('States:\n{}'.format(states))
        return states
    except:
        print("Error occurred at retrieving all states.")
        return


def view_Report4(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', state='AZ', verbose=False):
    """
    Return Report 4 content

    :param
    dbuser: str
    dbpw: str
    dbname: str
    state: str
    verbose: boolean

    :return
    report4_byState: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)

        sql_stmt = "SELECT StoreCityYearQuantity.StoreNumber AS StoreNumber, \n" \
                   "StoreCityYearQuantity.street_address AS street_address, \n" \
                   "StoreCityYearQuantity.CityName AS CityName, \n" \
                   "YEAR(StoreCityYearQuantity.CalendarDate) AS Year,\n" \
                   "SUM(StoreCityYearQuantity.retail_price * StoreCityYearQuantity.RetailQuantity + StoreCityYearQuantity.SalePrice * StoreCityYearQuantity.SaleQuantity) AS TotalRevenue\n" \
                   "FROM\n" \
                   "(\n" \
                   "SELECT Store.StoreNumber AS StoreNumber, Store.street_address AS street_address, City.CityName AS CityName, \n" \
                   "Sold.CalendarDate AS CalendarDate, Product.retail_price AS retail_price,\n" \
                   "(CASE\n" \
                   "  WHEN OnSale.SalePrice IS NULL THEN 0\n" \
                   "  ELSE OnSale.SalePrice\n" \
                   "  END) AS SalePrice,\n" \
                   " (CASE\n" \
                   " WHEN OnSale.SalePrice is NULL THEN Sold.quantity\n" \
                   " ELSE 0\n" \
                   " END) AS RetailQuantity,\n" \
                   "(CASE\n" \
                   " WHEN OnSale.SalePrice is NULL THEN 0\n" \
                   " ELSE Sold.quantity\n" \
                   " END) AS SaleQuantity\n" \
                   " FROM Sold\n" \
                   " LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber\n" \
                   " LEFT OUTER JOIN City ON Store.CityState = City.CityState\n" \
                   " LEFT OUTER JOIN Product ON Sold.PID = Product.PID\n" \
                   " LEFT OUTER JOIN OnSale ON Sold.PID = OnSale.PID AND Sold.CalendarDate = OnSale.CalendarDate\n" \
                   " WHERE City.State = %s\n" \
                   " ) StoreCityYearQuantity\n" \
                   " GROUP BY StoreCityYearQuantity.StoreNumber, YEAR(StoreCityYearQuantity.CalendarDate)\n" \
                   " ORDER BY Year ASC, TotalRevenue DESC;"

        report4_byState = pd.read_sql(sql_stmt, mydb, params=[state])
        if verbose:
            print('Report 4 by State {}:\n{}'.format(state, report4_byState))
        return report4_byState
    except:
        print("Error occurred when generating Report 4 for State {}.".format(state))
        return


def view_Report5(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return Report 5 - Air Conditioners on Groundhog Day

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    report5: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT *\n" \
                   "FROM Report5_ACGroundhog;"
        report5 = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('Report 5:\n{}'.format(report5))
        return report5
    except:
        print("Error occurred when generating Report 5 - Air Conditioners on Groundhog Day.")
        return


def view_yearmonth(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return a list (pd.DataFrame) of available "year & month"s in the database (For Report 6 use)

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    year_month: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT DISTINCT YEAR(CalendarDate) AS Year, MONTH(CalendarDate) AS Month\n" \
                   "FROM Cdate\n" \
                   "ORDER BY YEAR ASC, MONTH ASC;"
        year_month = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print("Available 'year & month's in the database {}:\n{}".format(dbname, year_month))
        return year_month
    except:
        print("Error occurred when all available 'year & month's in the database {}.".format(dbname))
        return


def view_Report6_master(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', year=2009, month=8, verbose=False):
    """
    Return Report 6 (master report) for a given pair of year and month

    :param
    dbuser: str
    dbpw: str
    dbname: str
    year: int
    month: int
    verbose: boolean

    :return
    report6_master: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT Rep6_prelim.CategoryName, Rep6_prelim.State, Rep6_prelim.Quantity\n" \
                   "FROM\n" \
                   "(\n" \
                   "SELECT BelongTo.CategoryName AS CategoryName, City.State AS State, SUM(Sold.quantity) AS Quantity\n" \
                   "FROM Sold\n" \
                   "LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber\n" \
                   "LEFT OUTER JOIN City ON Store.CityState = City.CityState\n" \
                   "LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID\n" \
                   "WHERE YEAR(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s\n" \
                   "GROUP BY BelongTo.CategoryName, City.State\n" \
                   ") AS Rep6_prelim\n" \
                   "INNER JOIN\n" \
                   "(\n" \
                   "SELECT CategoryName AS CategoryName, MAX(Quantity) AS Max_Quantity\n" \
                   "FROM \n" \
                   "(\n" \
                   "SELECT BelongTo.CategoryName AS CategoryName, City.State AS State, SUM(Sold.quantity) AS Quantity\n" \
                   "FROM Sold\n" \
                   "LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber\n" \
                   "LEFT OUTER JOIN City ON Store.CityState = City.CityState\n" \
                   "LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID\n" \
                   "WHERE YEAR(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s\n" \
                   "GROUP BY BelongTo.CategoryName, City.State\n" \
                   ") As temp\n" \
                   "GROUP BY CategoryName\n" \
                   ") AS Category_MaxQuantity\n" \
                   "ON Rep6_prelim.CategoryName = Category_MaxQuantity.CategoryName \n" \
                   "AND Rep6_prelim.Quantity = Category_MaxQuantity.Max_Quantity\n" \
                   "ORDER BY CategoryName ASC;"
        report6_master = pd.read_sql(sql_stmt, mydb, params=[year, month, year, month])
        if verbose:
            print('Report 6 (master report):\n{}'.format(report6_master))
        return report6_master
    except:
        print("Error occurred when generating Report 6 (master report).")
        return


def view_Report6_drilldown(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', state='AZ',
                           category='Air Conditioner', year=2009, month=8, verbose=False):
    """
    Return Report 6 (drill-down sub report) for a given set of state, category, year and month

    :param
    dbuser: str
    dbpw: str
    dbname: str
    state: str
    category: str
    year: int
    month: int
    verbose: boolean

    :return
    report6_drilldown: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT DISTINCT Store.StoreNumber AS StoreNumber, Store.street_address AS street_address, \n" \
                   "City.CityName AS CityName, Manager.manager_name AS manager_name, Manager.Email AS Email\n" \
                   "FROM Sold\n" \
                   "LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber\n" \
                   "LEFT OUTER JOIN City ON Store.CityState = City.CityState\n" \
                   "LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID\n" \
                   "LEFT OUTER JOIN AssignedTo ON Sold.StoreNumber = AssignedTo.StoreNumber\n" \
                   "LEFT OUTER JOIN Manager ON AssignedTo.Email = Manager.Email\n" \
                   "WHERE City.State = %s AND BelongTo.CategoryName = %s \n" \
                   "AND Year(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s\n" \
                   "AND Manager.EmploymentStatus = 1\n" \
                   "ORDER BY StoreNumber ASC;"
        report6_drilldown = pd.read_sql(sql_stmt, mydb, params=[state, category, year, month])
        if verbose:
            print('Report 6 (drill-down report):\n{}'.format(report6_drilldown))
        return report6_drilldown
    except:
        print("Error occurred when generating Report 6 (drill-down report).")
        return


def view_Report7_beforePivot(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False):
    """
    Return Report 7 - Revenue by Population (Before Pivot)

    :param
    dbuser: str
    dbpw: str
    dbname: str
    verbose: boolean

    :return
    report7_nopivot: pd.DataFrame

    """

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        sql_stmt = "SELECT *\n" \
                   "FROM Report7_Before_Pivot;"
        report7_nopivot = pd.read_sql(sql_stmt, mydb)
        if verbose:
            print('report7_nopivot:\n')
            print('{}'.format(np.array(report7_nopivot.columns)))
            for i in range(report7_nopivot.shape[0]):
                print('{}'.format(np.array(report7_nopivot.iloc[i, :])))
        return report7_nopivot
    except:
        print("Error occurred when generating Report 7 - Revenue by Population (Before Pivot).")
        return




if __name__ == '__main__':

    ## Database preparation using sample data ##
    #tables_preparation(input_dir=r'C:\Users\chu\Downloads\DB6400_Project_Phase_3\From_Phase2\Sample_Data',
    #                   input_excel_file=r'Sample Data.xlsx', output_excel_filename='Sample_data_cleaned.xlsx', verbose=True)
    #tables_creation_db(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False)
    #write_tables_to_db(input_dir=r'C:\Users\chu\Downloads\DB6400_Project_Phase_3\From_Phase2\Sample_Data\output',
    #                   cleaned_excel_file='Sample_data_cleaned.xlsx', dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False)

    ## Main menu statistics ##
    #stores_count = simple_count(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', table='Store', verbose=True)
    #manufacturers_count = simple_count(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', table='Manufacturer', verbose=True)
    #products_count = simple_count(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', table='Product', verbose=True)
    #managers_count = simple_count(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', table='Manager', verbose=True)

    ## View/Add holiday information ##
    #holidays = view_holidays(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)
    #add_holiday_success = add_a_holiday(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', year=2002, month=11, day=11,
    #                                    holiday_name='DDDDDD', verbose=False)

    ## View/Add/Remove/Activate/Deactivate a manager
    ## Assign/Unassign a manager to/from a store
    #managers = view_managers(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)
    #add_manager_success = add_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse',
    #                                    email='zzzzzzzzzzzzzzzz@setechstores.com', manager_name='zzzzzzzzzzzz',
    #                                    employment_status=1, verbose=False)
    #remove_manager_success = remove_a_manager(dbuser='root', dbpw='db6400',
    #                                          dbname='se_datawarehouse', email='zzzzzzzzzzzzzzzz@setechstores.com',
    #                                          verbose=False)
    #deactivate_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='abigail.burdell@setechstores.com',
    #                     verbose=False)
    #activate_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='abigail.burdell@setechstores.com',
    #                   verbose=False)
    #assign_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='lina.james@setechstores.com',
    #                 store_number=1, verbose=False)
    #unassign_a_manager(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', email='lina.james@setechstores.com',
    #                   store_number=1, verbose=False)

    ## View/Update city population and city_size
    #view_cities(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)
    #update_citypopulation(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', city_name='Akron', state='MO', verbose=False)

    ## Report 4 - Store Revenue by Year by State
    views_creation_db(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False)
    #view_states(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)
    #report4_byState = view_Report4(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', state='AZ', verbose=True)

    ## Report 5
    #view_Report5(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)

    ## Report 6
    #dates = view_yearmonth(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=True)
    #report6_master = view_Report6_master(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', year=2005, month=11, verbose=True)
    #report6_drilldown = view_Report6_drilldown(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', state='AZ',
    #                                           category='Air Conditioner', year=2005, month=12, verbose=True)

    ## Report 7
    #report7_before_pivot = view_Report7_beforePivot(dbuser='root', dbpw='db6400', dbname='se_datawarehouse', verbose=False)
    #report7_after_pivot = report7_before_pivot.pivot_table(values='Average_Revenue', index=['Year'], columns=['Population_Category'])[['Small', 'Medium', 'Large', 'Extra Large']]
    #verbose = False
    #if verbose:
    #    print('Report 7 after pivot:\n\t{}'.format(np.array(report7_after_pivot.columns)))
    #    for i in range(report7_after_pivot.shape[0]):
    #        print('{}: {}'.format(report7_after_pivot.index[i], np.array(report7_after_pivot.iloc[i, :])))

    pass