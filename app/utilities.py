import pandas as pd
import numpy as np
import os
import pymysql
import datetime as dt

def views_creation_db(dbuser='root', dbpw='19640804', dbname='se_datawarehouse', verbose=False):

    try:
        mydb = pymysql.connect(host="localhost", user=dbuser, password=dbpw, db=dbname)
        my_cursor = mydb.cursor()

        # Report 4 dropdown box for listing states
        my_cursor.execute("CREATE OR REPLACE VIEW States\n"
                          + "AS SELECT DISTINCT State\n"
                          + "FROM city\n"
                          + "ORDER BY State ASC;")

        # Report 5 - Air Conditioners on Groundhog Day
        my_cursor.execute("CREATE OR REPLACE VIEW Report5_ACGroundhog\n"
                          + "AS SELECT General.Year AS Year, General.ItemsSold_Annually AS ItemsSold_Annually, \n"
                          + "General.ItemsSold_DailyOnAverage AS ItemsSold_DailyOnAverage, Groundhog.ItemsSold_OnGroundhog AS ItemsSold_OnGroundhog\n"
                          + "FROM\n"
                          + "(\n"
                          + "SELECT YEAR(sold.CalendarDate) AS Year, SUM(sold.quantity) AS ItemsSold_Annually, SUM(sold.quantity)/365.0 AS ItemsSold_DailyOnAverage\n"
                          + "FROM sold\n"
                          + "LEFT OUTER JOIN belongto ON sold.PID = belongto.PID\n"
                          + "WHERE belongto.CategoryName = 'Air Conditioner'\n"
                          + "GROUP BY YEAR(sold.CalendarDate)\n"
                          + ") AS General\n"
                          + "LEFT OUTER JOIN\n"
                          + "(\n"
                          + "SELECT YEAR(sold.CalendarDate) AS Year, SUM(sold.quantity) AS ItemsSold_OnGroundhog\n"
                          + "FROM sold\n"
                          + "LEFT OUTER JOIN belongto ON sold.PID = belongto.PID\n"
                          + "WHERE belongto.CategoryName = 'Air Conditioner' AND MONTH(sold.CalendarDate) = 2 AND DAY(sold.CalendarDate) = 2\n"
                          + "GROUP BY YEAR(sold.CalendarDate)\n"
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
                          + "SELECT YEAR(sold.CalendarDate) AS Year, city.CityState AS CityState, \n"
                          + "city.city_size AS city_size, product.retail_price AS retail_price, \n"
                          + "(CASE\n"
                          + "WHEN onsale.SalePrice is NULL THEN 0\n"
                          + "ELSE onsale.SalePrice\n"
                          + "END) AS SalePrice,\n"
                          + "(CASE\n"
                          + "WHEN onsale.SalePrice is NULL THEN sold.quantity\n"
                          + "ELSE 0\n"
                          + "END) AS RetailQuantity,\n"
                          + "(CASE\n"
                          + "WHEN onsale.SalePrice is NULL THEN 0\n"
                          + "ELSE sold.quantity\n"
                          + "END) AS SaleQuantity\n"
                          + "FROM sold\n"
                          + "LEFT OUTER JOIN store ON sold.StoreNumber = store.StoreNumber\n"
                          + "LEFT OUTER JOIN city ON store.CityState = city.CityState\n"
                          + "LEFT OUTER JOIN product ON sold.PID = product.PID\n"
                          + "LEFT OUTER JOIN onsale ON sold.PID = onsale.PID AND sold.CalendarDate = onsale.CalendarDate\n"
                          + ") AS Report7_prelim\n"
                          + "GROUP BY Year, CityState\n"
                          + ") AS Report7_almostready\n"
                          + "GROUP BY Year, city_size\n"
                          + "ORDER BY Year ASC, Population_Category DESC;")
        mydb.commit()
        
    except:
        print("Error occurred when creating views in the database: {}.".format(dbname))
        return

if __name__=='__main__':
    views_creation_db()
    