/* Schema design (CREATE TABLE) */

CREATE TABLE City
    (CityState VARCHAR(50) NOT NULL,
     CityName VARCHAR(50) NOT NULL,
     State VARCHAR(50) NOT NULL,
     population INT NOT NULL,
     city_size VARCHAR(50) NOT NULL,
     PRIMARY KEY (CityState),
     CONSTRAINT chk_population_nonegative CHECK (population >= 0)
);

CREATE TABLE Store
    (StoreNumber INT NOT NULL,
     phone_number VARCHAR(50) NOT NULL,
     street_address VARCHAR(50) NOT NULL,
     CityState VARCHAR(50) NOT NULL,
     PRIMARY KEY (StoreNumber),
     FOREIGN KEY (CityState) REFERENCES City(CityState)
                    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Manufacturer
    (ManufacturerName VARCHAR(50) NOT NULL,
     max_discount FLOAT NOT NULL,
     PRIMARY KEY (ManufacturerName),
     CONSTRAINT chk_max_discount_range CHECK (max_discount >= 0.0 AND max_discount <= 0.9)
);    

CREATE TABLE Product
    (PID INT NOT NULL,
     product_name VARCHAR(50) NOT NULL,
     retail_price FLOAT NOT NULL,
     ManufacturerName VARCHAR(50) NOT NULL,
	 PRIMARY KEY (PID),
     FOREIGN KEY (ManufacturerName) REFERENCES Manufacturer(ManufacturerName)
                    ON DELETE CASCADE ON UPDATE CASCADE,
	 CONSTRAINT chk_retail_price_nonegative CHECK (retail_price >= 0.0)
);

CREATE TABLE Category
    (CategoryName VARCHAR(50) NOT NULL,
     PRIMARY KEY (CategoryName)
);

CREATE TABLE BelongTo
    (PID INT NOT NULL,
	 CategoryName VARCHAR(50) NOT NULL,
	 PRIMARY KEY (PID, CategoryName),
     FOREIGN KEY (PID) REFERENCES Product(PID)
                    ON DELETE CASCADE ON UPDATE CASCADE,
     FOREIGN KEY (CategoryName) REFERENCES Category(CategoryName)
                    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Cdate
    (CalendarDate DATE NOT NULL,
	 PRIMARY KEY(CalendarDate)
);


CREATE TABLE Holiday
    (CalendarDate DATE NOT NULL,
     HolidayName VARCHAR(50) NOT NULL,
     PRIMARY KEY (CalendarDate, HolidayName),
     FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)
                    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Sold
    (StoreNumber INT NOT NULL,
     PID INT NOT NULL,
     CalendarDate DATE NOT NULL,
     quantity INT NOT NULL,
     PRIMARY KEY (StoreNumber, PID, CalendarDate),
     FOREIGN KEY (StoreNumber) REFERENCES Store(StoreNumber)
                    ON DELETE CASCADE ON UPDATE CASCADE,
     FOREIGN KEY (PID) REFERENCES Product(PID)
                    ON DELETE CASCADE ON UPDATE CASCADE,
     FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)
                    ON DELETE CASCADE ON UPDATE CASCADE,
     CONSTRAINT chk_quantity_nonegative CHECK (quantity >= 0)
);

CREATE TABLE OnSale
    (PID INT NOT NULL,
     CalendarDate DATE NOT NULL,
     SalePrice FLOAT NOT NULL,
     PRIMARY KEY (PID, CalendarDate),
	 FOREIGN KEY (PID) REFERENCES Product(PID)
                    ON DELETE CASCADE ON UPDATE CASCADE,
     FOREIGN KEY (CalendarDate) REFERENCES Cdate(CalendarDate)
                    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Manager
    (Email VARCHAR(50) NOT NULL,
     manager_name VARCHAR(50) NOT NULL,
     EmploymentStatus TINYINT(1) NOT NULL,
     PRIMARY KEY (Email)
);

CREATE TABLE AssignedTo
    (Email VARCHAR(50) NOT NULL,
     StoreNumber INT NOT NULL,
     PRIMARY KEY (Email, StoreNumber),
     FOREIGN KEY (Email) REFERENCES Manager(Email)
                    ON UPDATE CASCADE,
     FOREIGN KEY (StoreNumber) REFERENCES Store(StoreNumber)
                    ON DELETE CASCADE ON UPDATE CASCADE
);


---------------------------------------------------------------------


/* Main Menu Stats - SQL queries for stats summary shown on Main Menu panel */

/* The count of stores */
SELECT COUNT(*) AS StoreCount
FROM Store

/* The count of manufacturers */
SELECT COUNT(*) AS ManufacturerCount
FROM Manufacturer

/* The count of products */
SELECT COUNT(*) AS ProductCount
FROM Product

/* The count of managers */
SELECT COUNT(*) AS ManagerCount
FROM Manager


---------------------------------------------------------------------


/* View/Edit Holiday Information */

/* User clicked on Vew/Edit Holiday Information 
and the software shows all CalendarDate and HolidayName */
SELECT CalendarDate, HolidayName FROM Holiday;

/* Add a holiday*/
INSERT INTO `Holiday`(CalendarDate, HolidayName)
VALUES (%s, %s);


---------------------------------------------------------------------


/* View/Edit Manager information */

/* View Manager Information */
SELECT M.Email AS Email, M.manager_name AS manager_name, M.EmploymentStatus AS EmploymentStatus, AT.StoreNumber AS StoreNumber
FROM (Manager AS M LEFT OUTER JOIN AssignedTo AS AT ON M.Email = AT.Email);

/* Add a manager */
INSERT INTO `Manager`(Email, manager_name, EmploymentStatus)
VALUES (%s, %s, %s);

/* Remove a manager */
DELETE FROM `Manager`
WHERE Email = %s;

/* Activate a manager */
UPDATE `Manager`
SET EmploymentStatus = 1
WHERE Email = %s;

/* Deactivate a manager */
UPDATE `Manager`
SET EmploymentStatus = 0
WHERE Email = %s;

/* Assign a manager to a store */
INSERT INTO `AssignedTo`(Email, StoreNumber)
VALUES (%s, %s);

/* Unassign a manager from a store */
DELETE FROM `AssignedTo`
WHERE Email = %s AND StoreNumber = %s;


---------------------------------------------------------------------


/* View/Edit City population information */

/* View City information */
SELECT CityName, State, population, city_size
FROM City;

/* Update city population for a given city */
/* Assignment of city_size is automatically done by Python code when user specifies population */
UPDATE `City`
SET population = %s, city_size = %s
WHERE CityName = %s AND State = %s;


---------------------------------------------------------------------


/* Report 1 Manufacturer's Product Report */

/* Report 1 - Master Report */
SELECT ManufacturerName, max_discount, COUNT(PID) AS TotalProduct, 
       AVG(retail_price) AS ave_retail_price, 
       MIN(retail_price) AS min_retail_price, 
       MAX(retail_price) AS max_retail_price
FROM
    (
        SELECT manufacturer.ManufacturerName, manufacturer.max_discount, product.PID, product.retail_price
        FROM manufacturer
        LEFT JOIN product
        ON manufacturer.ManufacturerName = product.ManufacturerName
    ) ManufacturerProduct
GROUP BY ManufacturerName
ORDER BY ave_retail_price DESC
LIMIT 100;

/* Report 1 - Drill-down Report */
SELECT PID, product_name, GROUP_CONCAT(DISTINCT CategoryName) as CategoryName, retail_price
FROM
    (
        SELECT ManufacturerProduct.PID, ManufacturerProduct.product_name, belongto.CategoryName, ManufacturerProduct.retail_price
        FROM
            (
                SELECT manufacturer.ManufacturerName, product.PID, product.retail_price, product.product_name
                FROM manufacturer
                LEFT JOIN product
                ON manufacturer.ManufacturerName = product.ManufacturerName
                WHERE manufacturer.ManufacturerName = '{}'
            ) ManufacturerProduct
        LEFT JOIN belongto ON ManufacturerProduct.PID = belongto.PID
    ) ManufacturerProductCategory
GROUP BY PID
ORDER BY retail_price DESC;


---------------------------------------------------------------------


/* Report 2 Category Report */

SELECT X.CategoryName, COUNT(X.PID) AS TotalProduct, 
COUNT(DISTINCT X.ManufacturerName) AS TotalManufacturer, 
AVG(X.retail_price) AS AvePrice
FROM
    (
        SELECT CategoryProduct.CategoryName, CategoryProduct.PID, 
               manufacturer.ManufacturerName, product.retail_price
        FROM
            (
                SELECT category.CategoryName, belongto.PID
                FROM category
                LEFT JOIN belongto ON category.CategoryName = belongto.CategoryName
            )CategoryProduct
        LEFT JOIN product ON product.PID = CategoryProduct.PID
        LEFT JOIN manufacturer ON manufacturer.ManufacturerName = product.ManufacturerName
    ) X
GROUP BY X.CategoryName
ORDER BY X.CategoryName ASC;


---------------------------------------------------------------------


/* Report 3 - Actual vs Predicted Revenue for GPS Units */

SELECT ProductRevenueByProduct.PID, ProductRevenueByProduct.product_name, ProductRevenueByProduct.retail_price, ProductRevenueByProduct.TotalUnitsSoldNoDiscount, ProductRevenueByProduct.TotalUnitsSoldWithDiscount,
        ProductRevenueByProduct.ActualRevenue, ProductRevenueByProduct.PredictedRevenue, 
        (ProductRevenueByProduct.ActualRevenue - ProductRevenueByProduct.PredictedRevenue) AS Difference
FROM
    (
    SELECT ProductRevenue.PID, ProductRevenue.product_name, ProductRevenue.retail_price, SUM(ProductRevenue.RetailQuantiy) AS TotalUnitsSoldNoDiscount,
            SUM(ProductRevenue.SaleQuantity) AS TotalUnitsSoldWithDiscount, SUM(ProductRevenue.retail_price * ProductRevenue.RetailQuantiy + ProductRevenue.SalePrice * ProductRevenue.SaleQuantity) AS ActualRevenue,
            SUM(ProductRevenue.retail_price * ProductRevenue.RetailQuantiy + ProductRevenue.retail_price * ProductRevenue.SaleQuantity * 0.75) AS PredictedRevenue
    FROM 
        (
        SELECT sold.CalendarDate, sold.PID, sold.quantity, product.product_name, product.retail_price, IFNULL(onsale.SalePrice,0) AS SalePrice,
        (CASE
          WHEN onsale.SalePrice is NULL THEN sold.quantity
          ELSE 0
        END) AS RetailQuantiy,
        (CASE
          WHEN onsale.SalePrice is NULL THEN 0
          ELSE sold.quantity
        END) AS SaleQuantity
        FROM sold
        LEFT JOIN onsale ON sold.CalendarDate = onsale.CalendarDate AND sold.PID = onsale.PID
        LEFT JOIN product ON sold.PID = product.PID
        LEFT JOIN belongto ON sold.PID = belongto.PID
        WHERE belongto.CategoryName = 'GPS'
        ) ProductRevenue
    GROUP BY ProductRevenue.PID, ProductRevenue.product_name, ProductRevenue.retail_price
    ) ProductRevenueByProduct
HAVING ABS(Difference) > 5000
ORDER BY Difference DESC;


---------------------------------------------------------------------


/*Report 4: View Store Revenue by Year by State*/

/*Report 4 dropdown box for listing states*/
CREATE OR REPLACE VIEW States
AS SELECT DISTINCT State
FROM City
ORDER BY State ASC;

/*SQL query for viewing all states*/
SELECT *
FROM States

/*SQL query for generating Report 4*/
SELECT StoreCityYearQuantity.StoreNumber AS StoreNumber, 
       StoreCityYearQuantity.street_address AS street_address, 
       StoreCityYearQuantity.CityName AS CityName, 
       YEAR(StoreCityYearQuantity.CalendarDate) AS Year,
       SUM(StoreCityYearQuantity.retail_price * StoreCityYearQuantity.RetailQuantity + StoreCityYearQuantity.SalePrice * StoreCityYearQuantity.SaleQuantity) AS TotalRevenue
FROM
    (
        SELECT Store.StoreNumber AS StoreNumber, Store.street_address AS street_address, City.CityName AS CityName, 
        Sold.CalendarDate AS CalendarDate, Product.retail_price AS retail_price,
        (CASE
          WHEN OnSale.SalePrice IS NULL THEN 0
          ELSE OnSale.SalePrice
         END) AS SalePrice,
        (CASE
          WHEN OnSale.SalePrice is NULL THEN Sold.quantity
          ELSE 0
        END) AS RetailQuantity,
        (CASE
          WHEN OnSale.SalePrice is NULL THEN 0
          ELSE Sold.quantity
        END) AS SaleQuantity
        FROM Sold
        LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber
        LEFT OUTER JOIN City ON Store.CityState = City.CityState
        LEFT OUTER JOIN Product ON Sold.PID = Product.PID
        LEFT OUTER JOIN OnSale ON Sold.PID = OnSale.PID AND Sold.CalendarDate = OnSale.CalendarDate
        WHERE City.State = %s
    ) StoreCityYearQuantity
GROUP BY StoreCityYearQuantity.StoreNumber, YEAR(StoreCityYearQuantity.CalendarDate)
ORDER BY Year ASC, TotalRevenue DESC;


---------------------------------------------------------------------


/* Report 5 - Air Conditioners on Groundhog Day */

/*Generate a view in database for Report 5*/
CREATE OR REPLACE VIEW Report5_ACGroundhog
AS SELECT General.Year AS Year, General.ItemsSold_Annually AS ItemsSold_Annually, 
General.ItemsSold_DailyOnAverage AS ItemsSold_DailyOnAverage, Groundhog.ItemsSold_OnGroundhog AS ItemsSold_OnGroundhog
FROM
    (
        SELECT YEAR(Sold.CalendarDate) AS Year, SUM(Sold.quantity) AS ItemsSold_Annually, SUM(Sold.quantity)/365.0 AS ItemsSold_DailyOnAverage
        FROM Sold
        LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID
        WHERE BelongTo.CategoryName = 'Air Conditioner'
        GROUP BY YEAR(Sold.CalendarDate)
    ) AS General
LEFT OUTER JOIN
    (
        SELECT YEAR(Sold.CalendarDate) AS Year, SUM(Sold.quantity) AS ItemsSold_OnGroundhog
        FROM Sold
        LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID
        WHERE BelongTo.CategoryName = 'Air Conditioner' AND MONTH(Sold.CalendarDate) = 2 AND DAY(Sold.CalendarDate) = 2
        GROUP BY YEAR(Sold.CalendarDate)
    ) AS Groundhog
ON General.Year = Groundhog.Year
ORDER BY Year ASC;

/*SQL query for viewing Report 5*/
SELECT *
FROM Report5_ACGroundhog;


---------------------------------------------------------------------


/* Report 6 - State with highest volume for each category */


/* Return year and month available from the dates in the database*/
SELECT DISTINCT YEAR(CalendarDate) AS Year, MONTH(CalendarDate) AS Month
           FROM Cdate
       ORDER BY YEAR ASC, MONTH ASC;

/* Report 6 - Master Report*/
SELECT Rep6_prelim.CategoryName, Rep6_prelim.State, Rep6_prelim.Quantity
FROM
(
    SELECT BelongTo.CategoryName AS CategoryName, City.State AS State, SUM(Sold.quantity) AS Quantity
    FROM Sold
    LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber
    LEFT OUTER JOIN City ON Store.CityState = City.CityState
    LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID
    WHERE YEAR(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s
    GROUP BY BelongTo.CategoryName, City.State
) AS Rep6_prelim
INNER JOIN
(
    SELECT CategoryName AS CategoryName, MAX(Quantity) AS Max_Quantity
    FROM 
    (
        SELECT BelongTo.CategoryName AS CategoryName, City.State AS State, SUM(Sold.quantity) AS Quantity
        FROM Sold
        LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber
        LEFT OUTER JOIN City ON Store.CityState = City.CityState
        LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID
        WHERE YEAR(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s
        GROUP BY BelongTo.CategoryName, City.State
    ) As temp
    GROUP BY CategoryName
) AS Category_MaxQuantity
ON Rep6_prelim.CategoryName = Category_MaxQuantity.CategoryName 
AND Rep6_prelim.Quantity = Category_MaxQuantity.Max_Quantity
ORDER BY CategoryName ASC;


/* Report 6 drill-down */
SELECT DISTINCT Store.StoreNumber AS StoreNumber, Store.street_address AS street_address, 
City.CityName AS CityName, Manager.manager_name AS manager_name, Manager.Email AS Email
FROM Sold
LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber
LEFT OUTER JOIN City ON Store.CityState = City.CityState
LEFT OUTER JOIN BelongTo ON Sold.PID = BelongTo.PID
LEFT OUTER JOIN AssignedTo ON Sold.StoreNumber = AssignedTo.StoreNumber
LEFT OUTER JOIN Manager ON AssignedTo.Email = Manager.Email
WHERE City.State = %s AND BelongTo.CategoryName = %s 
AND Year(Sold.CalendarDate) = %s AND MONTH(Sold.CalendarDate) = %s
AND Manager.EmploymentStatus = 1
ORDER BY StoreNumber ASC;


---------------------------------------------------------------------


/* Report 7 - Revenue by Population */

/* View created for Report 7 Before pivot */
CREATE OR REPLACE VIEW Report7_Before_Pivot
AS SELECT Year, city_size AS Population_Category, 1.0*SUM(CityAnnualRevenue)/COUNT(*) AS Average_Revenue
FROM
(
    SELECT Year, city_size, CityState, SUM(retail_price * RetailQuantity + SalePrice * SaleQuantity) AS CityAnnualRevenue
    FROM
    (
        SELECT YEAR(Sold.CalendarDate) AS Year, City.CityState AS CityState, 
        City.city_size AS city_size, Product.retail_price AS retail_price, 
        (CASE
            WHEN OnSale.SalePrice is NULL THEN 0
            ELSE OnSale.SalePrice
        END) AS SalePrice,
        (CASE
            WHEN OnSale.SalePrice is NULL THEN Sold.quantity
            ELSE 0
        END) AS RetailQuantity,
        (CASE
            WHEN OnSale.SalePrice is NULL THEN 0
            ELSE Sold.quantity
        END) AS SaleQuantity
        FROM Sold
        LEFT OUTER JOIN Store ON Sold.StoreNumber = Store.StoreNumber
        LEFT OUTER JOIN City ON Store.CityState = City.CityState
        LEFT OUTER JOIN Product ON Sold.PID = Product.PID
        LEFT OUTER JOIN OnSale ON Sold.PID = OnSale.PID AND Sold.CalendarDate = OnSale.CalendarDate
    ) AS Report7_prelim
    GROUP BY Year, CityState
) AS Report7_almostready
GROUP BY Year, city_size
ORDER BY Year ASC, Population_Category DESC;

/* SQL query for retrieving Report 7 (before pivot) */
SELECT *
FROM Report7_Before_Pivot;

/*Report 7 after pivot (as shown in the UI) is generated based on Report 7 before pivot by using Python Pandas*/


---------------------------------------------------------------------