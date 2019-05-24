#!/usr/bin/env python3

from flask import Flask, render_template,request, jsonify, request
import pymysql
import datetime as dt
import pandas as pd
app = Flask(__name__)

class Database:
    def __init__(self):
        self.con = pymysql.connect("localhost","root","root",'se_datawarehouse',cursorclass=pymysql.cursors.
                                   DictCursor)
        self.cur = self.con.cursor()
    def query_data(self,query):
        self.cur.execute(query)
        result = self.cur.fetchall()
        return result
    def execute(self,query,args):
        try:
            self.cur.execute(query,args)
            self.con.commit()
            error = 'Succeed!'
        except Exception as e:
            if hasattr(e, 'message'):
                error = str(e.message)
            else:
                error = str(e)
            
        return error
        
def db_query(query):
        db = Database()
        emps = db.query_data(query)
        return emps

def db_execute(query,args):
        db = Database()
        exes = db.execute(query,args)
        return exes
    
@app.route('/')
def main_menu():
    result = {}
    for table in ['store','manufacturer','product','manager']:
        sql_stmt = "SELECT COUNT(*) AS %s FROM `%s`;" % (table,table)
        result.update(db_query(sql_stmt)[0])
    result = [result]
    return render_template('main_menu.html',result=result, content_type='application/json')

@app.route('/manufacture_product_report')
def manufacture_product_report():
    
    res = db_query(
    """
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
    """
    )
    
    def get_manufacturer_info(name):
        query = """
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
        """.format(name)
        info = db_query(query)
        return info
    
    return render_template('manufacture_product_report.html', result=res, get_manufacturer_info=get_manufacturer_info, content_type='application/json')

@app.route('/air_conditioners_on_groundhog_day_report')
def air_conditioners_on_groundhog_day_report():
    res = db_query(
    "SELECT * FROM Report5_ACGroundhog"
    )
    return render_template('air_conditioners_on_groundhog_day_report.html', result=res, content_type='application/json')

@app.route('/category_report')
def category_report():
    res = db_query(
    """
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
    """
    )
    return render_template('category_report.html', result=res, content_type='application/json')

@app.route('/actual_vs_predicted_revenue_for_gps_units')
def actual_vs_predicted_revenue_for_gps_units():
    res = db_query(
    """
    SELECT ProductRevenueByProduct.PID, ProductRevenueByProduct.product_name, ProductRevenueByProduct.retail_price, (ProductRevenueByProduct.TotalUnitsSoldNoDiscount+ProductRevenueByProduct.TotalUnitsSoldWithDiscount) AS TotalUnitsSold,
    ProductRevenueByProduct.TotalUnitsSoldNoDiscount, ProductRevenueByProduct.TotalUnitsSoldWithDiscount,
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
    """
    )
    return render_template('actual_vs_predicted_revenue_for_gps_units.html', result=res, content_type='application/json')

@app.route('/store_revenue_by_year_by_state', methods=['POST', 'GET'])
def store_revenue_by_year_by_state():
    states = db_query("""SELECT * FROM States""")
    def selected():
        return request.form.get("state-select", None)
    return render_template('store_revenue_by_year_by_state.html', states=states, selected=selected, content_type='application/json')

@app.route('/_update_report4')
def update_report4():
    
    def get_report4_values(selected_state):
        query="SELECT StoreCityYearQuantity.StoreNumber AS StoreNumber, \n" \
                           "StoreCityYearQuantity.street_address AS street_address, \n" \
                           "StoreCityYearQuantity.CityName AS CityName, \n" \
                           "YEAR(StoreCityYearQuantity.CalendarDate) AS Year,\n" \
                           "SUM(StoreCityYearQuantity.retail_price * StoreCityYearQuantity.RetailQuantity + StoreCityYearQuantity.SalePrice * StoreCityYearQuantity.SaleQuantity) AS TotalRevenue\n" \
                           "FROM\n" \
                           "(\n" \
                           "SELECT store.StoreNumber AS StoreNumber, store.street_address AS street_address, city.CityName AS CityName, \n" \
                           "sold.CalendarDate AS CalendarDate, product.retail_price AS retail_price,\n" \
                           "(CASE\n" \
                           "  WHEN onsale.SalePrice IS NULL THEN 0\n" \
                           "  ELSE onsale.SalePrice\n" \
                           "  END) AS SalePrice,\n" \
                           " (CASE\n" \
                           " WHEN onsale.SalePrice is NULL THEN sold.quantity\n" \
                           " ELSE 0\n" \
                           " END) AS RetailQuantity,\n" \
                           "(CASE\n" \
                           " WHEN onsale.SalePrice is NULL THEN 0\n" \
                           " ELSE sold.quantity\n" \
                           " END) AS SaleQuantity\n" \
                           " FROM sold\n" \
                           " LEFT OUTER JOIN store ON sold.StoreNumber = store.StoreNumber\n" \
                           " LEFT OUTER JOIN city ON store.CityState = city.CityState\n" \
                           " LEFT OUTER JOIN product ON sold.PID = product.PID\n" \
                           " LEFT OUTER JOIN onsale ON sold.PID = onsale.PID AND sold.CalendarDate = onsale.CalendarDate\n" \
                           " WHERE city.State = '{}'\n" \
                           " ) StoreCityYearQuantity\n" \
                           " GROUP BY StoreCityYearQuantity.StoreNumber, YEAR(StoreCityYearQuantity.CalendarDate)\n" \
                           " ORDER BY Year ASC, TotalRevenue DESC;".format(selected_state)
        return db_query(query)
    
    # the value of the state dropdown (selected by the user)
    selected_state = request.args.get('selected_state', type=str)
    # get values for the table
    updated_values = get_report4_values(selected_state)
    # create the value of the table as a html string
    html_string_selected = """
    <tr>
                    <th>Store Number</th>
                    <th>Street Address</th>
                    <th>City Name</th>
                    <th>Year</th>
                    <th>Total Revenue</th>
    </tr>
    """
    for entry in updated_values:
        html_string_selected += '<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(entry['StoreNumber'],entry['street_address'],entry['CityName'],entry['Year'],entry['TotalRevenue'])

    return jsonify(html_string_selected=html_string_selected)

@app.route('/state_highest_volume_for_each_category')
def state_highest_volume_for_each_category():
    years = db_query("""SELECT DISTINCT YEAR(CalendarDate) AS Year FROM sold""")
    months = db_query("""SELECT DISTINCT MONTH(CalendarDate) AS Month FROM sold""")
    return render_template('state_highest_volume_for_each_category.html', years=years, months=months, content_type='application/json')


def get_report6_values(selected_year,selected_month):
        query = "SELECT Rep6_prelim.CategoryName, Rep6_prelim.State, Rep6_prelim.Quantity\n" \
                           "FROM\n" \
                           "(\n" \
                           "SELECT belongto.CategoryName AS CategoryName, city.State AS State, SUM(sold.quantity) AS Quantity\n" \
                           "FROM sold\n" \
                           "LEFT OUTER JOIN store ON sold.StoreNumber = store.StoreNumber\n" \
                           "LEFT OUTER JOIN city ON store.CityState = city.CityState\n" \
                           "LEFT OUTER JOIN belongto ON sold.PID = belongto.PID\n" \
                           "WHERE YEAR(sold.CalendarDate) = {} AND MONTH(sold.CalendarDate) = {}\n" \
                           "GROUP BY belongto.CategoryName, city.State\n" \
                           ") AS Rep6_prelim\n" \
                           "INNER JOIN\n" \
                           "(\n" \
                           "SELECT CategoryName AS CategoryName, MAX(Quantity) AS Max_Quantity\n" \
                           "FROM \n" \
                           "(\n" \
                           "SELECT belongto.CategoryName AS CategoryName, city.State AS State, SUM(sold.quantity) AS Quantity\n" \
                           "FROM sold\n" \
                           "LEFT OUTER JOIN store ON sold.StoreNumber = store.StoreNumber\n" \
                           "LEFT OUTER JOIN city ON store.CityState = city.CityState\n" \
                           "LEFT OUTER JOIN belongto ON sold.PID = belongto.PID\n" \
                           "WHERE YEAR(sold.CalendarDate) = {} AND MONTH(sold.CalendarDate) = {}\n" \
                           "GROUP BY belongto.CategoryName, city.State\n" \
                           ") As temp\n" \
                           "GROUP BY CategoryName\n" \
                           ") AS Category_MaxQuantity\n" \
                           "ON Rep6_prelim.CategoryName = Category_MaxQuantity.CategoryName \n" \
                           "AND Rep6_prelim.Quantity = Category_MaxQuantity.Max_Quantity\n" \
                           "ORDER BY CategoryName ASC;".format(selected_year,selected_month,selected_year,selected_month)
        return db_query(query)


@app.route('/_update_report6')
def update_report6():
    
    # the value of the year and month dropdown (selected by the user)
    selected_year = request.args.get('selected_year', type=str)
    selected_month = request.args.get('selected_month', type=str)
    # get values for the table
    updated_values = get_report6_values(selected_year,selected_month)
    # create the value of the table as a html string
    html_string_selected = """
    <tr>
                    <th>Category Name</th>
                    <th>State</th>
                    <th>Number of Units Sold</th>
                    <th>Details</th>
    </tr>
    """
    for entry in updated_values:
        html_string_selected += '<tr><td>{}</td><td>{}</td><td>{}\
        <td><button type="button" class="btn btn-primary" data-toggle="modal" data-target="#{}">More</button></td>\
        </tr>'.format(entry['CategoryName'],entry['State'],entry['Quantity'],(entry['CategoryName']+'_'+entry['State']).replace(' ',''))
    return jsonify(html_string_selected=html_string_selected)

@app.route('/_update_report6_modal')
def update_report6_modal():
    # the value of the year and month dropdown (selected by the user)
    selected_year = request.args.get('selected_year', type=str)
    selected_month = request.args.get('selected_month', type=str)
    # get values for the table
    updated_values = get_report6_values(selected_year,selected_month)
    
    modals = ""
    for entry in updated_values:
        modal = """
                        <!-- Modal -->
                        <div class="modal fade" id="{}" tabindex="-1" role="dialog" aria-labelledby="ModalLongTitle" aria-hidden="true">
                          <div class="modal-dialog modal-lg" role="document">
                            <div class="modal-content">
                              <div class="modal-header">
                                <h5 class="modal-title" id="ModalLongTitle"> Sub Report </h5>
                                <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                                  <span aria-hidden="true">&times;</span>
                                </button>
                              </div>
                              <div class="modal-body">
                                  <!-- Header -->
                                    <div class='Header' style="width:100%;margin-left=10%">
                                        <p>
                                            Category     :&nbsp;&nbsp;<b>{}</b><br/>
                                            Year/Month    :&nbsp;&nbsp;<b>{}</b><br/>
                                            State    :&nbsp;&nbsp;<b>{}</b>
                                        </p>    
                                    </div>    

                                    <hr style="color:grey">

                                    <div class="Table" style="width:100%">
                                        <div class="Table-header">
                                            <div class="Cell"><b>Store Number</b></div>
                                            <div class="Cell"><b>Street Address</b></div>
                                            <div class="Cell"><b>City Name</b></div>
                                            <div class="Cell"><b>Manager Name</b></div>
                                            <div class="Cell"><b>Email</b></div>
                                        </div> 
        """.format((entry['CategoryName']+'_'+entry['State']).replace(' ',''),entry['CategoryName'],str(selected_year)+'/'+str(selected_month).zfill(2),entry['State'])

        modal_table = db_query("SELECT DISTINCT store.StoreNumber AS StoreNumber, store.street_address AS street_address, \n" \
                   "city.CityName AS CityName, manager.manager_name AS manager_name, manager.Email AS Email\n" \
                   "FROM sold\n" \
                   "LEFT OUTER JOIN store ON sold.StoreNumber = store.StoreNumber\n" \
                   "LEFT OUTER JOIN city ON store.CityState = city.CityState\n" \
                   "LEFT OUTER JOIN belongto ON sold.PID = belongto.PID\n" \
                   "LEFT OUTER JOIN assignedto ON sold.StoreNumber = assignedto.StoreNumber\n" \
                   "LEFT OUTER JOIN manager ON assignedto.Email = manager.Email\n" \
                   "WHERE city.State = '{}' AND belongto.CategoryName = '{}' \n" \
                   "AND Year(sold.CalendarDate) = {} AND MONTH(sold.CalendarDate) = {}\n" \
                   "AND manager.EmploymentStatus = 1\n" \
                   "ORDER BY StoreNumber ASC;".format(entry['State'],entry['CategoryName'],selected_year,selected_month))
        for entry in modal_table:
            modal += """
                                    <div class="Row">
                                            <div class="Cell">{}</div>
                                            <div class="Cell">{}</div>
                                            <div class="Cell">{}</div>
                                            <div class="Cell">{}</div>
                                            <div class="Cell">{}</div>
                                        </div>

            """.format(entry['StoreNumber'],entry['street_address'],entry['CityName'],entry['manager_name'],entry['Email'])

        modal += """

                              </div>
                              <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
                              </div>
                            </div>
                          </div>
                        </div> 
                    </div>
        """
        modals += modal
    return jsonify(html_string_selected=modals)



@app.route('/revenue_by_population')
def revenue_by_population():
    res = pd.read_sql("SELECT * FROM Report7_Before_Pivot",Database().con).pivot_table(values='Average_Revenue', index=['Year'], columns=['Population_Category'])[['Small', 'Medium', 'Large', 'Extra Large']].reset_index()
    res = list(res.T.to_dict().values())
    return render_template('revenue_by_population.html', result=res, content_type='application/json')

@app.route('/edit_holiday')
def edit_holiday():
    res = db_query("SELECT * FROM `holiday`;")
    return render_template('edit_holiday.html', result=res, content_type='application/json')

@app.route('/edit_holiday',methods=['POST'])
def holiday_form_post():
    # get input text 
    new_date = request.form['new_calendar_date']
    new_name = request.form['new_holiday_name']
    
    sql_stmt = "INSERT INTO `holiday`(CalendarDate, HolidayName)\n" \
                   "VALUES (%s, %s);"
    error = db_execute(sql_stmt, (dt.datetime.strptime(new_date,"%m/%d/%Y"), new_name))
    
    res = db_query("SELECT * FROM `holiday`;")
    return render_template('edit_holiday.html', result=res, error=error, content_type='application/json')
    
@app.route('/edit_manager')
def edit_manager():
    sql_stmt = "SELECT M.Email AS Email, M.manager_name AS manager_name, M.EmploymentStatus AS EmploymentStatus, AT.StoreNumber AS StoreNumber\n" \
                   "FROM (manager AS M LEFT OUTER JOIN assignedto AS AT\n" \
                   "ON M.Email = AT.Email);"
    res = db_query(sql_stmt)
    return render_template('edit_manager.html', result=res, content_type='application/json')

@app.route('/edit_manager',methods=['POST'])
def manager_form_post():
    error = None
    # Add
    if request.form['action'] == 'Add the manager':
        # get input text 
        new_email = request.form["new_manager_email"]
        new_name = request.form["new_manager_name"]
        employment_status = request.form["employment_status"]
        
        sql_stmt = "INSERT INTO `manager`(Email, manager_name, EmploymentStatus)\n" \
                   "VALUES (%s, %s, %s);"
        error = db_execute(sql_stmt, (new_email, new_name, employment_status))
    # Remove
    elif request.form['action'] == 'Remove Selected Manager':
        email2remove = str(request.form.get("manager-select")).split('_')[0]
        assignedto_list = [i['Email'] for i in db_query("SELECT * FROM `assignedto`")]
        
        if email2remove in assignedto_list:
            error = "Error! This manager is still assigned! Please unassign him first!"
        else:
            sql_stmt = "DELETE FROM `manager`\n" \
                       "WHERE Email = %s;"
            error = db_execute(sql_stmt, (email2remove))
            
    # Assign
    elif request.form['action'] == 'Assign/Unassign the manager':
        email2assign = str(request.form.get("manager-select")).split('_')[0]
        if str(request.form.get("manager-assign")) == "Assign":
            store2assign = int(request.form["new_store"])
            sql_stmt = "INSERT INTO `assignedto`(Email, StoreNumber)\n" \
                   "VALUES (%s, %s);"
            error = db_execute(sql_stmt, (email2assign,store2assign))
        elif str(request.form.get("manager-assign")) == "Unassign":
            store2unassign = int(str(request.form.get("manager-select")).split('_')[1])
            sql_stmt = "DELETE FROM `assignedto`\n" \
                   "WHERE Email = %s AND StoreNumber = %s;"
            error = db_execute(sql_stmt, (email2assign,store2unassign))
    # Activate
    elif request.form['action'] == 'Activate/Deactivate the manager':
        email2activate = str(request.form.get("manager-select")).split('_')[0]
        if str(request.form.get("manager-activate")) == "Activate":
            sql_stmt = "UPDATE `manager`\n" \
                   "SET EmploymentStatus = 1\n" \
                   "WHERE Email = %s;"
            error = db_execute(sql_stmt, (email2activate))
        elif str(request.form.get("manager-activate")) == "Deactivate":
            sql_stmt = "UPDATE `manager`\n" \
                   "SET EmploymentStatus = 0\n" \
                   "WHERE Email = %s;"
            error = db_execute(sql_stmt, (email2activate))
            
    sql_stmt = "SELECT M.Email AS Email, M.manager_name AS manager_name, M.EmploymentStatus AS EmploymentStatus, AT.StoreNumber AS StoreNumber\n" \
                   "FROM (manager AS M LEFT OUTER JOIN assignedto AS AT\n" \
                   "ON M.Email = AT.Email);"
    res = db_query(sql_stmt)
    
    return render_template('edit_manager.html', result=res, error=error, content_type='application/json')


@app.route('/edit_population')
def edit_population():
    sql_stmt = "SELECT CityName, State, population, city_size\n" \
                   "FROM city;"
    res = db_query(sql_stmt)
    return render_template('edit_population.html', result=res, content_type='application/json')

@app.route('/edit_population',methods=['POST'])
def population_form_post():
    # get input text 
    population_int = int(request.form['new_population'])
    # checkbox
    city2chg = str(request.form.get("city-select")).split('_')[0]
    state2chg = str(request.form.get("city-select")).split('_')[1]
    
    sql_stmt = "UPDATE `city`\n" \
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
        
    error = db_execute(sql_stmt, (population_int, city_size, city2chg, state2chg))
    
    res = db_query("SELECT CityName, State, population, city_size FROM city;")
    return render_template('edit_population.html', result=res, error=error, content_type='application/json')


if __name__=='__main__':
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='0.0.0.0',debug=True)









