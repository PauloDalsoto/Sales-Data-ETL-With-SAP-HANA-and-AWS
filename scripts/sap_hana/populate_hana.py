import sys
from hdbcli import dbapi
import pandas as pd
import json

sap_hana_config_file = "config/hana_cloud_config.json"
with open(sap_hana_config_file) as f:
    sap_hana_config = json.load(f)
    db_url  = sap_hana_config['url']
    db_port = sap_hana_config['port']
    db_user = sap_hana_config['user']
    db_pwd  = sap_hana_config['pwd']


csv_files = {
        'Categories': 'raw_data/categories.csv',
        'Suppliers': 'raw_data/suppliers.csv',
        'Shippers': 'raw_data/shippers.csv',
        'Employees': 'raw_data/employees.csv',
        'Customers': 'raw_data/customers.csv',
        'Products': 'raw_data/products.csv',
        'Orders': 'raw_data/orders.csv',
        'OrderDetails': 'raw_data/orderdetails.csv'
} 

csv_files = {
    'Categories': 'raw_data/categories.csv',
    'Suppliers': 'raw_data/suppliers.csv',
    'Shippers': 'raw_data/shippers.csv',
    'Employees': 'raw_data/employees.csv',
    'Customers': 'raw_data/customers.csv',
    'Products': 'raw_data/products.csv',
    'Orders': 'raw_data/orders.csv',
    'OrderDetails': 'raw_data/orderdetails.csv'
}

try:
    connection = dbapi.connect(
        address=db_url,
        port=db_port,
        user=db_user,
        password=db_pwd
    )
    cursor = connection.cursor()

    print("Inserindo Caregories...")
    df = pd.read_csv(csv_files['Categories'], sep=';', dtype={'CategoryID': int, 'CategoryName': str, 'Description': str})
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Categories" ("CategoryID", "CategoryName", "Description") VALUES (?, ?, ?)',
                (row['CategoryID'], row['CategoryName'], row['Description'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Categories, linha {index}: {e}")
    connection.commit() 

    print("Inserindo Suppliers...")
    df = pd.read_csv(csv_files['Suppliers'], sep=';', dtype={
        'SupplierID': int,
        'CompanyName': str,
        'ContactName': str,
        'ContactTitle': str,
        'Address': str,
        'City': str,
        'Region': str,
        'PostalCode': str,
        'Country': str,
        'Phone': str,
        'Fax': str,
        'HomePage': str
    })

    df.fillna('', inplace=True)

    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Suppliers" ("SupplierID", "CompanyName", "ContactName", "ContactTitle", "Address", "City", "Region", "PostalCode", "Country", "Phone", "Fax", "HomePage") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['SupplierID'], row['CompanyName'], row['ContactName'], row['ContactTitle'], row['Address'], row['City'], row['Region'], row['PostalCode'], row['Country'], row['Phone'], row['Fax'], row['HomePage'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Suppliers, linha {index}: {e}")
    connection.commit()  

    print("Inserindo Shippers...")
    df = pd.read_csv(csv_files['Shippers'], sep=';')
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Shippers" ("ShipperID", "CompanyName", "Phone") VALUES (?, ?, ?)',
                (row['ShipperID'], row['CompanyName'], row['Phone'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Shippers, linha {index}: {e}")
    connection.commit()  

    print("Inserindo Employees...")
    df = pd.read_csv(csv_files['Employees'], sep=';', dtype={
        'EmployeeID': int,
        'LastName': str,
        'FirstName': str,
        'Title': str,
        'TitleOfCourtesy': str,
        'BirthDate': str,
        'HireDate': str,
        'Address': str,
        'City': str,
        'Region': str,
        'PostalCode': str,
        'Country': str,
        'HomePhone': str,
        'Extension': str,
        'Notes': str,
        'ReportsTo': int,
        'PhotoPath': str,
        'Salary': float
    })

    df.fillna({'Salary': 0}, inplace=True) 
    
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Employees" ("EmployeeID", "LastName", "FirstName", "Title", "TitleOfCourtesy", "BirthDate", "HireDate", "Address", "City", "Region", "PostalCode", "Country", "HomePhone", "Extension", "Notes", "ReportsTo", "PhotoPath", "Salary") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['EmployeeID'], row['LastName'], row['FirstName'], row['Title'], row['TitleOfCourtesy'], row['BirthDate'], row['HireDate'], row['Address'], row['City'], row['Region'], row['PostalCode'], row['Country'], row['HomePhone'], row['Extension'], row['Notes'], row['ReportsTo'], row['PhotoPath'], row['Salary'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Employees, linha {index}: {e}")
    connection.commit()  

    print("Inserindo Customers...")
    df = pd.read_csv(csv_files['Customers'], sep=';')
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Customers" ("CustomerID", "CompanyName", "ContactName", "ContactTitle", "Address", "City", "Region", "PostalCode", "Country", "Phone") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['CustomerID'], row['CompanyName'], row['ContactName'], row['ContactTitle'], row['Address'], row['City'], row['Region'], row['PostalCode'], row['Country'], row['Phone'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Customers, linha {index}: {e}")
    connection.commit() 

    print("Inserindo Products...")
    df = pd.read_csv(csv_files['Products'], sep=';')
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Products" ("ProductID", "ProductName", "SupplierID", "CategoryID", "QuantityPerUnit", "UnitPrice", "UnitsInStock", "UnitsOnOrder", "ReorderLevel", "Discontinued") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['ProductID'], row['ProductName'], row['SupplierID'], row['CategoryID'], row['QuantityPerUnit'], row['UnitPrice'], row['UnitsInStock'], row['UnitsOnOrder'], row['ReorderLevel'], row['Discontinued'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Products, linha {index}: {e}")
    connection.commit()

    print("Inserindo Orders...")
    df = pd.read_csv(csv_files['Orders'], sep=';')
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "Orders" ("OrderID", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate", "ShipVia", "Freight", "ShipName", "ShipAddress", "ShipCity", "ShipRegion", "ShipPostalCode", "ShipCountry") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['OrderID'], row['CustomerID'], row['EmployeeID'], row['OrderDate'], row['RequiredDate'], row['ShippedDate'], row['ShipVia'], row['Freight'], row['ShipName'], row['ShipAddress'], row['ShipCity'], row['ShipRegion'], row['ShipPostalCode'], row['ShipCountry'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela Orders, linha {index}: {e}")
    connection.commit()

    print("Inserindo OrderDetails...")
    df = pd.read_csv(csv_files['OrderDetails'], sep=';')
    for index, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO "OrderDetails" ("OrderID", "ProductID", "UnitPrice", "Quantity", "Discount") VALUES (?, ?, ?, ?, ?)',
                (row['OrderID'], row['ProductID'], row['UnitPrice'], row['Quantity'], row['Discount'])
            )
        except Exception as e:
            print(f"Erro ao inserir na tabela OrderDetails, linha {index}: {e}")
    connection.commit()

    print("Todos os dados foram inseridos com sucesso!")

except Exception as e:
    print(f'Erro ao conectar ou consultar: {e}')

finally:
    if connection:
        cursor.close()
        connection.close()