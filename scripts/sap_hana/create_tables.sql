CREATE COLUMN TABLE "Categories" (
    "CategoryID" SMALLINT NOT NULL,
    "CategoryName" VARCHAR(15) NOT NULL,
    "Description" NCLOB 
);

CREATE COLUMN TABLE "Customers" (
    "CustomerID" VARCHAR(40) NOT NULL,
    "CompanyName" VARCHAR(40) NOT NULL,
    "ContactName" VARCHAR(30),
    "ContactTitle" VARCHAR(30),
    "Address" VARCHAR(60),
    "City" VARCHAR(15),
    "Region" VARCHAR(15),
    "PostalCode" VARCHAR(10),
    "Country" VARCHAR(15),
    "Phone" VARCHAR(24)
);

CREATE COLUMN TABLE "Employees" (
    "EmployeeID" SMALLINT NOT NULL,
    "LastName" VARCHAR(20) NOT NULL,
    "FirstName" VARCHAR(10) NOT NULL,
    "Title" VARCHAR(30),
    "TitleOfCourtesy" VARCHAR(25),
    "BirthDate" DATE,
    "HireDate" DATE,
    "Address" VARCHAR(60),
    "City" VARCHAR(15),
    "Region" VARCHAR(15),
    "PostalCode" VARCHAR(10),
    "Country" VARCHAR(15),
    "HomePhone" VARCHAR(24),
    "Extension" VARCHAR(4),
    "Notes" NCLOB, 
    "ReportsTo" SMALLINT,
    "PhotoPath" VARCHAR(255),
    "Salary" REAL 
);

CREATE COLUMN TABLE "OrderDetails" (
    "OrderID" SMALLINT NOT NULL,
    "ProductID" SMALLINT NOT NULL,
    "UnitPrice" REAL NOT NULL,
    "Quantity" SMALLINT NOT NULL,
    "Discount" REAL NOT NULL
);

CREATE COLUMN TABLE "Orders" (
    "OrderID" SMALLINT NOT NULL,
    "CustomerID" VARCHAR(40),
    "EmployeeID" SMALLINT,
    "OrderDate" DATE,
    "RequiredDate" DATE,
    "ShippedDate" DATE,
    "ShipVia" SMALLINT,
    "Freight" REAL,
    "ShipName" VARCHAR(40),
    "ShipAddress" VARCHAR(60),
    "ShipCity" VARCHAR(15),
    "ShipRegion" VARCHAR(15),
    "ShipPostalCode" VARCHAR(10),
    "ShipCountry" VARCHAR(15)
);

CREATE COLUMN TABLE "Products" (
    "ProductID" SMALLINT NOT NULL,
    "ProductName" VARCHAR(40) NOT NULL,
    "SupplierID" SMALLINT,
    "CategoryID" SMALLINT,
    "QuantityPerUnit" VARCHAR(20),
    "UnitPrice" REAL,
    "UnitsInStock" SMALLINT,
    "UnitsOnOrder" SMALLINT,
    "ReorderLevel" SMALLINT,
    "Discontinued" INTEGER NOT NULL
);

CREATE COLUMN TABLE "Shippers" (
    "ShipperID" SMALLINT NOT NULL,
    "CompanyName" VARCHAR(40) NOT NULL,
    "Phone" VARCHAR(24)
);

CREATE COLUMN TABLE "Suppliers" (
    "SupplierID" SMALLINT NOT NULL,
    "CompanyName" VARCHAR(40) NOT NULL,
    "ContactName" VARCHAR(30),
    "ContactTitle" VARCHAR(30),
    "Address" VARCHAR(60),
    "City" VARCHAR(15),
    "Region" VARCHAR(15),
    "PostalCode" VARCHAR(10),
    "Country" VARCHAR(15),
    "Phone" VARCHAR(24),
    "Fax" VARCHAR(24),
    "HomePage" NCLOB 
);
