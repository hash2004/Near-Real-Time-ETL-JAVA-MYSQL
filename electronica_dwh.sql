DROP DATABASE electronica_dw;

create database electronica_dw;
use electronica_dw;

-- Fact Table: OrderFacts
CREATE TABLE OrderFacts (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    ProductID INT,
    CustomerID INT,
    QuantityOrdered INT,
    productPrice DECIMAL(10, 2)
);

-- Dimension Table: ProductDimension
CREATE TABLE ProductDimension (
    ProductID INT PRIMARY KEY,
    productName VARCHAR(255),
    supplierID INT,
    FOREIGN KEY (supplierID) REFERENCES SupplierDimension(supplierID)
);

-- Dimension Table: CustomerDimension
CREATE TABLE CustomerDimension (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10)
);

-- Dimension Table: SupplierDimension
CREATE TABLE SupplierDimension (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(255)
);

-- Dimension Table: StoreDimension
CREATE TABLE StoreDimension (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255)
);
SELECT COUNT(*) AS NumberOfRows
FROM OrderFacts;

SELECT COUNT(*) AS NumberOfRows
FROM SupplierDimension;

SELECT COUNT(*) AS NumberOfRows
FROM CustomerDimension;

SELECT COUNT(*) AS NumberOfRows
FROM StoreDimension;

SELECT COUNT(*) AS NumberOfRows
FROM ProductDimension;
