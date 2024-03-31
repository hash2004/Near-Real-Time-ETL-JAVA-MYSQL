-- Q1 - Present total sales of all products supplied by each supplier with respect to quarter and
-- month using drill down concept.
SELECT 
    p.supplierID,
    s.supplierName,
    YEAR(o.OrderDate) AS Year,
    QUARTER(o.OrderDate) AS Quarter,
    MONTH(o.OrderDate) AS Month,
    SUM(o.QuantityOrdered * o.productPrice) AS TotalSales
FROM 
    OrderFacts o
JOIN 
    ProductDimension p ON o.ProductID = p.ProductID
JOIN 
    SupplierDimension s ON p.supplierID = s.supplierID
GROUP BY 
    p.supplierID,
    s.supplierName,
    YEAR(o.OrderDate),
    QUARTER(o.OrderDate),
    MONTH(o.OrderDate)
ORDER BY 
    s.supplierID,
    Year,
    Quarter,
    Month;

-- Q2 - Find total sales of product with respect to month using feature of rollup on month and
-- feature of dicing on supplier with name "DJI" and Year as "2019". You will use the
-- grouping sets feature to achieve rollup. Your output should be sequentially ordered
-- according to product and month.

SELECT
    P.productName,
    MONTH(O.OrderDate) AS Month,
    YEAR(O.OrderDate) AS Year,
    SUM(O.QuantityOrdered * O.productPrice) AS TotalSales
FROM
    OrderFacts O
    JOIN ProductDimension P ON O.ProductID = P.ProductID
    JOIN SupplierDimension S ON P.supplierID = S.supplierID
WHERE
    S.supplierName = 'DJI'
    AND YEAR(O.OrderDate) = 2019
GROUP BY
    P.productName, YEAR(O.OrderDate), MONTH(O.OrderDate) WITH ROLLUP
ORDER BY
    P.productName, Year, Month;
-- Q3
SELECT
    p.productName,
    SUM(o.QuantityOrdered) AS TotalQuantity
FROM
    OrderFacts o
JOIN
    ProductDimension p ON o.ProductID = p.ProductID
WHERE
    DAYOFWEEK(o.OrderDate) IN (1, 7) -- 1 for Sunday, 7 for Saturday
GROUP BY
    p.productName
ORDER BY
    TotalQuantity DESC
LIMIT 5;


-- Q4 
SELECT
    p.productName,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 1 AND 3 THEN o.QuantityOrdered * o.productPrice ELSE 0 END) AS Q1_Sales,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 4 AND 6 THEN o.QuantityOrdered * o.productPrice ELSE 0 END) AS Q2_Sales,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 7 AND 9 THEN o.QuantityOrdered * o.productPrice ELSE 0 END) AS Q3_Sales,
    SUM(CASE WHEN MONTH(o.OrderDate) BETWEEN 10 AND 12 THEN o.QuantityOrdered * o.productPrice ELSE 0 END) AS Q4_Sales,
    SUM(o.QuantityOrdered * o.productPrice) AS TotalYearlySales
FROM
    OrderFacts o
JOIN
    ProductDimension p ON o.ProductID = p.ProductID
WHERE
    YEAR(o.OrderDate) = 2019
GROUP BY
    p.productName
ORDER BY
    p.productName;
 
-- Q5
SELECT
    p.productName,
    AVG(o.QuantityOrdered * o.productPrice) AS AverageSales,
    MAX(o.QuantityOrdered * o.productPrice) AS MaxSales
FROM
    OrderFacts o
JOIN
    ProductDimension p ON o.ProductID = p.ProductID
GROUP BY
    p.productName
HAVING
    MAX(o.QuantityOrdered * o.productPrice) > 10 * AVG(o.QuantityOrdered * o.productPrice);
/*This query calculates the average sales and maximum sales for each product and identifies products where the
 maximum sales exceed 10 times the average sales. This might indicate potential anomalies where certain products 
 have significantly higher sales compared to their 
typical performance.*/

-- Q6
CREATE VIEW STOREANALYSIS_VIEW AS
SELECT StoreDimension.storeID AS STORE_ID,
       ProductDimension.ProductID AS PROD_ID,
       SUM(OrderFacts.QuantityOrdered * OrderFacts.productPrice) AS STORE_TOTAL
FROM OrderFacts
JOIN ProductDimension ON OrderFacts.ProductID = ProductDimension.ProductID
JOIN CustomerDimension ON OrderFacts.CustomerID = CustomerDimension.CustomerID
JOIN StoreDimension ON ProductDimension.supplierID = StoreDimension.storeID
GROUP BY StoreDimension.storeID, ProductDimension.ProductID;

SELECT * FROM STOREANALYSIS_VIEW;

-- Q7
SELECT MONTH(o.OrderDate) AS Month,
       YEAR(o.OrderDate) AS Year,
       p.productName AS ProductName,
       SUM(o.QuantityOrdered * o.productPrice) AS TotalSales
FROM OrderFacts o
JOIN ProductDimension p ON o.ProductID = p.ProductID
JOIN StoreDimension s ON o.CustomerID = s.storeID
WHERE s.storeName = 'Tech Haven'
GROUP BY YEAR(o.OrderDate), MONTH(o.OrderDate), p.productName
ORDER BY Year, Month, TotalSales DESC;

-- Q8
CREATE VIEW SUPPLIER_PERFORMANCE_VIEW AS
SELECT YEAR(o.OrderDate) AS Year,
       MONTH(o.OrderDate) AS Month,
       p.supplierID,
       s.supplierName,
       SUM(o.QuantityOrdered * o.productPrice) AS TotalSales
FROM OrderFacts o
JOIN ProductDimension p ON o.ProductID = p.ProductID
JOIN SupplierDimension s ON p.supplierID = s.supplierID
GROUP BY YEAR(o.OrderDate), MONTH(o.OrderDate), p.supplierID, s.supplierName;

SELECT * FROM SUPPLIER_PERFORMANCE_VIEW;

-- Q9
SELECT cd.CustomerID, cd.CustomerName, COUNT(DISTINCT ProductID) AS UniqueProductsPurchased
FROM OrderFacts
JOIN CustomerDimension cd ON OrderFacts.CustomerID = cd.CustomerID
WHERE YEAR(OrderFacts.OrderDate) = 2019
GROUP BY cd.CustomerID, cd.CustomerName
ORDER BY UniqueProductsPurchased DESC
LIMIT 5;


-- Q10
CREATE VIEW CUSTOMER_STORE_SALES_MV AS
SELECT 
    s.storeID,
    s.storeName,
    c.CustomerID,
    c.CustomerName,
    YEAR(o.OrderDate) AS OrderYear,
    MONTH(o.OrderDate) AS OrderMonth,
    SUM(o.productPrice * o.QuantityOrdered) AS MonthlySales
FROM 
    OrderFacts o
JOIN 
    ProductDimension p ON o.ProductID = p.ProductID
JOIN 
    SupplierDimension sup ON p.supplierID = sup.supplierID
JOIN 
    StoreDimension s ON sup.supplierID = s.storeID
JOIN 
    CustomerDimension c ON o.CustomerID = c.CustomerID
GROUP BY 
    s.storeID,
    s.storeName,
    c.CustomerID,
    c.CustomerName,
    YEAR(o.OrderDate),
    MONTH(o.OrderDate);

SELECT * FROM CUSTOMER_STORE_SALES_MV;





