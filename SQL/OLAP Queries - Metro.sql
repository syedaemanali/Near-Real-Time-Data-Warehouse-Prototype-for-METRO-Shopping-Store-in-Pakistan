/*Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
Find the top 5 products that generated the highest revenue, separated by weekday and weekend
sales, with results grouped by month for a specified year.*/

SELECT t.month, 
       p.Product_Name AS 'Revenue Generating Products On Weekend', 
       SUM(s.Total_revenue) AS 'Total Revenue'
FROM sales_fact s
INNER JOIN timedimension t ON s.time_ID = t.time_ID
INNER JOIN products p ON p.Product_ID = s.Product_ID
WHERE t.is_weekend = TRUE AND t.year=2019
GROUP BY t.month, p.Product_Name
ORDER BY SUM(s.Total_revenue) DESC
LIMIT 5;


SELECT t.month, 
       p.Product_Name AS 'Revenue Generating Products On Weekdays', 
       SUM(s.Total_revenue) AS 'Total Revenue'
FROM sales_fact s
INNER JOIN timedimension t ON s.time_ID = t.time_ID
INNER JOIN products p ON p.Product_ID = s.Product_ID
WHERE t.is_weekend = false AND t.year=2019
GROUP BY t.month, p.Product_Name
ORDER BY SUM(s.Total_revenue) DESC
LIMIT 5;


/*Q2*/

WITH RevenueData AS (
    SELECT 
        st.Store_ID,
        st.Store_Name,
        t.Quarter,
        SUM(s.total_Revenue) AS total_revenue
    FROM 
        Store st
    INNER JOIN 
        Sales_Fact s ON st.Store_ID = s.Store_ID
    INNER JOIN 
        Timedimension t ON s.Time_ID = t.Time_ID
    WHERE 
        t.year = 2019
    GROUP BY 
        st.Store_ID, st.Store_Name, t.Quarter
),
QuarterMapping AS (
    SELECT DISTINCT 
        Quarter,
        LEAD(Quarter) OVER (ORDER BY Quarter) AS next_quarter
    FROM Timedimension
    WHERE year = 2019
)
SELECT 
    rd.Store_ID,
    rd.Store_Name,
    rd.Quarter,
    rd.total_revenue,
    prev.total_revenue AS previous_quarter_revenue,
    CASE 
        WHEN prev.total_revenue IS NULL THEN NULL
        ELSE ROUND((rd.total_revenue - prev.total_revenue) / prev.total_revenue * 100, 2)
    END AS revenue_growth_rate
FROM 
    RevenueData rd
INNER JOIN 
    QuarterMapping qm ON rd.Quarter = qm.Quarter
INNER JOIN 
    RevenueData prev ON rd.Store_ID = prev.Store_ID AND prev.Quarter = qm.next_quarter
ORDER BY 
    rd.Store_ID, rd.Quarter;


/*Q3. Detailed Supplier Sales Contribution by Store and Product Name
For each store, show the total sales contribution of each supplier broken down by product name. The
output should group results by store, then supplier, and then product name under each supplier.*/
SELECT
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name,
    SUM(p.price) AS TotalSales
FROM
    Sales_Fact s
    INNER JOIN Store st ON st.Store_ID = s.Store_ID
    INNER JOIN Products p ON s.Product_ID = p.Product_ID
    INNER JOIN Supplier sp on sp.Supplier_ID=s.Supplier_ID
GROUP BY
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name
ORDER BY
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name DESC;




/*Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall,
Winter). This can help understand product performance across seasonal periods.*/
SELECT 
    p.Product_Name,
    CASE 
        WHEN MONTH(t.Order_Date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(t.Order_Date) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(t.Order_Date) IN (9, 10, 11) THEN 'Fall'
        WHEN MONTH(t.Order_Date) IN (12, 1, 2) THEN 'Winter'
    END AS Season,
    SUM(s.Total_Revenue) AS Total_Sales
FROM Sales_Fact s
INNER JOIN products p ON p.Product_ID=s.Product_ID
INNER JOIN TimeDimension t ON s.Time_ID = t.Time_ID
GROUP BY p.Product_Name, 
         CASE 
             WHEN MONTH(t.Order_Date) IN (3, 4, 5) THEN 'Spring'
             WHEN MONTH(t.Order_Date) IN (6, 7, 8) THEN 'Summer'
             WHEN MONTH(t.Order_Date) IN (9, 10, 11) THEN 'Fall'
             WHEN MONTH(t.Order_Date) IN (12, 1, 2) THEN 'Winter'
         END
ORDER BY p.Product_Name;

SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));


/*Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
Calculate the month-to-month revenue volatility for each store and supplier pair. Volatility can be
defined as the percentage change in revenue from one month to the next, helping identify stores
or suppliers with highly fluctuating sales.*/
WITH Previous_Month_Revenue AS (
    SELECT 
        sf.Store_ID,
        sf.Supplier_ID,
        td.Year,
        td.Month,
        SUM(sf.Total_Revenue) AS Prev_Month_Revenue
    FROM Sales_Fact sf
    INNER JOIN TimeDimension td ON sf.Time_ID = td.Time_ID
    WHERE (td.Year < (SELECT MAX(Year) FROM TimeDimension) OR 
           (td.Year = (SELECT MAX(Year) FROM TimeDimension) AND td.Month < (SELECT MAX(Month) FROM TimeDimension)))
    GROUP BY sf.Store_ID, sf.Supplier_ID, td.Year, td.Month
)
SELECT 
    st.Store_Name, 
    sup.Supplier_Name, 
    td.Year, 
    td.Month, 
    SUM(sf.Total_Revenue) AS Total_Revenue,
    IFNULL((SUM(sf.Total_Revenue) - pmr.Prev_Month_Revenue) / pmr.Prev_Month_Revenue * 100, 0) AS Revenue_Volatility
FROM 
    Sales_Fact sf
INNER JOIN 
    Store st ON sf.Store_ID = st.Store_ID
INNER JOIN 
    Products p ON sf.Product_ID = p.Product_ID
INNER JOIN 
    Supplier sup ON p.Supplier_ID = sup.Supplier_ID
INNER JOIN 
    TimeDimension td ON sf.Time_ID = td.Time_ID
LEFT JOIN
    Previous_Month_Revenue pmr ON sf.Store_ID = pmr.Store_ID 
    AND sf.Supplier_ID = pmr.Supplier_ID 
    AND td.Year = pmr.Year 
    AND td.Month = pmr.Month
GROUP BY 
    st.Store_Name, 
    sup.Supplier_Name, 
    td.Year, 
    td.Month
ORDER BY 
    st.Store_Name, 
    sup.Supplier_Name, 
    td.Year, 
    td.Month;


/*Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
Identify the top 5 products frequently bought together within a set of orders (i.e., multiple
products purchased in the same transaction). This product affinity analysis could inform potential
product bundling strategies.*/
SELECT 
    prod1.Product_Name AS Product_1, 
    prod2.Product_Name AS Product_2, 
    COUNT(*) AS Frequency
FROM 
    Sales_Fact fact1
INNER JOIN 
    Sales_Fact fact2 
    ON fact1.Order_ID = fact2.Order_ID 
    AND fact1.Product_ID < fact2.Product_ID
INNER JOIN 
    Products prod1 
    ON fact1.Product_ID = prod1.Product_ID
INNER JOIN 
    Products prod2 
    ON fact2.Product_ID = prod2.Product_ID
GROUP BY 
    prod1.Product_Name, 
    prod2.Product_Name
ORDER BY 
    Frequency DESC
LIMIT 5;



/*Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product,
enabling a comprehensive overview from individual product-level details up to total revenue per
store. This query should provide an overview of cumulative and hierarchical sales figures.*/
SELECT 
    Year,
    Store_ID,
    Supplier_ID,
    Product_ID,
    SUM(Total_Revenue) AS Total_Revenue
FROM (
    SELECT 
        YEAR(t.Order_Date) AS Year,
        sf.Store_ID,
        sf.Supplier_ID,
        sf.Product_ID,
        sf.Total_Revenue
    FROM Sales_Fact sf
    JOIN TimeDimension t ON sf.Time_ID = t.Time_ID
) AS DerivedTable
GROUP BY Year, Store_ID, Supplier_ID, Product_ID WITH ROLLUP
ORDER BY Year, Store_ID, Supplier_ID, Product_ID;



/*Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
For each product, calculate the total revenue and quantity sold in the first and second halves of
the year, along with yearly totals. This split-by-time-period analysis can reveal changes in product
popularity or demand over the year.*/
SELECT 
    p.Product_Name,
    SUM(CASE WHEN t.Month <= 6 THEN s.Total_Revenue ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN t.Month <= 6 THEN s.Quantity_Ordered ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN t.Month > 6 THEN s.Total_Revenue ELSE 0 END) AS H2_Revenue,
    SUM(CASE WHEN t.Month > 6 THEN s.Quantity_Ordered ELSE 0 END) AS H2_Quantity,
    SUM(s.Total_Revenue) AS Yearly_Revenue,
    SUM(s.Quantity_Ordered) AS Yearly_Quantity
FROM Sales_Fact s
INNER JOIN Products p ON s.Product_ID = p.Product_ID
INNER JOIN TimeDimension t ON s.Time_ID = t.Time_ID
GROUP BY p.Product_Name,t.Year
ORDER BY p.Product_Name;


/*Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers
Calculate daily average sales for each product and flag days where the sales exceed twice the daily
average by product as potential outliers or spikes. Explain any identified anomalies in the report,
as these may indicate unusual demand events.*/
SELECT 
    p.Product_Name,
    t.Order_Date,
    SUM(s.Total_Revenue) AS Daily_Sales,
    AVG(SUM(s.Total_Revenue)) OVER (PARTITION BY p.Product_ID ORDER BY t.Order_Date) AS Daily_Avg_Sales,
    CASE 
        WHEN SUM(s.Total_Revenue) > 2 * AVG(SUM(s.Total_Revenue)) OVER (PARTITION BY p.Product_ID ORDER BY t.Order_Date)
        THEN 'High Revenue Spike (Outlier)'
        ELSE 'Normal'
    END AS Sales_Status
FROM Sales_Fact s
INNER JOIN Products p ON s.Product_ID = p.Product_ID
INNER JOIN TimeDimension t ON s.Time_ID = t.Time_ID
GROUP BY p.Product_Name, t.Order_Date, p.Product_ID
ORDER BY p.Product_Name, t.Order_Date;


/*Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
Create a view named STORE_QUARTERLY_SALES that aggregates total quarterly sales by store,
ordered by store name. This view allows quick retrieval of store-specific trends across quarters,
significantly improving query performance for regular sales analysis.*/
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.Store_ID,
    st.Store_Name,
    t.Quarter AS Quarter,
    t.Year AS Year,
    SUM(s.Total_Revenue) AS Total_Quarterly_Revenue,
    SUM(s.Quantity_Ordered) AS Total_Quarterly_Quantity
FROM Sales_Fact s
INNER JOIN Store st ON s.Store_ID = st.Store_ID
INNER JOIN TimeDimension t ON s.Time_ID = t.Time_ID
GROUP BY s.Store_ID, st.Store_Name, Quarter, Year
ORDER BY st.Store_Name, Year, Quarter;
 