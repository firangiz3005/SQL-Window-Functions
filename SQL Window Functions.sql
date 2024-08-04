
--Aggregate Window Functions (count, sum, avg, min, max)
--count() use cases:
--1. Overall analysis: Quick summary of entire dataset
select 
	count(*) TotalOrders
from sales.orders


--2. Category analysis: Total per groups, to understand patterns within different categories
--Find total number of orders, 
--Total number of orders for each customer, 
--Additionally provide details such as oderId, orderDate

select 
	orderId, 
	orderDate, 
	customerID,
	count(*) over() TotalOrders,
	count(*) over(partition by customerId) OrdersByCustomer
from sales.orders


--3. Quality checks: Identify nulls
--Find total number of customers 
--Find total number of scores for the customers
--Additionally provide All Customers details
select *,
	count(*) over() TotalCustomers,
	count(coalesce(score,0)) over() TotalNumberOfScores
from sales.customers

--4. Quality checks: Identify duplicates
--Check whether the table 'Orders" contains any duplicate rows
select*
from
	(select orderID,
	count(*) over(partition by orderId) CheckPk
	from sales.orders) t
where checkPk>1

--Sum() use cases:
--1. Overall analysis: Quick summary of entire dataset
select 
	sum(sales) over() TotalSales
from sales.orders

--2. Category analysis: Total per groups, to understand patterns within different categories
--Find total sales across all orders, 
--Total sales for each product, 
--Additionally provide details such as orderId, orderDate
select 
	orderid, 
	orderdate, 
	sales,
	sum(sales) over() totalsales,
	sum(sales) over(partition by productId) SalesByProducts
from sales.orders


--3. Part-to-whole: Shows contribution of each data point to the overall dataset
--Find the percentage contribution of each product's sales to the total sales 

select 
	orderId,
	productId,
	sales,
	sum(sales) over() TotalSales,
	round(cast(Sales as float)/sum(sales) over() * 100,2) PercentageOfTotal
from sales.orders


--Avg() use cases:
--1. Overall analysis: Quick summary of entire dataset
select 
	avg(coalesce(sales,0)) over() AvgSales
from sales.orders

--2. Category analysis: Total per groups, to understand patterns within different categories
--Find avg sales across all orders, 
--Avg sales for each product, 
--Additionally provide details such as orderId, orderDate
select 
	orderid, 
	orderdate, 
	sales,
	avg(sales) over() AvgSales,
	avg(sales) over(partition by productId) AvgSalesByProducts
from sales.orders


--3. Compare to average: Help to evaluate whether a value is above or below the average
--Find all orders where sales are higher than the average sales across all orders 

select*
from (
	select 
		orderId,
		productId,
		sales,
		avg(sales) over() AvgSales
	from sales.orders
	) t
	where Sales > AvgSales


--Handling NULL values
--Find the average scores of customers
--Additionally provide details such as CustomerId and LastName
select 
	customerId, LastName,
	Score,
	coalesce(score,0) CustomerScore,
	avg(score) over() AvgScore,
	avg(coalesce(score,0)) over() AvgScoreWithoutNull
from sales.customers



--MAx() and Min() use cases:
--1. Overall analysis: Quick summary of entire dataset
--Find the highest and lowest sales across all orders,  
--Additionally provide details such as orderId, orderDate
select 
	orderId,
	orderDate,
	productId,
	sales,
	min(sales) over() LowestSales,
	max(sales) over() HighestSales
from sales.orders

--2. Category analysis: Total per groups, to understand patterns within different categories
--Find the highest and lowest sales for each product,
--Additionally provide details such as orderId, orderDate
select 
	orderId,
	orderDate,
	productId,
	sales,
	min(sales) over(partition by productId) LowestSalesByProduct,
	max(sales) over(partition by productId) HighestSalesByProduct
from sales.orders

--Show the employees who have the highest salaries
select*
from (
	select*,
	max(salary) over() HighestSalary
	from sales.Employees
	) t
where salary=HighestSalary

--Compare to extremes (outlier detection)
--Find the deviation of each sales from the maximum and minimum sales amounts
select 
	orderId,
	orderDate,
	productId,
	sales,
	min(sales) over() LowestSales,
	max(sales) over() HighestSales,
	Sales-Min(Sales) over() DeviationFromMin,
	Max(sales) over() - Sales DeviationFromMax
from sales.orders

--Running total
--Calculate moving average of sales for each product over time
--Calculate moving average of sales for each product over time, including only the next order
select 
	orderId,
	orderDate,
	productId,
	sales,
	Avg(sales) over(partition by productId) AvgByProduct,
	Avg(sales) over(partition by productId order by OrderDate) MovingAvg,
	Avg(sales) over(partition by productId order by OrderDate Rows between Current Row and 1 Following) RollingAvg
from sales.orders




--Ranking Window Functions 
--Integer Based Ranking (row_number(), rank(), dense_rank(), ntile())
--Distribution Analysis (cume_dist, percent_rank)

--Rank orders based on their sales from highest to lowest using row_number(), rank(), dense_rank()
select 
	orderId,
	productId,
	sales,
	row_number() over(order by sales desc) SalesRank_Row,
	rank() over(order by sales desc) SalesRank_Rank,
	dense_rank() over(order by sales desc) SalesRank_Dense
from sales.orders

--Row_Number() use cases: 
--1. Top-N analysis
--Find the top highest sales for each product
select *
from (
	select 
		orderId,
		productId,
		sales,
		row_number() over(partition by productId order by sales desc) RankByProduct
	from sales.orders) t
where RankByProduct=1

--2. Bottom-N analysis
--Find the lowest 2 customers based on their total sales
select *
from (
	select 
		customerId,
		sum(sales) TotalSales,
		row_number() over(order by sum(sales) desc) RankCustomers
	from sales.orders
	group by customerId) t
where RankCustomers <= 2

--3. Assign uniqe ID
--Assign unique Ids to the rows of the "Orders Archive" table
select
	row_number() over(order by orderId, orderDate) uniqueId,
	*
from sales.orders

--3. Identify duplicates
--Identify duplicate rows in the table "Orders archive"
--and return a clean result without any duplicates
select *
from (
	select 
	row_number() over (partition by orderId order by creationTime desc) rn,
	*
	from sales.OrdersArchive) t
where rn=1

--identify duplicate rows in the table orders archive and return a clean result without any duplicates
With dupRows AS 
(
select orderId, 
row_number() over(partition by orderId order by orderId) dupRowCount
from sales.OrdersArchive
) 
delete 
from dupRows
where dupRowCount>1


--Ntile() use cases: 
--1. Data segmentation
--Segment all data into 3 categories: high, medium, low

select *,
case when Buckets = 1 then 'High'
	 when Buckets = 2 then 'Medium'
	 when Buckets = 3 then 'Low'
End SalesSegmentations
from (
	select 
		orderId,
		sales,
		ntile(3) over(order by sales desc) Buckets
	from sales.orders) t


--Distibution Analysis (cume_dist, percent_rank)
--Cume_dist use case:
--Find the products that fall within the highest 40 % of the prices
select*,
concat (DistRank*100, '%') DistRankCume_Dist
from (
	select 
	product,
	price,
	cume_dist() over (order by price desc) DistRank,
	percent_rank() over (order by price desc) DistRankPerc
	from sales.products) t
where DistRank <=0.4





--Value Window Functions (lead, lag, first_value, last_value)








--Time series analysis 
--Year-over-Year (YoY) analyze the overall growth or decline of the business's performance over time
--Month-over-Month (MoM) analyze short-term trends and discover patterns in seasonality
--Analyse month-over-month performance by finding the percentage change in sales between the current and the previous month
select *,
CurrentMnthSales-previousMonthSales as MoM_change,
concat(coalesce(round(cast((CurrentMnthSales-previousMonthSales) as float)/previousMonthSales*100,2),0),'%') as MoM_perct
from(
select month(orderDate) MonthOrder, 
sum(sales) CurrentMnthSales,
lag(sum(sales)) over(order by month(orderDate)) previousMonthSales
from sales.orders
group by month(orderDate)) t

--Customer Retention Analysis
--Analyze customer loyalty ,
--Rank customers based on the avergae days between their orders
select 
customerId,
Avg(DaysUntilNextOrder) AvgDays,
Rank() over(order by coalesce(Avg(DaysUntilNextOrder),9999999)) RankAvg
from (
	select 
	orderId,
	customerId,
	orderDate CurrentOrder,
	lead(orderDate) over(partition by customerId order by OrderDate) NextOrder,
	datediff(day, orderDate,lead(orderDate) over(partition by customerId order by OrderDate)) DaysUntilNextOrder
	from sales.orders
	) t
group by customerId

--First_Value(), Last_Value() use case:
--Compare to Extremes: how well the value is performing relative to the extremes
--Find the lowest and highest sales for each product
--Find the difference in sales between the current and the lowest sales

select 
	orderId,
	productId,
	sales,
	first_value(sales) over(partition by productId order by Sales) LowestSales,
	last_value(sales) over(partition by productId order by Sales Rows between Current Row and Unbounded Following) HighestSales,
	first_value(sales) over(partition by productId order by Sales desc) HighestSales2,
	Min(Sales) over (partition by productId) LowestSales2,
	Max(Sales) over (partition by productId) HighestSales3
from sales.orders

--Find the difference in sales between the current and the lowest sales
select 
	orderId,
	productId,
	sales,
	first_value(sales) over(partition by productId order by Sales) LowestSales,
	last_value(sales) over(partition by productId order by Sales Rows between Current Row and Unbounded Following) HighestSales,
	Sales - first_value(sales) over(partition by productId order by Sales) as SalesDifference
from sales.orders



