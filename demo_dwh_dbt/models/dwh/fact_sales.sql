{{ config(materialized='table') }}
select so.OrderID
    , so.CustomerID
    , so.StoreID
    , sd.ProductID
    , so.OrderDate
    , sd.Quantity
    , sd.TotalPrice as SalesAmount
from {{ source('sales','sales_orders') }} as so
JOIN {{ source('sales','sales_details') }} as sd ON sd.OrderID = so.OrderID