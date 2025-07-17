--Declare an OrderItems Table
DECLARE @MyOrder OrderItemType;

--Add products say productID =1 and Quantity =3, productID =4 and Quantity =2, productID =6 and Quantity =5
--This testing whether system can handle multiple products in a single order.
INSERT INTO @MyOrder(productID, Quantity)
VALUES (1,80),(4,2),(6,5)

--Place the order for CustomerID = 2
--procedure call or usecase for place Order 
EXEC PlaceOrder @CustomerID = 5, @OrderItems = @MyOrder;



--procedure call for replenish low stock 
EXEC ReplenishLowStock;


--procedure call for UpdateCustomerTiers
EXEC UpdateCustomerTiers;

