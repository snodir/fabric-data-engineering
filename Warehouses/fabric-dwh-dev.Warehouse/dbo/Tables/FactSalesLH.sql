CREATE TABLE [dbo].[FactSalesLH] (
    [SalesKey]    INT             NOT NULL,
    [ProductKey]  INT             NULL,
    [CustomerKey] INT             NULL,
    [OrderDate]   DATE            NULL,
    [UnitsSold]   INT             NULL,
    [Revenue]     DECIMAL (10, 2) NULL
);


GO