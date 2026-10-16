CREATE TABLE [dbo].[FactSales] (
    [SalesKey]       INT             NOT NULL,
    [DateKey]        INT             NULL,
    [CustomerKey]    INT             NULL,
    [ProductKey]     INT             NULL,
    [ChannelKey]     INT             NULL,
    [StoreKey]       INT             NULL,
    [OrderQuantity]  INT             NULL,
    [UnitPrice]      DECIMAL (10, 2) NULL,
    [TotalAmount]    DECIMAL (10, 2) NULL,
    [DiscountAmount] DECIMAL (10, 2) NULL
);


GO

ALTER TABLE [dbo].[FactSales]
    ADD CONSTRAINT [FK_FactSales_Channel] FOREIGN KEY ([ChannelKey]) REFERENCES [dbo].[DimChannel] ([ChannelKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactSales]
    ADD CONSTRAINT [FK_FactSales_Customer] FOREIGN KEY ([CustomerKey]) REFERENCES [dbo].[DimCustomer] ([CustomerKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactSales]
    ADD CONSTRAINT [FK_FactSales_Date] FOREIGN KEY ([DateKey]) REFERENCES [dbo].[DimDate] ([DateKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactSales]
    ADD CONSTRAINT [FK_FactSales_Product] FOREIGN KEY ([ProductKey]) REFERENCES [dbo].[DimProduct] ([ProductKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactSales]
    ADD CONSTRAINT [FK_FactSales_Store] FOREIGN KEY ([StoreKey]) REFERENCES [dbo].[DimStore] ([StoreKey]) NOT ENFORCED;


GO