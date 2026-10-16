CREATE TABLE [dbo].[DimCustomer] (
    [CustomerKey]  INT           NOT NULL,
    [CustomerID]   VARCHAR (50)  NULL,
    [CustomerName] VARCHAR (100) NULL,
    [City]         VARCHAR (50)  NULL,
    [State]        VARCHAR (50)  NULL
);


GO

ALTER TABLE [dbo].[DimCustomer]
    ADD CONSTRAINT [PK_DimCustomer] PRIMARY KEY NONCLUSTERED ([CustomerKey] ASC) NOT ENFORCED;


GO