CREATE TABLE [dbo].[DimProduct] (
    [ProductKey]  INT           NOT NULL,
    [ProductID]   VARCHAR (50)  NULL,
    [ProductName] VARCHAR (100) NULL,
    [Category]    VARCHAR (50)  NULL
);


GO

ALTER TABLE [dbo].[DimProduct]
    ADD CONSTRAINT [PK_DimProduct] PRIMARY KEY NONCLUSTERED ([ProductKey] ASC) NOT ENFORCED;


GO