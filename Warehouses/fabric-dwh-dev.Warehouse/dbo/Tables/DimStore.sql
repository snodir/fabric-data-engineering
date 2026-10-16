CREATE TABLE [dbo].[DimStore] (
    [StoreKey]  INT           NOT NULL,
    [StoreID]   VARCHAR (50)  NULL,
    [StoreName] VARCHAR (100) NULL,
    [Region]    VARCHAR (50)  NULL
);


GO

ALTER TABLE [dbo].[DimStore]
    ADD CONSTRAINT [PK_DimStore] PRIMARY KEY NONCLUSTERED ([StoreKey] ASC) NOT ENFORCED;


GO