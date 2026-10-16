CREATE TABLE [dbo].[DimDate] (
    [DateKey]  INT  NOT NULL,
    [FullDate] DATE NULL,
    [Day]      INT  NULL,
    [Month]    INT  NULL,
    [Quarter]  INT  NULL,
    [Year]     INT  NULL
);


GO

ALTER TABLE [dbo].[DimDate]
    ADD CONSTRAINT [PK_DimDate] PRIMARY KEY NONCLUSTERED ([DateKey] ASC) NOT ENFORCED;


GO