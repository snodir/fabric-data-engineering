CREATE TABLE [dbo].[DimChannel] (
    [ChannelKey]  INT          NOT NULL,
    [ChannelName] VARCHAR (50) NULL
);


GO

ALTER TABLE [dbo].[DimChannel]
    ADD CONSTRAINT [PK_DimChannel] PRIMARY KEY NONCLUSTERED ([ChannelKey] ASC) NOT ENFORCED;


GO