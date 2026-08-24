CREATE TABLE [dbo].[Bk_DataEquivalence] (
    [IdEquivalence] BIGINT         IDENTITY (1, 1) NOT NULL,
    [MappingGroup]  VARCHAR (100)  NOT NULL,
    [SourceValue]   NVARCHAR (500) NOT NULL,
    [TargetValue]   NVARCHAR (500) NOT NULL,
    [LookupType]    VARCHAR (50)   NOT NULL,
    [Priority]      INT            NOT NULL,
    [IsActive]      BIT            NOT NULL,
    [CreatedDate]   DATETIME2 (3)  NOT NULL,
    [CreatedBy]     NVARCHAR (100) NULL
);


GO

