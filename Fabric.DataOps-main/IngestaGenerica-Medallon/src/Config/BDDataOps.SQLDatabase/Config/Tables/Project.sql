CREATE TABLE [Config].[Project] (
    [Code]            VARCHAR (50)   NOT NULL,
    [Name]            VARCHAR (200)  NOT NULL,
    [Description]     NVARCHAR (500) NULL,
    [FunctionalOwner] NVARCHAR (100) NULL,
    [TechnicalOwner]  NVARCHAR (100) NULL,
    [TechnicalEmail]  NVARCHAR (200) NULL,
    [FunctionalEmail] NVARCHAR (200) NULL,
    [IsActive]        BIT            NOT NULL,
    [CreatedDate]     DATETIME2 (6)  NOT NULL,
    [CreatedBy]       NVARCHAR (100) NOT NULL,
    PRIMARY KEY CLUSTERED ([Code] ASC)
);


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique project identifier code.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'Code';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'User who created the project record.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'CreatedBy';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Date and time when the project record was created.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'CreatedDate';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Optional project description.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'Description';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contact email for the functional owner.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'FunctionalEmail';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Name of the functional owner for the project.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'FunctionalOwner';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Flag indicating if the project is active.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'IsActive';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Human-friendly project name.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'Name';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contact email for the technical owner.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'TechnicalEmail';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Name of the technical owner for the project.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project', @level2type = N'COLUMN', @level2name = N'TechnicalOwner';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Project catalog for configuration grouping.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Project';


GO

