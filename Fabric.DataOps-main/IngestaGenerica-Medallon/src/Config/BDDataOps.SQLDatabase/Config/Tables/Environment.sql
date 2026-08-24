CREATE TABLE [Config].[Environment] (
    [Code]        VARCHAR (50)   NOT NULL,
    [Name]        NVARCHAR (100) NOT NULL,
    [Description] NVARCHAR (500) NULL,
    [IsActive]    BIT            NOT NULL,
    [CreatedDate] DATETIME2 (6)  NOT NULL,
    [CreatedBy]   NVARCHAR (100) NOT NULL,
    PRIMARY KEY CLUSTERED ([Code] ASC)
);


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique identifier code for the environment.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'Code';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Login or user that created the record.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'CreatedBy';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Date and time when the environment record was created.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'CreatedDate';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Optional description of the environment.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'Description';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Flag that indicates whether the environment is active.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'IsActive';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Human-friendly name for the environment.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment', @level2type = N'COLUMN', @level2name = N'Name';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Configuration table that stores environment definitions (e.g. development, production).', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Environment';


GO

