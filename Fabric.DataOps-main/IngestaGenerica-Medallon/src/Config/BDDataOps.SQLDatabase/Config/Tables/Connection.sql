CREATE TABLE [Config].[Connection] (
    [Code]                 VARCHAR (50)   NOT NULL,
    [Name]                 NVARCHAR (100) NOT NULL,
    [EnvironmentCode]      VARCHAR (50)   NOT NULL,
    [ConnectionType]       NVARCHAR (100) NOT NULL,
    [KeyVaultSecretName]   NVARCHAR (100) NULL,
    [IsActive]             BIT            NOT NULL,
    [CreatedDate]          DATETIME2 (6)  NOT NULL,
    [CreatedBy]            NVARCHAR (100) NOT NULL,
    [ConnectionObjectId]   NVARCHAR (50)  NULL,
    [ConnectionProperties] NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([Code] ASC),
    CONSTRAINT [CK_Connection_Properties_JSON] CHECK (isjson([ConnectionProperties])>(0)),
    CONSTRAINT [FK_Connection_Environment] FOREIGN KEY ([EnvironmentCode]) REFERENCES [Config].[Environment] ([Code])
);


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique identifier for the connection definition.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'Code';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Technical type of connection (e.g. JDBC, ODBC, SQLServer).', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'ConnectionType';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'User who created the connection record.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'CreatedBy';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Date and time when the connection record was created.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'CreatedDate';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Foreign key to the environment where the connection is used.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'EnvironmentCode';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Flag indicating if the connection is active.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'IsActive';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Name of the secret in Key Vault that holds the connection string or credential reference.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'KeyVaultSecretName';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Human-friendly connection name.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection', @level2type = N'COLUMN', @level2name = N'Name';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Connection definitions and credentials reference (Key Vault).', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'Connection';


GO

