CREATE TABLE [Config].[ProjectTopic] (
    [Code]        VARCHAR (50)   NOT NULL,
    [Name]        NVARCHAR (100) NOT NULL,
    [ProjectCode] VARCHAR (50)   NOT NULL,
    [Description] NVARCHAR (500) NULL,
    [IsActive]    BIT            NOT NULL,
    [CreatedDate] DATETIME2 (6)  NOT NULL,
    [CreatedBy]   NVARCHAR (100) NOT NULL,
    PRIMARY KEY CLUSTERED ([Code] ASC),
    CONSTRAINT [FK_ProjectTopic_Project] FOREIGN KEY ([ProjectCode]) REFERENCES [Config].[Project] ([Code])
);


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique identifier for the topic within a project.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'Code';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'User who created the topic record.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'CreatedBy';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Date and time when the topic record was created.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'CreatedDate';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Optional description of the topic.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'Description';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Flag indicating whether the topic is active.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'IsActive';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Human-friendly name for the topic.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'Name';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Foreign key to the project the topic belongs to.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic', @level2type = N'COLUMN', @level2name = N'ProjectCode';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Topics or functional groups within a project.', @level0type = N'SCHEMA', @level0name = N'Config', @level1type = N'TABLE', @level1name = N'ProjectTopic';


GO

