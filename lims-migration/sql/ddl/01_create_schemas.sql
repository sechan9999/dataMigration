/*
    LIMS Analytics Database - Schema Creation
    Creates schemas for organizing database objects
*/

-- Core schemas
CREATE SCHEMA IF NOT EXISTS dim;
GO

CREATE SCHEMA IF NOT EXISTS fact;
GO

CREATE SCHEMA IF NOT EXISTS etl;
GO

CREATE SCHEMA IF NOT EXISTS audit;
GO

PRINT 'Schemas created successfully';
GO
