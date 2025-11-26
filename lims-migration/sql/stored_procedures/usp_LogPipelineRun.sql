/*
    Stored Procedure: Log Pipeline Run
    Logs pipeline execution details for monitoring
*/

CREATE OR ALTER PROCEDURE etl.usp_LogPipelineRun
    @PipelineName VARCHAR(200),
    @RunId VARCHAR(100),
    @TableName VARCHAR(100),
    @RowsCopied BIGINT,
    @DataRead BIGINT,
    @ExecutionDurationSeconds INT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.PipelineRunLog (
        PipelineName,
        RunId,
        TableName,
        Status,
        RowsCopied,
        DataRead,
        ExecutionDurationSeconds,
        StartTime,
        EndTime
    )
    VALUES (
        @PipelineName,
        @RunId,
        @TableName,
        'Success',
        @RowsCopied,
        @DataRead,
        @ExecutionDurationSeconds,
        DATEADD(SECOND, -@ExecutionDurationSeconds, GETDATE()),
        GETDATE()
    );

    -- Update watermark processed row count
    UPDATE etl.Watermark
    SET ProcessedRowCount = ProcessedRowCount + @RowsCopied,
        ModifiedDate = GETDATE()
    WHERE TableName = @TableName;

END;
GO
