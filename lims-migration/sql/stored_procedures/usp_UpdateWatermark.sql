/*
    Stored Procedure: Update Watermark
    Updates the watermark for incremental load tracking
*/

CREATE OR ALTER PROCEDURE etl.usp_UpdateWatermark
    @TableName VARCHAR(100),
    @WatermarkValue DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE etl.Watermark
    SET WatermarkValue = @WatermarkValue,
        LastProcessedDate = GETDATE(),
        ModifiedDate = GETDATE()
    WHERE TableName = @TableName;

    -- If no rows updated, insert new record
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO etl.Watermark (TableName, WatermarkValue)
        VALUES (@TableName, @WatermarkValue);
    END

END;
GO
