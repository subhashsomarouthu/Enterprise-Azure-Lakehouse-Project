CREATE OR ALTER PROCEDURE metadata.usp_update_adf_watermarks
    @source_system_id NVARCHAR(100),
    @new_watermark_value DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE metadata.adf_watermark_tracking
    SET
        last_watermark_value = @new_watermark_value,
        updated_at = SYSUTCDATETIME()
    WHERE source_system_id = @source_system_id
      AND EXISTS (
          SELECT 1
          FROM metadata.adf_ingestion_tables t
          WHERE t.source_system_id = metadata.adf_watermark_tracking.source_system_id
            AND t.source_schema = metadata.adf_watermark_tracking.source_schema
            AND t.source_table = metadata.adf_watermark_tracking.source_table
            AND t.is_active = 1
      );
END;
GO
