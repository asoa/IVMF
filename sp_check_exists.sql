USE [SERVES]
GO

/****** Object:  StoredProcedure [dbo].[check_exists]    Script Date: 7/27/2019 11:28:26 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[check_exists]
    @service_episode_id NVARCHAR(50),
	@do_exists INT OUTPUT

AS
	BEGIN
	SET NOCOUNT ON;
    IF EXISTS (SELECT service_episode_id FROM se_episode WHERE service_episode_id = @service_episode_id)
	SELECT @do_exists = 0
	ELSE SELECT @do_exists = 1
	END
RETURN 
GO


