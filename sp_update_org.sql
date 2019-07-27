USE [SERVES]
GO

/****** Object:  StoredProcedure [dbo].[update_org]    Script Date: 7/27/2019 11:30:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[update_org]
	@service_episode_id varchar(50),
	@organization_name NVARCHAR(100),
	@originating_organization NVARCHAR(100),
	@last_updated DATETIME2,
	@result INT OUTPUT

AS
	BEGIN
		DECLARE @network_episode_id_ INT = NULL  -- pk from se_network_episode that will be fk to se_network_organization

		--get network pk
		SELECT TOP 1 @network_episode_id_ = ne.network_episode_id
		FROM se_episode ep
		INNER JOIN se_network_episode ne
		ON ep.service_episode_id = ne.service_episode_id
		ORDER BY ne.last_updated desc

		INSERT INTO se_network_organization(network_episode_id, organization_name, originating_organization, last_updated)
		VALUES(@network_episode_id_, @organization_name, @originating_organization, @last_updated)

		IF @@ERROR = 0
			SELECT @result = 0
		ELSE
			SELECT @result = 1
	END
RETURN
GO


