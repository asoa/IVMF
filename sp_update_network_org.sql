USE [SERVES]
GO

/****** Object:  StoredProcedure [dbo].[update_network_org]    Script Date: 7/27/2019 11:29:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[update_network_org]
	@service_episode_id varchar(50),
    @network_id INT,
	@originalnetwork INT,
	@organization_name NVARCHAR(100),
	@originating_organization NVARCHAR(100),
	@last_updated DATETIME2,
	@result INT OUTPUT

AS
	BEGIN
		DECLARE @network_episode_id INT = NULL

		--update network
		INSERT INTO se_network_episode(service_episode_id, network_id, originalnetwork, last_updated)
		VALUES(@service_episode_id, @network_id, @originalnetwork, @last_updated)
		SET @network_episode_id = SCOPE_IDENTITY()

		
		--update organization
		INSERT INTO se_network_organization(network_episode_id, organization_name, originating_organization, last_updated)
		VALUES(@network_episode_id, @organization_name, @originating_organization, @last_updated)
		
		IF @@ERROR = 0
			SELECT @result = 0
		ELSE
			SELECT @result = 1
	END
RETURN
GO


