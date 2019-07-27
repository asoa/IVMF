USE [SERVES]
GO

/****** Object:  StoredProcedure [dbo].[check_net_org_same]    Script Date: 7/27/2019 11:29:02 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[check_net_org_same]
	@service_episode_id varchar(50),
    @network_id INT,
	@org_name NVARCHAR(100),
	@result INT OUTPUT

AS
	BEGIN
		SET NOCOUNT ON;
		declare @current_net int
		declare @current_org varchar(100)

		select top 1 @current_net = net.network_id,  @current_org = org.organization_name
		from se_episode se
		inner join se_network_episode net
		on se.service_episode_id = net.service_episode_id
		inner join se_network_organization org
		on net.network_episode_id = org.network_episode_id
		where se.service_episode_id = @service_episode_id -- and net.network_id = @network_id and org.organization_name = @org_name
		order by net.last_updated desc, org.last_updated desc


		begin
			if (@network_id = @current_net and @org_name = @current_org)
				-- net and org are the same
				select @result = 0 
			else if(@network_id <> @current_net and @org_name = @current_org) 
				-- net changed, org same
				select @result = 1
			else if(@network_id = @current_net and @org_name <> @current_org)
				--net same, org changed
				select @result = 2
			else
				--both changed
				select @result = 3
		end
	END
RETURN 
GO


