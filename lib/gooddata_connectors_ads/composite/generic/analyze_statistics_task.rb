module GoodData
  module Connectors
    module Ads
      class AnalyzeStatisticsTask < Task

        def initialize(entity)
          super("Analyze Statistics Task",entity)

        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = @entity.id
          @sql = Base::Templates.make("analyze_statistics",input)
        end
      end
    end
  end
end
