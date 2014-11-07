module GoodData
  module Connectors
    module Ads
      class CreateHistoryForInputTask < Task

        def initialize(entity)
          super("Create History For Input",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_#{@entity.id}_history"
          input["fields"] = GoodData::Connectors::Base::BaseStorage::HISTORY_TABLE.map{|k,v| {"name" => k, "type" => Metadata::BaseType.create(v)} }
          @sql = Base::Templates.make("create_table",input)
        end
      end
    end
  end
end
