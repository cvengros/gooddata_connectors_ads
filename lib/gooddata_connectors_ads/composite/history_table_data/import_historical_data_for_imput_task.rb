module GoodData
  module Connectors
    module Ads
      class ImportHistoricalDataForInputTask < Task

        def initialize(entity,dependent_entity)
          super("Import Historical Data For Imput Task",entity)
          @dependent_entity = dependent_entity
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id + "_history"
          input["fields"] = GoodData::Connectors::Base::BaseStorage::HISTORY_TABLE.keys
          input["filename"] = File.expand_path(@dependent_entity.runtime["parsed_filename"])
          input["exception_filename"] = File.expand_path("output/exception.csv")
          input["rejected_filename"] = File.expand_path("output/rejected.csv")
          @sql = Base::Templates.make("copy_from_local",input)
        end

      end
    end
  end
end
