module GoodData
  module Connectors
    module Ads
      class ImportDeletedDataForInputTask < Task

        def initialize(entity)
          super("Import Deleted Data For Imput Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id + "_deleted"
          input["fields"] = []
          input["fields"] << @entity.custom["id"]
          input["computed_fields"] = []
          input["computed_fields"] = Helper.computed_fields(@entity,true,true)
          input["filename"] = File.expand_path(@entity.runtime["deleted_parsed_filename"])
          input["exception_filename"] = File.expand_path("output/#{@entity.id}_deleted_exception.csv")
          input["rejected_filename"] = File.expand_path("output/#{@entity.id}_deleted_rejected.csv")
          @sql = Base::Templates.make("copy_from_local",input)
        end

      end
    end
  end
end
