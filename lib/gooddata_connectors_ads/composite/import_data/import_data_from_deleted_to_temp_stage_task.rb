module GoodData
  module Connectors
    module Ads
      class ImportDataFromDeletedToTempStageTask < Task

        def initialize(entity)
          super("Import Data From Deleted To Temp Stage Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["stage_table_name"] = "temp_" + @entity.id + "_stage"
          input["table_name"] = "temp_" + @entity.id + "_deleted"
          input["id"] = @entity.custom["id"]
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["metadata_declaration"] = []
          input["metadata_declaration"] << Helper.computed_fields(@entity,true).find{|v| v["function"] == "timestamp"}
          input["metadata_declaration"] << Helper.computed_fields(@entity,true).find{|v| v["function"] == "is_deleted"}
          @sql = Base::Templates.make("entity_to_temp_stage",input)
        end

      end
    end
  end
end
