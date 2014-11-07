module GoodData
  module Connectors
    module Ads
      class ImportDataFromEntityToTempStageTask < Task

        def initialize(entity)
          super("Import Data From Entity To Temp Stage Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["stage_table_name"] = "temp_" + @entity.id + "_stage"
          input["table_name"] = "temp_" + @entity.id
          input["id"] = @entity.custom["id"]
          input["fields"] = []
          # We need to create table with timestamp value, because we don't want to have it in final table
          @entity.get_enabled_fields_objects.each do |v|
            if (v.id != @entity.custom["timestamp"] and v.id != @entity.custom["id"])
              input["fields"] << v.id
            end
          end
          input["metadata_declaration"] = Helper.computed_fields(@entity,true,true)
          @sql = Base::Templates.make("entity_to_temp_stage",input)
        end

      end
    end
  end
end
