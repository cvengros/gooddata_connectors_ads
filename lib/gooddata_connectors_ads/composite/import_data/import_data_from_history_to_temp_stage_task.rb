module GoodData
  module Connectors
    module Ads
      class ImportDataFromHistoryToTempStageTask < Task

        def initialize(entity)
          super("Import Data From History To Temp Stage Task",entity)
        end

        def create_command
          # Import data from history tables
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["stage_table_name"] = "temp_" + @entity.id + "_stage"
          input["table_name"] = "temp_" + @entity.id + "_history"
          input["id"] = @entity.custom["id"]
          input["history_id"] = GoodData::Connectors::Base::Global::HISTORY_ID
          input["history_timestamp"] = GoodData::Connectors::Base::Global::HISTORY_TIMESTAMP
          # TO-DO CHANGE This
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          @entity.get_enabled_fields_objects.each do |v|
            if (v.id != @entity.custom["timestamp"] and v.id != @entity.custom["id"])
              input["fields"] << {"name" => v.id, "type" => v.type}
            end
          end
          input["metadata_fields"] = []
          input["metadata_fields"] << Helper.computed_fields(@entity,true,true).map{|v| v["name"]}
          input["metadata_declaration"] = Helper.computed_fields(@entity,true,true)
          input["metadata_timestamp"] = Helper.DEFAULT_META_COMPUTED_FIELDS.find{|v| v["function"] == "timestamp"}["name"]
          @sql = Base::Templates.make("history_to_temp_stage",input)
        end

      end
    end
  end
end
