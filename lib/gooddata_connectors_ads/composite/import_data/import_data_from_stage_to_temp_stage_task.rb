module GoodData
  module Connectors
    module Ads
      class ImportDataFromStageToTempStageTask < Task

        def initialize(entity)
          super("Import Data From Stage To Temp Stage Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["stage_table_name"] = "temp_" + @entity.id + "_stage"
          input["table_name"] = @entity.id
          input["fields"] = []
          input["fields"] << @entity.get_enabled_fields.find_all{|v| v != @entity.custom["timestamp"]}
          input["metadata_fields"] = Helper.computed_fields(@entity,true,false).map{|v| v["name"]}
          input["metadata_declaration"] = Helper.computed_fields(@entity,true,false)
          input["metadata_timestamp"] = Helper.DEFAULT_META_COMPUTED_FIELDS.find{|v| v["function"] == "timestamp"}["name"]
          @sql = Base::Templates.make("last_from_stage_to_temp_stage",input)
        end

      end
    end
  end
end
