module GoodData
  module Connectors
    module Ads
      class IntegrateHistoricalDataTask < Task

        def initialize(entity)
          super("Integrate Historical Data Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["temp_table_name"] = "temporary_" + @entity.id
          input["temp_stage_table_name"] = "temp_" + @entity.id + "_stage"
          input["id"] = @entity.custom["id"]
          # In case that this two values are null - mostly because empty main table
          input["fields"] = []
          @entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != @entity.custom["timestamp"] and v.id != @entity.custom["id"])
              input["fields"] << v.id
            end
          end
          input["metadata_declaration"] = Helper.computed_fields(@entity,true,false)
          input["metadata_declaration_default"] = Helper.computed_fields(@entity,true,true)
          input["metadata_timestamp"] = Helper.computed_fields(@entity,true).find{|v| v["function"] == "timestamp"}["name"]
          @sql = Base::Templates.make("integration",input)
        end

      end
    end
  end
end
