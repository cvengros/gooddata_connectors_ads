module GoodData
  module Connectors
    module Ads
      class MergeHistoricalDataTask < Task

        def initialize(entity)
          super("Merge Historical Data Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = @entity.id
          input["temp_table_name"] = "temporary_" + @entity.id
          input["id"] = @entity.custom["id"]

          input["fields"] = []
          @entity.get_enabled_fields_objects.each do |v|
            if (v.id != @entity.custom["timestamp"] and v.id != @entity.custom["id"])
              input["fields"] << v.id
            end
          end
          input["metadata_declaration"] = Helper.computed_fields(@entity,true,false)
          input["metadata_hash"] = "_HASH"
          @sql = Base::Templates.make("merge",input)
        end

      end
    end
  end
end
