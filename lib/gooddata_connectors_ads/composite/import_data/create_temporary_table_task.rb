module GoodData
  module Connectors
    module Ads
      class CreateTemporaryTableTask < Task

        def initialize(entity)
          super("Create Temporary Table Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temporary_" + @entity.id
          input["temporary"] = true
          input["preserve_rows"] = true
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          @entity.get_enabled_fields_objects.each do |v|
            if (v.id != @entity.custom["timestamp"])
              input["fields"] << {"name" => v.id, "type" => v.type}
            end
          end
          input["fields"] += Helper.computed_fields(@entity,true)
          @sql = Base::Templates.make("create_table",input)
        end

      end
    end
  end
end
