module GoodData
  module Connectors
    module Ads
      class CreateDeletedTableForInputTask < Task

        def initialize(entity)
          super("Create Deleted Table For Input",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id + "_deleted"
          fields = []
          fields << {"name" => @entity.custom["id"], "type" => @entity.get_field(@entity.custom["id"]).type }
          fields += Helper.computed_fields(@entity,true,true)
          input["fields"] = fields
          @sql = Base::Templates.make("create_table",input)
        end
      end
    end
  end
end
