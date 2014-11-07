module GoodData
  module Connectors
    module Ads
      class DropDeletedTableForInputTask < Task

        def initialize(entity)
          super("Drop Deleted Table For Input",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id + "_deleted"
          @sql = Base::Templates.make("drop_table",input)
        end

      end
    end
  end
end
