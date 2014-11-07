module GoodData
  module Connectors
    module Ads
      class DropTableForInputTask < Task

        def initialize(entity)
          super("Drop Table For Input",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id
          @sql = Base::Templates.make("drop_table",input)
        end

      end
    end
  end
end
