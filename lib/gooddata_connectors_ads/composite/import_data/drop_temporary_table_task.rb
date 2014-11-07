module GoodData
  module Connectors
    module Ads
      class DropTemporaryTableTask < Task

        def initialize(entity)
          super("Drop Temporary Table Task",entity)
        end

        def create_command
          # LETS INTEGRATE - this is multiple query select run as one query
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temporary_" + @entity.id
          @sql = Base::Templates.make("drop_table",input)
        end

      end
    end
  end
end
