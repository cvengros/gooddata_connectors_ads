module GoodData
  module Connectors
    module Ads
      class CreateTableForInputTask < Task

        def initialize(entity)
          super("Create Table For Input",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id
          input["fields"] = @entity.fields.values.find_all{|f| !f.disabled? }.map{|v| {"name" => v.id, "type" => v.type}}
          if (!@entity.custom["computed_id"].nil?)
            input["fields"] << {"name" => "_COMPUTED_ID", "type" => Metadata::BaseType.create("string-255")}
          end
          input["fields"] += Helper.computed_fields(@entity,@history,@default)

          if (!@entity.custom["computed_id"].nil?)
            input["segmented_key"] = "#{@entity.custom["computed_id"]["fields"].join(",")}"
          else
            input["segmented_key"] = @entity.custom["id"]
          end
          @sql = Base::Templates.make("create_table",input)
        end

      end
    end
  end
end
