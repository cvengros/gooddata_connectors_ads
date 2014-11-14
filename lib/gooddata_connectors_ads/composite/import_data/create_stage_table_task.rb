module GoodData
  module Connectors
    module Ads
      class CreateStageTableTask < Task

        def initialize(entity)
          super("Create Stage Table Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id + "_stage"
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          @entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != @entity.custom["timestamp"])
              input["fields"] << {"name" => v.id, "type" => v.type}
            end
          end
          input["fields"] += Helper.computed_fields(@entity,true)
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
