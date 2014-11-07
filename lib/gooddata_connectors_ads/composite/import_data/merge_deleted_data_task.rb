module GoodData
  module Connectors
    module Ads
      class MergeDeletedDataTask < Task

        def initialize(entity)
          super("Merge Deleted Data Task",entity)
        end

        def create_command
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = @entity.id
          input["temp_table_name"] = "temp_" + @entity.id
          if (!@entity.custom["computed_id"].nil?)
            input["id"] = "_COMPUTED_ID"
          else
            input["id"] = @entity.custom["id"]
          end
          input["fields"] = []
          @entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != @entity.custom["id"])
              input["fields"] << v.id
            end
          end
          input["metadata_declaration"] = Helper.computed_fields(@entity,false)
          @sql = Base::Templates.make("merge_without_history",input)
        end
      end
    end
  end
end
