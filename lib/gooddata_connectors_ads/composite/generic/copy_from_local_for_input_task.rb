module GoodData
  module Connectors
    module Ads
      class CopyFromLocalForInputTask < Task

        def initialize(entity,file,index)
          super("Copy From Local For Input",entity)
          @file = file
          @index = index
        end

        def create_command
          # $log.info "Processing file #{file} (#{index}) - COPY FROM LOCAL"
          input = {}
          input["schema"] = Helper.get_configuration_by_type_and_key("instance_id")
          input["table_name"] = "temp_" + @entity.id
          input["timestamp"] = @entity.custom["timestamp"]
          input["is_deleted"] = "default"
          input["fields"] =  @entity.get_enabled_fields
          input["filename"] = File.expand_path(@file)
          if (!@entity.custom["computed_id"].nil?)
            elements = []
            entity.custom["computed_id"]["fields"].each do |field|
              elements << "COALESCE((#{field})::VARCHAR(255),'')"
            end
            input["computed_id"] = "MD5(#{elements.join(" || ")})"
          end
          input["computed_fields"] = Helper.computed_fields(@entity,@history,@default,{"file" => @file,"index" => @index})
          input["skiped_rows"] = @entity.custom["skip_rows"] if @entity.custom.include?("skip_rows")
          input["column_separator"] = @entity.custom["column_separator"] if @entity.custom.include?("column_separator")
          input["db_parser"] = @entity.custom["db_parser"] if @entity.custom.include?("db_parser")
          input["file_format"] = @entity.custom["file_format"] if @entity.custom.include?("file_format")
          input["exception_filename"] = File.expand_path("output/#{@entity.id}_#{@index}_#{DateTime.now.strftime("%Y%m%dT%H%M")}_exception.csv")
          input["rejected_filename"] = File.expand_path("output/#{@entity.id}_#{@index}_#{DateTime.now.strftime("%Y%m%dT%H%M")}_rejected.csv")
          @sql = Base::Templates.make("copy_from_local",input)
        end
      end
    end
  end
end
