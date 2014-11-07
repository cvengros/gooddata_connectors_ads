module GoodData
  module Connectors
    module Ads

      class Helper

        class << self

          def DEFAULT_META_COMPUTED_FIELDS()
            [
                {
                    "name" => "_LOAD_ID",
                    "function" => "load_id",
                    "type" => "integer",
                    "non-history" => true,
                    "default" => false

                },
                {
                    "name" => "_LOAD_AT",
                    "function" => "load_at",
                    "type" => "date-true",
                    "non-history" => true,
                    "default" => false

                },
                {
                    "name" => "_INSERTED_AT",
                    "function" => "now",
                    "type" => "date-true",
                    "non-history" => true,
                    "default" => false
                },
                {
                    "name" => "_IS_DELETED",
                    "function" => "is_deleted",
                    "type" => "boolean",
                    "non-history" => false,
                    "default" => true

                },
                {
                    "name" => "_VALID_FROM",
                    "function" => "timestamp",
                    "type" => "date-true",
                    "non-history" => false,
                    "default" => true
                },
                {
                    "name" => "_HASH",
                    "function" => "md5",
                    "type" => "string-255",
                    "non-history" => true,
                    "default" => true
                }

            ]
          end


          def set_up_metadata_object(metadata,type)
            @metadata = metadata
            @type = type
          end

          def get_configuration_by_type_and_key(key)
            @metadata.get_configuration_by_type_and_key(@type,key)
          end

          def set_up_logger(logger)
            @logger = logger
          end

          def logger
            @logger
          end

          def metadata
            @metadata
          end

          def computed_fields(entity,history = false,default = false,options = {})
            computed_fields_values = Helper.DEFAULT_META_COMPUTED_FIELDS.find_all do |v|
              if (default and history)
                v["default"] and !v["non-history"]
              elsif (default and !history)
                v["default"] and v["non-history"]
              elsif (history)
                true
              else
                v["non-history"]
              end
            end
            computed_fields_values += Helper.get_configuration_by_type_and_key("computed_fields") || [] if !default
            if (!computed_fields_values.nil?)
              output = []
              computed_fields_values.each do |field|
                hash = {"name" => field["name"],"function" => field["function"],"type" => Metadata::BaseType.create(field["type"])}
                case field["function"]
                  when "is_deleted"
                    hash.merge!({"value" => "%IS_DELETED%"})
                  when "load_id"
                    hash.merge!({"value" => Metadata::Runtime.get_load_id})
                  when "load_at"
                    hash.merge!({"value" => "'#{$now}'"})
                  when "source_file_name"
                    hash.merge!({"value" => "'#{options["file"].split("/").last}'"}) if !options["file"].nil?
                  when "md5"
                    elements = []
                    entity.get_enabled_fields.each do |field|
                      if (!entity.custom.include?("timestamp") or field != entity.custom["timestamp"])
                        elements << "COALESCE((#{field})::VARCHAR(255),'')"
                      end
                    end
                    hash.merge!({"value" => "MD5(#{elements.join(" || ")})"})
                  when "hash"
                    elements = []
                    entity.get_enabled_fields.each do |field|
                      if (!entity.custom.include?("timestamp") or field != entity.custom["timestamp"])
                        elements << field
                      end
                    end
                    hash.merge!({"value" => "(#{elements.join(",")})"})
                  when "hash_key"
                    if (!entity.custom["id"].nil?)
                      hash.merge!({"value" => "MD5(COALESCE((#{entity.custom["id"]})::VARCHAR(255),''))"})
                    elsif(!entity.custom["computed_id"].nil?)
                      elements = []
                      entity.custom["computed_id"]["fields"].each do |field|
                        elements << "COALESCE((#{field})::VARCHAR(255),'')"
                      end
                      hash.merge!({"value" => "MD5(#{elements.join(" || ")})"})
                    end
                  when "metadata"
                    if (!options["index"].nil?)
                      path = field["path"].split("|")
                      result = entity.runtime
                      path.each do |value|
                        if (value == "index")
                          result = result[options["index"]]
                        else
                          result = result[value]
                        end
                      end
                      hash.merge!({"value" => "'#{result}'"})
                    end
                  when "now"
                    hash.merge!({"value" => "now()"})
                  when "timestamp"
                    hash.merge!({"value" => "%TIMESTAMP%"})
                end
                output << hash
              end
              output
            else
              []
            end
          end


          # def computed_fields_merge()
          #   computed_fields = @metadata.get_configuration_by_type_and_key(@type,"computed_fields")
          #   if (!computed_fields.nil?)
          #     output = []
          #     computed_fields.each do |field|
          #       case field["function"]
          #         when "copy"
          #           output << {"source" => field["from"],"target" => field["name"]}
          #         else
          #           output << {"source" => field["name"],"target" => field["name"]}
          #       end
          #     end
          #     output
          #   else
          #     []
          #   end
          # end






        end


      end



    end
  end
end