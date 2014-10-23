require 'sequel'
require 'jdbc/dss'


module GoodData
  module Connectors
    module Ads



      class AdsStorage < Base::BaseStorage

        META_COLUMNS ={
            "_load_id" => "integer NOT NULL",
            "_load_at" => "timestamp NOT NULL",
            "_inserted_at" => "timestamp NOT NULL DEFAULT now()",
            "_is_deleted" => "boolean NOT NULL DEFAULT FALSE",
            "_valid_from" => "timestamp NOT NULL",
            "_valid_to" => "timestamp"
        }


        META_COLUMNS_WITHOUT_HISTORY ={
            "_load_id" => "integer NOT NULL",
            "_load_at" => "timestamp NOT NULL",
            "_inserted_at" => "timestamp NOT NULL DEFAULT now()",
            "_is_deleted" => "boolean NOT NULL DEFAULT FALSE"
        }

        DEFAULT_VALIDATION_DIRECTORY = File.join(File.dirname(__FILE__),"../validations")


        def initialize(metadata,options ={})
          @type = "ads_storage"
          super(metadata,File.dirname(__FILE__) + "/erb",options)
          ads_instance_id = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          username = @metadata.get_configuration_by_type_and_key(@type,"username")
          password = @metadata.get_configuration_by_type_and_key(@type,"password")
          Connection.set_up(ads_instance_id,username,password)

        end


        def define_mandatory_configuration
          {
              @type => ["instance_id","username","password"]
          }.merge!(super)
        end

        def define_default_configuration
          {
              @type => {}
          }
        end

        def test_erb_template(entity)
          input = {}
          input["schema"] = "u0fbe97c1460b4a274c72fc35efc7da2"
          input["table_name"] = entity.id
          input["fields"] = entity.fields.values.map {|v| {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}}
          puts Base::Templates.make("create_table",input)
        end

        def load_database_structure
          database_tables = []
          Connection.db.fetch("select * from tables") do |row|
            database_tables << row
          end
          database_columns = []
          Connection.db.fetch("select * from columns") do |row|
            database_columns << row
          end
          @database_entities = Metadata::Entities.new()
          database_tables.each do |table|
            columns = database_columns.find_all{|c| c[:table_id] == table[:table_id]}
            entity = Metadata::Entity.new("id" => table[:table_name],"name" => table[:table_name])
            columns.each do |column|
              if (!META_COLUMNS.include?(column[:column_name]) and !META_COLUMNS_WITHOUT_HISTORY.include?(column[:column_name]))
                field = Metadata::Field.new("id" => column[:column_name],"name" => column[:column_name],"type" => TypeConverter.from_database_type(column))
                entity.add_field(field)
              end
            end
            @database_entities << entity
          end
        end

        def process_entity(entity_id,dependent_entities = nil)
          entity = @metadata.get_entity(entity_id)
          history = false
          if (entity.custom.include?("history"))
            history = entity.custom["history"]
          end
          structural_changes(entity,history)
          if (!dependent_entities.nil?)
            # All dependent entities, will be merge to one historical table
            # Lets create it
            create_historical_temp_table(entity)
            dependent_entities.each do |dependent_entity_id|
              dependent_entity = @metadata.get_entity(dependent_entity_id)
              if (dependent_entity.custom["type"] =~ /normalized|denormalized/)
                import_historical_data(entity,dependent_entity)
              else
                raise Base::AdsException, "Unsupported type of dependent entity #{dependent_entity.custom["type"]}"
              end
            end
          end
          if (history)
            import_data(entity)
            import_deleted_data(entity)
            integrate_entity_with_history(entity)
            integrate_entity_synchronization(entity)
          else
            import_data(entity)
            import_deleted_data(entity)
            integrate_entity(entity)
          end
          # perform_validations(entity)
        end




        private

        def structural_changes(entity,history)
          #Lets try to find entity in the database structure
          db_entity = nil
          if (!@database_entities.include?(entity.id))
            # The DB entity don't exist lets create it in ADS
            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = entity.id
            # We need to create table with timestamp value, because we don't want to have it in final table
            input["fields"] = []
            entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
              input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}
            end
            if (history)
              input["fields"] += META_COLUMNS.map{|k,v| {"name" => k,"type" => v}}
            else
              input["fields"] += META_COLUMNS_WITHOUT_HISTORY.map{|k,v| {"name" => k,"type" => v}}
            end
            if (!entity.custom["computed_id"].nil?)
              input["fields"] << {"name" => "computed_id","type" => "BIGINT"}
            end

            # Lets solve computed fields section in ADS gem configuration
            # Computed fields are added only when creating the table
            input["fields"] += computed_fields_structure()
            Connection.db.run(Base::Templates.make("create_table",input))
          else
            # We have found the DB entity
            db_entity = @database_entities[entity.id]
            diff = entity.diff(db_entity)
            if (!diff["fields"]["only_in_source"].empty?)
              # There are new fields in source file, lets alter table and add new field
              diff["fields"]["only_in_source"].each do |v|
                if (v.id != entity.custom["timestamp"] and !v.disabled? and v.id != "computed_id")
                  input = {}
                  input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
                  input["table_name"] = entity.id
                  input["name"] = v.id
                  input["type"] = TypeConverter.to_database_type(v.type)
                  puts Base::Templates.make("alter_table_add_columns",input)
                  Connection.db.run(Base::Templates.make("alter_table_add_columns",input))
                end
              end
            end
            if (!diff["fields"]["changed"].empty?)
              diff["fields"]["changed"].each do |field|
                if (field.include?("type"))
                  # Ups we have problem
                  # Database type is different then entity type
                  $log.error  "The database type is different then entity type for entity #{entity.id} and field #{field["field"].id}"
                end
              end
            end
            # if (!entity.custom.include?("validate") or entity.custom["validate"])
            #   folders = []
            #   folders << File.absolute_path(DEFAULT_VALIDATION_DIRECTORY)
            #   entity.generate_validations(folders,@type)
            # end
          end
        end


        def create_historical_temp_table(entity)
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_#{entity.id}_history"
          Connection.db.run(Base::Templates.make("drop_table",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_#{entity.id}_history"
          input["fields"] = HISTORY_TABLE.map{|k,v| {"name" => k, "type" => v} }
          Connection.db.run(Base::Templates.make("create_table",input))
        end

        def import_historical_data(root_entity,entity)
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + root_entity.id + "_history"
          input["fields"] = HISTORY_TABLE.keys
          input["filename"] = File.expand_path(entity.runtime["parsed_filename"])
          input["exception_filename"] = File.expand_path("output/exception.csv")
          input["rejected_filename"] = File.expand_path("output/rejected.csv")
          Connection.db.run(Base::Templates.make("copy_from_local",input))
        end


        def import_data(entity)
          # Lets create the temporary table
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id
          Connection.db.run(Base::Templates.make("drop_table",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id
          input["fields"] = entity.fields.values.find_all{|f| !f.disabled? }.map{|v| {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}}
          if (!entity.custom["computed_id"].nil?)
            input["fields"] << {"name" => "computed_id", "type" => "BIGINT"}
          end
          input["fields"] += computed_fields_structure()
          puts Base::Templates.make("create_table",input)
          Connection.db.run(Base::Templates.make("create_table",input))


          files_to_process = []
          if (entity.runtime.include?("parsed_filename"))
            files_to_process << entity.runtime["parsed_filename"]
          end

          if (entity.runtime.include?("parsed_filenames"))
            files_to_process += entity.runtime["parsed_filenames"]
          end

          files_to_process.each_with_index do |file,index|
            $log.info "Processing file #{file} (#{index}) - COPY FROM LOCAL"
            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id
            input["fields"] =  entity.get_enabled_fields
            #input["fields"] += META_COLUMNS.map{|k,v| k}
            input["filename"] = File.expand_path(file)
            if (!entity.custom["computed_id"].nil?)
              case entity.custom["computed_id"]["function"]
                when "hash",nil
                  input["computed_id"] = "HASH(#{entity.custom["computed_id"]["fields"].join(",")})"
              end
            end
            input["computed_fields"] = computed_fields_data(entity, {"file" => file,"index" => index})
            input["skiped_rows"] = entity.custom["skip_rows"] if entity.custom.include?("skip_rows")
            input["column_separator"] = entity.custom["column_separator"] if entity.custom.include?("column_separator")
            input["file_format"] = entity.custom["file_format"] if entity.custom.include?("file_format")
            input["exception_filename"] = File.expand_path("output/#{entity.id}_#{index}_exception.csv")
            input["rejected_filename"] = File.expand_path("output/#{entity.id}_#{index}_rejected.csv")
            puts Base::Templates.make("copy_from_local",input)
            Connection.db.run(Base::Templates.make("copy_from_local",input))
            $log.info "Processing finished - COPY FROM LOCAL"
          end
        end

        def import_deleted_data(entity)
          if (entity.runtime.include?("deleted_parsed_filename"))
            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_deleted"
            Connection.db.run(Base::Templates.make("drop_table",input))

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_deleted"
            fields = []
            fields << {"name" => entity.custom["id"], "type" => TypeConverter.to_database_type(entity.get_field(entity.custom["id"]).type) }
            fields << {"name" => entity.custom["timestamp"], "type" => TypeConverter.to_database_type(entity.get_field(entity.custom["timestamp"]).type) }
            fields << {"name" => "IsDeleted", "type" => "boolean"}
            input["fields"] = fields
            Connection.db.run(Base::Templates.make("create_table",input))

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_deleted"
            input["fields"] =  [entity.custom["id"],entity.custom["timestamp"],"IsDeleted"]
            #input["fields"] += META_COLUMNS.map{|k,v| k}
            input["filename"] = File.expand_path(entity.runtime["deleted_parsed_filename"])
            input["exception_filename"] = File.expand_path("output/#{entity.id}_deleted_exception.csv")
            input["rejected_filename"] = File.expand_path("output/#{entity.id}_deleted_rejected.csv")
            puts Base::Templates.make("copy_from_local",input)
            Connection.db.run(Base::Templates.make("copy_from_local",input))
          end
        end



        def integrate_entity_with_history(entity)
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id + "_stage"
          Connection.db.run(Base::Templates.make("drop_table",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id + "_stage"
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"])
              input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}
            end
          end
          input["fields"] += META_COLUMNS.map{|k,v| {"name" => k,"type" => v}}
          Connection.db.run(Base::Templates.make("create_table",input))

          #Import last records from Stage table to temp stage table
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["stage_table_name"] = "temp_" + entity.id + "_stage"
          input["table_name"] = entity.id
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"])
              input["fields"] << v.id
            end
          end
          puts Base::Templates.make("last_from_stage_to_temp_stage",input)
          Connection.db.run(Base::Templates.make("last_from_stage_to_temp_stage",input))

          # Import data from history tables
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["stage_table_name"] = "temp_" + entity.id + "_stage"
          input["table_name"] = "temp_" + entity.id + "_history"
          input["id"] = entity.custom["id"]
          input["history_id"] = Base::Global::HISTORY_ID
          input["history_timestamp"] = Base::Global::HISTORY_TIMESTAMP
          # TO-DO CHANGE This
          input["load_id"] = Metadata::Runtime.get_load_id
          input["load_at"] = DateTime.now
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type), "type_object" => v.type}
            end
          end
          puts Base::Templates.make("history_to_temp_stage",input)
          Connection.db.run(Base::Templates.make("history_to_temp_stage",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["stage_table_name"] = "temp_" + entity.id + "_stage"
          input["table_name"] = "temp_" + entity.id
          input["id"] = entity.custom["id"]
          input["timestamp"] = entity.custom["timestamp"]
          # TO-DO CHANGE This
          input["load_id"] = Metadata::Runtime.get_load_id
          input["load_at"] = DateTime.now
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["insert_fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["insert_fields"] << v.id
            end
          end
          input["select_fields"] = input["insert_fields"]
          puts Base::Templates.make("entity_to_temp_stage",input)
          Connection.db.run(Base::Templates.make("entity_to_temp_stage",input))


          # Lets load the Deleted records to temp_stage table
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["stage_table_name"] = "temp_" + entity.id + "_stage"
          input["table_name"] = "temp_" + entity.id + "_deleted"
          input["id"] = entity.custom["id"]
          input["timestamp"] = entity.custom["timestamp"]
          # TO-DO CHANGE This
          input["load_id"] = Metadata::Runtime.get_load_id
          input["load_at"] = DateTime.now
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["select_fields"] = ["IsDeleted"]
          input["insert_fields"] = ["_is_deleted"]
          puts Base::Templates.make("entity_to_temp_stage",input)
          Connection.db.run(Base::Templates.make("entity_to_temp_stage",input))

          # LETS INTEGRATE - this is multiple query select run as one query
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temporary_" + entity.id
          command = Base::Templates.make("drop_table",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temporary_" + entity.id
          command = Base::Templates.make("drop_table",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temporary_" + entity.id
          input["temporary"] = true
          input["preserve_rows"] = true
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"])
              input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}
            end
          end
          input["fields"] += META_COLUMNS.map{|k,v| {"name" => k,"type" => v}}
          command << Base::Templates.make("create_table",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["temp_table_name"] = "temporary_" + entity.id
          input["temp_stage_table_name"] = "temp_" + entity.id + "_stage"
          input["id"] = entity.custom["id"]
          # In case that this two values are null - mostly because empty main table
          input["default_load_id"] = Metadata::Runtime.get_load_id
          input["default_load_at"] = DateTime.now
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << v.id
            end
          end
          puts Base::Templates.make("integration",input)
          command << Base::Templates.make("integration",input)


          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = entity.id
          input["temp_table_name"] = "temporary_" + entity.id
          input["id"] = entity.custom["id"]
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << v.id
            end
          end
          puts Base::Templates.make("merge",input)
          command << Base::Templates.make("merge",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id + "_stage"
          command << Base::Templates.make("drop_table",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id + "_history"
          command << Base::Templates.make("drop_table",input)

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id
          command << Base::Templates.make("drop_table",input)

          Connection.db.run(command)
        end

        def integrate_entity(entity)
          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = entity.id
          input["temp_table_name"] = "temp_" + entity.id
          if (!entity.custom["computed_id"].nil?)
            input["id"] = "computed_id"
          else
            input["id"] = entity.custom["id"]
          end
          input["timestamp"] = entity.custom["timestamp"] if entity.custom.include?("timestamp")
          input["load_id"] = Metadata::Runtime.get_load_id
          input["load_at"] = DateTime.now
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << v.id
            end
          end
          input["computed_fields"] = computed_fields_merge
          puts Base::Templates.make("merge_without_history",input)
          Connection.db.run(Base::Templates.make("merge_without_history",input))

          if (entity.runtime.include?("deleted_parsed_filename"))
            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = entity.id
            input["temp_table_name"] = "temp_" + entity.id + "_deleted"
            input["id"] = entity.custom["id"]
            input["timestamp"] = entity.custom["timestamp"]
            input["load_id"] = Metadata::Runtime.get_load_id
            input["load_at"] = DateTime.now
            puts Base::Templates.make("merge_deleted_records",input)
            Connection.db.run(Base::Templates.make("merge_deleted_records",input))
          end

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id
          Connection.db.run(Base::Templates.make("drop_table",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id + "_deleted"
          Connection.db.run(Base::Templates.make("drop_table",input))
        end

        def integrate_entity_synchronization(entity)

          new_fields = entity.fields.values.find_all{|f| !f.disabled? and f.custom["synchronized"] == false }
          if (!new_fields.empty? and !Metadata::Runtime.get_entity_last_load(entity.id).nil?)

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_synchronization"
            Connection.db.run(Base::Templates.make("drop_table",input))

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_synchronization"
            input["fields"] = []
            input["fields"] << {"name" => entity.custom["id"], "type" => TypeConverter.to_database_type(entity.get_field(entity.custom["id"]).type)}
            input["fields"] << {"name" => entity.custom["timestamp"], "type" => TypeConverter.to_database_type(entity.get_field(entity.custom["timestamp"]).type)}
            input["fields"] += new_fields.map{|v| {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}}
            Connection.db.run(Base::Templates.make("create_table",input))

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_synchronization"
            input["fields"] = []
            input["fields"] << entity.custom["id"]
            input["fields"] << entity.custom["timestamp"]
            input["fields"] += new_fields.map{|v| v.id}
            #input["fields"] += META_COLUMNS.map{|k,v| k}
            input["filename"] = File.expand_path(entity.runtime["synchronization_parsed_filename"])
            input["exception_filename"] = File.expand_path("output/synchronization_#{entity.id}_exception.csv")
            input["rejected_filename"] = File.expand_path("output/synchronization_#{entity.id}_rejected.csv")
            puts Base::Templates.make("copy_from_local",input)
            Connection.db.run(Base::Templates.make("copy_from_local",input))


            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["temp_table_name"] = "temp_" + entity.id + "_synchronization"
            input["table_name"] = entity.id
            input["id"] = entity.custom["id"]
            input["fields_from_main"] = entity.get_enabled_fields - new_fields.map{|v| v.id}
            input["fields_from_temp"] = new_fields.map{|v| v.id}
            input["load_id"] = Metadata::Runtime.get_load_id
            input["load_at"] = DateTime.now
            pp input
            puts Base::Templates.make("merge_new_entity_fields",input)
            Connection.db.run(Base::Templates.make("merge_new_entity_fields",input))

            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = "temp_" + entity.id + "_synchronization"
            Connection.db.run(Base::Templates.make("drop_table",input))
          end

          entity.fields.values.each do |field|
            field.custom["synchronized"] = true
          end
        end


        def perform_validations(metadata_entity)
          metadata_entity.validations.each_pair do |key,types|
            types.each_pair do |type,validation|
              if (type == @type)
                values = {
                    "id" => metadata_entity.custom["id"],
                    "timestamp" => metadata_entity.custom["timestamp"],
                    "to" => GoodData::Connectors::Metadata::Runtime.now,
                    "entity_id" => metadata_entity.id,
                    "schema" => @metadata.get_configuration_by_type_and_key(@type,"instance_id"),
                    "history" => metadata_entity.custom["history"] || true
                }
                sql = Base::Templates.make_validation_template(validation,values)
                values = []
                Connection.db.fetch(sql) do |row|
                  values << row
                end
              end
            end
          end
        end


        def computed_fields_structure()
          computed_fields = @metadata.get_configuration_by_type_and_key(@type,"computed_fields")
          if (!computed_fields.nil?)
             output = []
             computed_fields.each do |field|
               raise Metadata::TypeException("The custom fiels #{field["name"]} don't have type attribute") if (field["type"].nil?)
               output << {"name" => field["name"], "type" => TypeConverter.to_database_type(Metadata::BaseType.create(field["type"]))}
             end
             output
          else
            []
          end
        end


        def computed_fields_data(entity,options = {})
          computed_fields = @metadata.get_configuration_by_type_and_key(@type,"computed_fields")
          if (!computed_fields.nil?)
            output = []
            computed_fields.each do |field|
              case field["function"]
                when "source_file_name"
                    output << "#{field["name"]} as '#{options["file"].split("/").last}'"
                when "hash"
                    output << "#{field["name"]} as HASH(#{entity.get_enabled_fields.join(",")})"
                when "hash_key"
                    if (!entity.custom["id"].nil?)
                      output << "#{field["name"]} as HASH(#{entity.custom["id"]})"
                    elsif(!entity.custom["computed_id"].nil?)
                      output << "#{field["name"]} as HASH(#{entity.custom["computed_id"]["fields"].join(",")})"
                    end
                when "metadata"
                    path = field["path"].split("|")
                    result = entity.runtime
                    path.each do |value|
                      if (value == "index")
                        result = result[options["index"]]
                      else
                        result = result[value]
                      end
                    end
                    output << "#{field["name"]} as '#{result}'"
                when "now"
                  output << "#{field["name"]} as '#{$now}'"
              end
            end
            output
          else
            []
          end
        end


        def computed_fields_merge()
          computed_fields = @metadata.get_configuration_by_type_and_key(@type,"computed_fields")
          if (!computed_fields.nil?)
            output = []
            computed_fields.each do |field|
              case field["function"]
                when "copy"
                  output << {"source" => field["from"],"target" => field["name"]}
                else
                  output << {"source" => field["name"],"target" => field["name"]}
              end
            end
            output
          else
            []
          end
        end


      end
    end
  end
end