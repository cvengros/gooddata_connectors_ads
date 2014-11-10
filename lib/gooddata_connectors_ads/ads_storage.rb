require 'sequel'
require 'jdbc/dss'


module GoodData
  module Connectors
    module Ads



      class AdsStorage < Base::BaseStorage

        DEFAULT_VALIDATION_DIRECTORY = File.join(File.dirname(__FILE__),"../validations")


        def initialize(metadata,options ={})
          @type = "ads_storage"
          super(metadata,File.dirname(__FILE__) + "/erb",options)
          ads_instance_id = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          username = @metadata.get_configuration_by_type_and_key(@type,"username")
          password = @metadata.get_configuration_by_type_and_key(@type,"password")
          Connection.set_up(ads_instance_id,username,password)
          Helper.set_up_metadata_object(@metadata,@type)
          Helper.set_up_logger(options["GDC_LOGGER"])
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
              if (Helper.DEFAULT_META_COMPUTED_FIELDS.find{|v| v["name" == column[:column_name]]}.nil?)
                field = Metadata::Field.new("id" => column[:column_name],"name" => column[:column_name],"type" => TypeConverter.from_database_type(column))
                entity.add_field(field)
              end
            end
            @database_entities << entity
          end
        end

        def process_entity(entity_id,dependent_entities = nil)
          Connection.disconnect # For connection refresh
          entity = @metadata.get_entity(entity_id)
          history = false
          if (entity.custom.include?("history"))
            history = entity.custom["history"]
          end
          structural_changes(entity,history)
          if (!dependent_entities.nil?)
            # All dependent entities, will be merge to one historical table
            # Lets create it

            historical_task = Task.new("Integrate History Task",entity)
            historical_task << DropHistoryForInputTask.new(entity)
            historical_task << CreateHistoryForInputTask.new(entity)
            dependent_entities.each do |dependent_entity_id|
              dependent_entity = @metadata.get_entity(dependent_entity_id)
              if (dependent_entity.custom["type"] =~ /normalized|denormalized/)
                historical_task << ImportHistoricalDataForInputTask.new(entity,dependent_entity)
              else
                raise Base::AdsException, "Unsupported type of dependent entity #{dependent_entity.custom["type"]}"
              end
            end
            historical_task.run_sql
          end
          if (history)
            import_data_with_history(entity)
            import_deleted_data(entity)
            integrate_entity_with_history(entity)
          else
            import_data_without_history(entity)
            import_deleted_data(entity)
            integrate_entity(entity)
          end
          Connection.disconnect
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
              if (!entity.custom.include?("timestamp") or v.id != entity.custom["timestamp"])
                input["fields"] << {"name" => v.id, "type" => v.type}
              end
            end
            if (!entity.custom["computed_id"].nil?)
              input["fields"] << {"name" => "computed_id","type" => Metadata::BaseType.create("integer")}
            end
            input["fields"] += Helper.computed_fields(entity,history)
            Connection.db.run(Base::Templates.make("create_table",input))
          else
            # We have found the DB entity
            # TO DO Change structural tables in case of change in computed fields
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
                  input["type"] = v.type
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
          end
        end


        def import_data_with_history(entity)

          files_to_process = []
          if (entity.runtime.include?("parsed_filename"))
            files_to_process << entity.runtime["parsed_filename"]
          end

          if (entity.runtime.include?("parsed_filenames"))
            files_to_process += entity.runtime["parsed_filenames"]
          end
          import_data_task = ImportDataTask.new(entity,files_to_process,true)
          import_data_task.set_default(true)
          import_data_task.run_sql
        end


        def import_data_without_history(entity)

          files_to_process = []
          if (entity.runtime.include?("parsed_filename"))
            files_to_process << entity.runtime["parsed_filename"]
          end

          if (entity.runtime.include?("parsed_filenames"))
            files_to_process += entity.runtime["parsed_filenames"]
          end
          import_data_task = ImportDataTask.new(entity,files_to_process,false)
          import_data_task.set_default(false)
          import_data_task.run_sql
        end

        def import_deleted_data(entity)
          if (entity.runtime.include?("deleted_parsed_filename"))
            deleted_records_task = Task.new("Integrate Deleted Records Task",entity)
            deleted_records_task << DropDeletedTableForInputTask.new(entity)
            deleted_records_task << CreateDeletedTableForInputTask.new(entity)
            deleted_records_task << ImportDeletedDataForInputTask.new(entity)
            deleted_records_task.run_sql
          end
        end



        def integrate_entity_with_history(entity)
          history_wrapper_task = HistoryWrapperTask.new(entity)
          history_wrapper_task.run_sql
        end

        def integrate_entity(entity)
          integrate_entity_task = Task.new("Integrate Entity Data Without History",entity)
          integrate_entity_task << MergeDataTask.new(entity)
          integrate_entity_task << DropDeletedTableForInputTask.new(entity)
          integrate_entity_task << DropTableForInputTask.new(entity)
          integrate_entity_task << AnalyzeStatisticsTask.new(entity)
          integrate_entity_task.run_sql
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




      end
    end
  end
end