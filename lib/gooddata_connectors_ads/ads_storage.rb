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
              if (!META_COLUMNS.include?(column[:column_name]))
                field = Metadata::Field.new("id" => column[:column_name],"name" => column[:column_name],"type" => TypeConverter.from_database_type(column))
                entity.add_field(field)
              end
            end
            @database_entities << entity
          end
        end

        def process_entity(entity_id,dependent_entities = nil)
          entity = @metadata.get_entity(entity_id)
          structural_changes(entity)
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
          import_data(entity)
          integrate_entity(entity)



        end




        private

        def structural_changes(entity)
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
              if (v.id != entity.custom["timestamp"])
                input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}
              end
            end
            input["fields"] += META_COLUMNS.map{|k,v| {"name" => k,"type" => v}}
            Connection.db.run(Base::Templates.make("create_table",input))
          else
            # We have found the DB entity
            db_entity = @database_entities[entity.id]
            diff = entity.diff(db_entity)
            if (!diff["fields"]["only_in_source"].empty?)
              # There are new fields in source file, lets alter table and add new field
              diff["fields"]["only_in_source"].each do |v|
                if (v.id != entity.custom["timestamp"] and !v.disabled?)
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
          Connection.db.run(Base::Templates.make("create_table",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["table_name"] = "temp_" + entity.id
          input["fields"] =  entity.get_enabled_fields
          #input["fields"] += META_COLUMNS.map{|k,v| k}
          input["filename"] = File.expand_path(entity.runtime["parsed_filename"])
          input["exception_filename"] = File.expand_path("output/#{entity.id}_exception.csv")
          input["rejected_filename"] = File.expand_path("output/#{entity.id}_rejected.csv")
          puts Base::Templates.make("copy_from_local",input)
          Connection.db.run(Base::Templates.make("copy_from_local",input))

        end


        def integrate_entity(entity)
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
          input["load_at"] = Time.now.utc
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}
            end
          end
          Connection.db.run(Base::Templates.make("history_to_temp_stage",input))

          input = {}
          input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
          input["stage_table_name"] = "temp_" + entity.id + "_stage"
          input["table_name"] = "temp_" + entity.id
          input["id"] = entity.custom["id"]
          input["timestamp"] = entity.custom["timestamp"]
          # TO-DO CHANGE This
          input["load_id"] = Metadata::Runtime.get_load_id
          input["load_at"] = Time.now.utc
          # We need to create table with timestamp value, because we don't want to have it in final table
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << v.id
            end
          end
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
          input["default_load_at"] = Time.now.utc
          input["fields"] = []
          entity.fields.values.find_all{|f| !f.disabled? }.each do |v|
            if (v.id != entity.custom["timestamp"] and v.id != entity.custom["id"])
              input["fields"] << v.id
            end
          end
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
      end
    end
  end
end