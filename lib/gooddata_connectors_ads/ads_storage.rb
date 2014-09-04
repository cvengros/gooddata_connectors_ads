require 'sequel'
require 'jdbc/dss'


module GoodDataConnectorsAds


  class AdsStorage < GoodDataConnectorsBase::BaseStorage

    META_COLUMNS ={
        "_LOAD_ID" => "VARCHAR(255) NOT NULL",
        "_LOAD_AT" => "TIMESTAMP NOT NULL",
        "_INSERTED_AT" => "TIMESTAMP NOT NULL DEFAULT now()",
        "_IS_DELETED" => "boolean NOT NULL DEFAULT FALSE"
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
      puts GoodDataConnectorsBase::Templates.make("create_table",input)
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
      @database_entities = GoodDataConnectorsMetadata::Entities.new()
      database_tables.each do |table|
        columns = database_columns.find_all{|c| c[:table_id] == table[:table_id]}
        entity = GoodDataConnectorsMetadata::Entity.new("id" => table[:table_name],"name" => table[:table_name])
        columns.each do |column|
          if (!META_COLUMNS.include?(column[:column_name]))
            field = GoodDataConnectorsMetadata::Field.new("id" => column[:column_name],"name" => column[:column_name],"type" => TypeConverter.from_database_type(column))
            entity.add_field(field)
          end
        end
        @database_entities << entity
      end
    end

    def process_entity(entity)
      structural_changes(entity)
      import_data(entity)
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
        input["fields"] = entity.fields.values.map{|v| {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}}
        input["fields"] += META_COLUMNS.map{|k,v| {"name" => k,"type" => v}}
        Connection.db.run(GoodDataConnectorsBase::Templates.make("create_table",input))
      else
        # We have found the DB entity
        db_entity = @database_entities[entity.id]
        diff = entity.diff(db_entity)
        if (!diff["fields"]["only_in_source"].empty?)
          # There are new fields in source file, lets alter table and add new field
          diff["fields"]["only_in_source"].each do |v|
            input = {}
            input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
            input["table_name"] = entity.id
            input["name"] = v.id
            input["type"] = TypeConverter.to_database_type(v.type)
            Connection.db.run(GoodDataConnectorsBase::Templates.make("alter_table_add_columns",input))
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


    def import_data(entity)



      # Lets create the temporary table
      input = {}
      input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
      input["table_name"] = entity.id
      Connection.db.run(GoodDataConnectorsBase::Templates.make("drop_table",input))

      input = {}
      input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
      input["table_name"] = "temp_" + entity.id
      input["fields"] = entity.fields.values.find_all{|f| !f.disabled? }.map{|v| {"name" => v.id, "type" => TypeConverter.to_database_type(v.type)}}
      Connection.db.run(GoodDataConnectorsBase::Templates.make("create_table",input))

      # Copy from LOCAL
      input = {}
      input["schema"] = @metadata.get_configuration_by_type_and_key(@type,"instance_id")
      input["table_name"] = "temp_" + entity.id
      input
    end



  end

end
