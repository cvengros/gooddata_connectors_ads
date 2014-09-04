module GoodData
  module Connectors
    module Storage
      class SQLGenerator
        def initialize(params)
          @params = params
          @name_prefix = params['table_name_prefix']
        end

        def create_loads(columns, source_prefix=nil)
          @source = source_prefix
          return create(
            LOAD_INFO_NAME,
            columns.map {|k, v| {'name' => k}},
            :meta => true,
            :prefix => source_prefix
          )
        end

        def insert_load(column_values)
          columns = column_values.keys
          values = column_values.values_at(*columns)
          columns.push("_DIFF_HASH")
          # diffhash handled separateley as it's an expression, not value
          values_string = values.map {|e| "'#{e}'"}.join(', ') + ", #{hash_expression(values, true)}"

          return "INSERT INTO #{load_info_table_name} (#{columns.join(',')}) VALUES (#{values_string})"
        end

        def extract_load_info
          table_name = load_info_table_name
          return "SELECT * FROM #{table_name} #{last_load_condition}"
        end

        def load_info_table_name
          sql_table_name(LOAD_INFO_NAME, :meta => true, :prefix => @source)
        end

        LOAD_INFO_NAME = 'meta_loads'

        def extract(object, columns, prefix)
          return "SELECT #{columns.join(', ')} FROM #{sql_view_name(object, :last_snapshot => true, :prefix => prefix)}"
        end

        def create_last_snapshot_view(object, fields, prefix)
          field_names = fields.map {|f| f['name']}.join(', ')
          return "CREATE OR REPLACE VIEW #{sql_view_name(object, :last_snapshot => true, :prefix => prefix)} AS SELECT #{field_names}, _LOAD_ID, _LOAD_AT, _INSERTED_AT FROM #{sql_table_name(object, :prefix => prefix)} #{last_load_condition}"
        end

        def last_load_condition(not_equal=false)
          eql = not_equal ? '<>' : '='
          return "WHERE _LOAD_ID #{eql} (SELECT MAX(_LOAD_ID) FROM #{load_info_table_name})"
        end

        # filename is absolute
        def upload(table, fields, filename, load_id, load_at, prefix)
          field_string_list = fields.map {|f| f['name']}
          field_list = field_string_list.join(', ')

          hash_exp = hash_expression(field_string_list)

          return %Q{COPY #{sql_table_name(table, :prefix => prefix)} (#{field_list}, _LOAD_ID AS '#{load_id}', _LOAD_AT AS '#{load_at}', _DIFF_HASH AS #{hash_exp})
          FROM LOCAL '#{filename}' WITH PARSER GdcCsvParser()
          ESCAPE AS '"'
           SKIP 1
          EXCEPTIONS '#{except_filename(filename)}'
          REJECTED DATA '#{reject_filename(filename)}' }
        end

        def history_loading(object, load_history_from_params, object_fields, load_id, load_at, prefix)
          if !load_id
            raise "load the data first! load_id is empty"
          end

          snapshot_table_name = sql_table_name(object, :historization => true, :prefix => prefix)
          history_table_name = sql_table_name(load_history_from_params["name"], :prefix => prefix)
          # those that are mapped to nil, are ignored
          ignored_fields = load_history_from_params["column_mapping"].select {|f, t| !t}.keys
          valid_mappings = load_history_from_params["column_mapping"].select {|f,t| t}

          # fields to load to
          dest_fields = valid_mappings.keys

          # fields to load from
          src_fields = valid_mappings.values

          all_fields = object_fields.map {|o| o['name']}

          common_fields = (Set.new(all_fields) - Set.new(dest_fields) - Set.new(src_fields) - Set.new(ignored_fields)).to_a

          load_id_string = "'#{load_id}'"
          load_at_string = "'#{load_at}'"
          dest_meta = ["_LOAD_ID", "_LOAD_AT", "_LAST_SEEN_ID", "_DIFF_HASH"]
          src_meta = [load_id_string, load_at_string, load_id_string, hash_expression(src_fields + common_fields)]

          field_list_dest = (dest_fields + common_fields + dest_meta).join(", ")
          field_list_src = (src_fields + common_fields + src_meta).join(", ")

          sql = "INSERT INTO #{snapshot_table_name} (#{field_list_dest})\n"
          sql += "SELECT #{field_list_src} \n"

          sql += "FROM #{history_table_name} #{last_load_condition}"
          return sql
        end

        def load_count
          return "SELECT COUNT(*) FROM #{load_info_table_name}"
        end

        def create(table, fields, options={})
          historization = options[:historization]
          meta = options[:meta]
          prefix = options[:prefix]
          fields_string = fields.map{|f| "#{f['name']} #{TYPE_MAPPING[f['type']] || DEFAULT_TYPE}"}.join(", ")
          meta_cols = col_strings(META_COLUMNS)
          hist_cols = historization ? ", #{col_strings(HISTORIZATION_COLUMNS)}" : ""

          # id column isn't used for historization tables as merge to tables with id doesn't work.
          # it can be somehow faked using sequences
          id_col = historization ? "" : "#{ID_COLUMN.keys[0]} #{ID_COLUMN.values[0]},"

          return "CREATE TABLE IF NOT EXISTS #{sql_table_name(table, :historization => historization, :meta => meta, :prefix => prefix)}
          (#{id_col} #{fields_string}, #{meta_cols} #{hist_cols})"
        end

        def column_count(object, column)
          "SELECT COUNT(column_name) FROM columns WHERE table_name = '#{sql_table_name(object)}' and column_name = '#{column}'"
        end

        def delete_but_last_load(object, prefix)
          table_name = sql_table_name(object, :prefix => prefix)
          return "DELETE FROM #{table_name} #{last_load_condition(true)}"
        end

        def historization_merge(object, merge_from_params, object_fields, source)

          # get all the params to be used
          snapshot_table_name = sql_table_name(object, :historization => true, :prefix => source)
          object_table_name = sql_table_name(merge_from_params["name"], :prefix => source)
          fields = object_fields.map {|o| o['name']}

          sql = "MERGE  INTO #{snapshot_table_name} s USING #{object_table_name} o\n"

          # Matching

          # finding matches in all fields - in the hash
          # string like s.Id = o.Id AND os.StageName = o.StageName

          sql += "ON (s._DIFF_HASH = o._DIFF_HASH)\n"

          # Updating

          # when we find a match just update from the column mapping, plus the load id
          col_mapping = merge_from_params["column_mapping"].merge({
            "_LAST_SEEN_ID" => "_LOAD_ID",
          })
          set_string = col_mapping.map {|dest, src| "#{dest} = o.#{src}"}.join(", ")
          sql += "WHEN MATCHED THEN UPDATE SET #{set_string}\n"

          # Inserting

          # if not matched insert a new row from the (staging) Opportunity table
          # columns that are caried over
          insert_identity_cols = ["_LOAD_ID", "_LOAD_AT", "_DIFF_HASH"]

          # custom defined mappings
          col_mapping_keys = col_mapping.keys
          col_mapping_values = col_mapping.values_at(*col_mapping_keys)

          dest_string = (col_mapping_keys + fields + insert_identity_cols).join(", ")
          src_string = (col_mapping_values + fields + insert_identity_cols).map{|f| "o.#{f}"}.join(", ")
          sql += "WHEN NOT MATCHED THEN INSERT (#{dest_string}) VALUES (#{src_string})"

          return sql
        end

        def except_filename(filename)
          return "#{filename}.except.log"
        end

        def reject_filename(filename)
          return "#{filename}.reject.log"
        end

        private

        ID_COLUMN = {"_oid" => "IDENTITY PRIMARY KEY"}

        META_COLUMNS = [
          {"_DIFF_HASH" => "VARCHAR(1023) NOT NULL"},
          {"_LOAD_ID" => "VARCHAR(255) NOT NULL"},
          {"_LOAD_AT" => "TIMESTAMP NOT NULL"},
          {"_INSERTED_AT" => "TIMESTAMP NOT NULL DEFAULT now()"},
          {"_IS_DELETED" => "boolean NOT NULL DEFAULT FALSE"},
        ]

        HISTORIZATION_COLUMNS = [
          {"_VALID_FROM" => "TIMESTAMP"},
          {"_LAST_SEEN_ID" => "VARCHAR(255) NOT NULL"}
        ]

        TYPE_MAPPING = {
          "date" => "DATE",
          "datetime" => "TIMESTAMP",
          "string" => "VARCHAR(255)",
          "double" => "DOUBLE PRECISION",
          "int" => "INTEGER",
          # the vertica currency doesn't work well with parsing sfdc values
          "currency" => "DECIMAL",
          "boolean" => "BOOLEAN",
          "textarea" => "VARCHAR(32769)"
        }

        DEFAULT_TYPE = "VARCHAR(255)"

        def col_strings(col_list)
          return col_list.map {|col| "#{col.keys[0]} #{col.values[0]}"}.join(", ")
        end

        HASH_LIMIT = 32

        # fields is a list of strings
        # if these are values the expression will contain apostrophes around each value
        def hash_expression(fields, values=false)

          if values
            fields.map!{|f| "'#{f}'"}
          end

          field_list = fields.join(', ')

          if fields.length <= HASH_LIMIT
            return "HASH(#{field_list})"
          end

          # arrays HASH_LIMIT long .. .map - make hashes out of them
          hashes = fields.each_slice(HASH_LIMIT).map{|a| "HASH(#{a.join(', ')})"}
          # do a reduce to CONCAT( HASH(something), CONCAT(HASH, CONCAT...))
          return hashes[1, hashes.length-1].reduce(hashes[0]) do |product, hsh|
            "CONCAT(#{hsh}, #{product})"
          end
        end

        DEFAULT_TABLE_PREFIX = ''

        def table_prefix
          return @params['table_name_prefix'] ? "#{@params['table_name_prefix']}_" : DEFAULT_TABLE_PREFIX
        end

        def sql_table_name(obj, options={})
          historization = options[:historization]
          meta = options[:meta]
          ds_prefix = options[:prefix] ? "#{options[:prefix]}_" : ''
          hist_postfix = historization || meta ? '' : '_in'

          return "#{table_prefix}#{ds_prefix}#{obj}#{hist_postfix}"
        end

        def sql_view_name(obj, options={})
          last_snapshot_postfix = options[:last_snapshot] ? "_last_snapshot": ''
          ds_prefix = options[:prefix] ? "#{options[:prefix]}_" : ''
          return "#{table_prefix}#{ds_prefix}#{obj}#{last_snapshot_postfix}"
        end

        def obj_name(sql_table)
          start = table_prefix.length
          return sql_table[start..-1]
        end
      end
    end
  end
end