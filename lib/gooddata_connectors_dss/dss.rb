require "gooddata_connectors_base"
require 'jdbc/dss'
require 'sequel'


require_relative "sql_generator"

module GoodData
  module Connectors
    module Storage
      class Dss < GoodData::Connectors::Base::BaseStorage
        def initialize(metadata, params)
          @type = "dss"
          super(metadata, params)

          if (!@params["gdc_username"]) || (!@params["gdc_password"])
            # use the standard ones
            @params["gdc_username"] = params["GDC_USERNAME"]
            @params["gdc_password"] = params["GDC_PASSWORD"]
          end
          @logger = @global_params["GDC_LOGGER"]

          @generator = SQLGenerator.new(@params)

          Jdbc::DSS.load_driver
          Java.com.gooddata.dss.jdbc.driver.DssDriver
        end

        def save_full(params)
          downloaded_info = params['local_files']

          downloaded_info.each do |source, info|
            # create dss tables
            create_tables(source, info, @params['historized_objects'])

            # load the data as is
            load_data(source, info)

            # reshuffle the data to the historization tables
            load_historization_data(source, info, @params['historized_objects'])
          end
        end

        private

        def create_tables(source, info, historized_objects)
          # create the load table if it doesn't exist yet
          create_sql = @generator.create_loads(info['objects'])
          execute(create_sql)

          # create the tables one by one
          info['objects'].each do |table, table_meta|
            fields = table_meta['fields']
            sql = @generator.create(table, fields, :prefix => source)
            execute(sql)

            sql_view = @generator.create_last_snapshot_view(table, fields, source)
            execute(sql_view)

            # if it should be historized create one more
            if historized_objects && historized_objects[table]
              sql = @generator.create(table, fields, :historization => true, :prefix => source)
              execute(sql)
            end
          end
        end

        # expecting hash:
        # table name ->
        #   :fields -> list of columns
        #   :filename -> name of the csv file
        def load_data(source, info)

          # save the info and load the tables
          save_download_info(info['meta'])

          # load the data for each table and each file to be loaded there
          info['objects'].each do |table, table_meta|
            if ! table_meta['filenames']
              fail "There're no filenames for #{table}"
            end
            table_meta['filenames'].each do |filename|
              sql = @generator.upload(table, table_meta['fields'], filename, @load_id, @load_at, source)
              execute(sql)

              # if there's something in the reject/except, raise an error
              if File.size?(@generator.except_filename(filename)) || File.size?(@generator.reject_filename(filename))
                raise "Some of the records were rejected: see #{filename}"
              end
            end
          end

          @data_loaded = true
        end

        def load_historization_data(source, downloaded_info, historized_objects_params)
          if ! @data_loaded
            raise "No data loaded, nothing to shuffle to historized datasets. First load data with load_data."
          end

          first_load = first_load?
          (historized_objects_params || []).each do |object, hist_info|
            # if we're doing the first load, load data from history table
            if first_load
              load_from_params = historized_objects_params[object]["load_history_from"]
              # if there's a table to load the history from
              if load_from_params
                @logger.info "First load and load_history_from given, so we're loading data from history object for #{object}" if @logger

                # load data from history tables i.e. OpportunityHistory
                load_from_params = historized_objects_params[object]["load_history_from"]

                # for fields use the history object fields
                if ! downloaded_info['objects'][load_from_params["name"]]
                  raise "The source for historized object #{load_from_params["name"]} is missing in the downloaded info: #{downloaded_info['objects']}"
                end
                fields = downloaded_info['objects'][load_from_params["name"]]['fields']

                load_hist_sql = @generator.history_loading(
                  object,
                  load_from_params,
                  fields,
                  @load_id,
                  @load_at,
                  source
                )
                execute(load_hist_sql)
              end
            else
              @logger.info "Deleting old loads for #{object}" if @logger

              # if it's not the first load only keep the latest load in the merge_from table
              delete_sql = @generator.delete_but_last_load(historized_objects_params[object]["merge_from"]["name"], source)
              execute(delete_sql)
            end
            @logger.info "Merging data into history table for #{object}"
            # merge the data from object tables last load i.e. Opportunity
            merge_sql = @generator.historization_merge(
              object,
              historized_objects_params[object]["merge_from"],
              downloaded_info['objects'][object]['fields'],
              source
            )
            execute(merge_sql)
          end
        end

        def first_load?
          if @first_load.nil?
            raise "you need to load something first to find out if it was the first load"
          end

          return @first_load
        end

        # .each{|t| puts "DROP TABLE dss_#{t};"}

        # save the info about the download
        # return the load id
        def save_download_info(downloaded_info)
          # generate load id
          load_at = Time.now
          load_id = load_at.to_i

          downloaded_info ||= {}

          # check out if it's a new load
          count = execute_select(@generator.load_count, nil, true)
          @first_load = (count == 0)

          # insert it there
          insert_sql = @generator.insert_load(downloaded_info.merge(
            "_LOAD_ID" => load_id,
            "_LOAD_AT" => load_at
          ))
          execute(insert_sql)

          # save it for later
          @load_at = load_at
          @load_id = load_id

          return load_id
        end

        DIRNAME = "tmp"

        # extracts data to be filled in to datasets,
        # writes them to a csv file
        def extract_data(datasources)
          # create the directory if it doesn't exist
          Dir.mkdir(DIRNAME) if ! File.directory?(DIRNAME)

          # extract load info and put it my own params
          @params['load_info'] = get_load_info

          datasources.each do |datasource, datasets|
          # extract each dataset from vertica
            datasets.each do |dataset, ds_structure|

              # if custom sql given
              if ds_structure["extract_sql"]
                # get the sql from the file
                sql = File.open(ds_structure["extract_sql"], 'rb') { |f| f.read }
                columns_gd = nil
              else
                # get the columns and generate the sql
                columns = get_columns(ds_structure)
                columns_gd = columns[:gd]
                sql = @generator.extract(
                  ds_structure["source_object"],
                  columns[:sql],
                  datasource
                )
              end

              name = "tmp/#{dataset}-#{DateTime.now.to_i.to_s}.csv"

              # columns of the sql query result
              sql_columns = nil

              # open a file to write select results to it
              CSV.open(name, 'w', :force_quotes => true) do |csv|

                fetch_handler = lambda do |f|
                  sql_columns = f.columns
                  # write the columns to the csv file as a header
                  csv << sql_columns
                end

                # execute the select and write row by row
                execute_select(sql, fetch_handler) do |row|
                  row_array = sql_columns.map {|col| row[col]}
                  csv << row_array
                end

                if columns_gd && (sql_columns != columns_gd.map {|c| c.to_sym})
                  raise "something is weird, the columns of the sql '#{sql_columns}' aren't the same as the given cols '#{columns_gd}' "
                end
              end

              absolute_path = File.absolute_path(name)
              ds_structure["csv_filename"] = absolute_path
              @logger.info("Written results to file #{absolute_path}") if @logger
            end
          end
          return datasources
        end

        def object_has_field(object, field)
          sql = @generator.column_count(object, field)
          count = execute_select(sql, nil, true)

          return count > 0
        end

        # get columns to be part of the SELECT query .. only when sql needs to be generated
        def get_columns(ds_structure)
          columns_sql = []
          columns_gd = []

          if ds_structure["extract_sql"]
            raise "something is wrong, generating colums for sql when custom sql given"
          end

          columns = ds_structure["columns"]

          # go through all the fields of the dataset
          columns.each do |csv_column_name, s|
            # push the gd short_identifier to list of csv columns
            columns_gd.push(csv_column_name)

            # if it's optional and it's not in the table, return empty
            if s["optional"]
              source_column = s["source_column"]
              if ! source_column
                raise "source column must be given for optional: #{f}"
              end

              if ! object_has_field(ds_structure["source_table"], source_column)
                columns_sql.push("'' AS #{csv_column_name}")
                next
              end
            end

            if !s
              raise "no source given for field: #{f}"
            end

            # if column name given, push it there directly
            if s["source_column"]
              columns_sql.push("#{s['source_column']} AS #{csv_column_name}")
              next
            end

            # same if source_column_expression given
            if s["source_column_expression"]
              columns_sql.push("#{s['source_column_expression']} AS #{csv_column_name}")
              next
            end

            # if there's something to be evaluated, do it
            if s["source_column_concat"]
              # through the stuff to be concated
              concat_strings = s["source_column_concat"].map do |c|
                # if it's a symbol get it from the load params
                if c[0] == ":"
                  param_name = c[1..-1].to_sym
                  param_value = @params['load_info'][param_name]

                  # if it's not in params raise an error
                  if ! param_value
                    raise "The parameter #{param_name} is missing in meta params: #{@params['load_info']}"
                  end
                  "'#{param_value}'"
                else
                  # take the value as it is, including apostrophes if any
                  c
                end
              end
              columns_sql.push("(#{concat_strings.join(' || ')}) AS #{csv_column_name}")
              next
            end
            raise "column or source_column_concat must be given for #{f}"
          end
          return {
            :sql => columns_sql,
            :gd => columns_gd
          }
        end

        def get_load_info
          # get information from the meta table latest row
          # return it in form source_column name -> value
          select_sql = @generator.extract_load_info
          info = {}
          execute_select(select_sql) do |row|
            info.merge!(row)
          end
          return info
        end

        private

        # connect and pass execution to a block
        def connect
          Sequel.connect @params["jdbc_url"],
            :username => @params["gdc_username"],
            :password => @params["gdc_password"] do |connection|
              yield(connection)
          end
        end

        # executes sql (select), for each row, passes execution to block
        def execute_select(sql, fetch_handler=nil, count=false)
          connect do |connection|
            # do the query
            f = connection.fetch(sql)

            @logger.info("Executing sql: #{sql}") if @logger
            # if handler was passed call it
            if fetch_handler
              fetch_handler.call(f)
            end

            if count
              return f.first[:count]
            end

            # go throug the rows returned and call the block
            return f.each do |row|
              yield(row)
            end
          end
        end

        # execute sql, return nothing
        def execute(sql_strings)
          if ! sql_strings.kind_of?(Array)
            sql_strings = [sql_strings]
          end
          connect do |connection|
            sql_strings.each do |sql|
              @logger.info("Executing sql: #{sql}") if @logger
              connection.run(sql)
            end
          end
        end

      end
    end
  end
end