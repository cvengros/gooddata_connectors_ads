require 'spec_helper'
require 'sequel'
require 'erb'

FILE1_PATH = File.expand_path('spec/Bike.csv')
FILE2_PATH = File.expand_path('spec/Bike2.csv')
OUT_DIRNAME = 'tmp_test'
SQL_FILE = "spec/bike.sql"

class Hash
  def deep_merge(second)
    merger = proc { |key, v1, v2| v1.is_a?(Hash) && v2.is_a?(Hash) ? v1.merge(v2, &merger) : v2 }
    merge(second, &merger)
  end
end

describe GoodData::Connectors::Storage::Dss do
  def get_params(prefix, historization=false)
    pars = {
      'config' => {
        'storage' => {
          'dss' => {
            'gdc_username' => ENV["gdc_username"],
            'gdc_password' => ENV["gdc_password"],
            'jdbc_url' => ENV["jdbc_url"],
            'table_name_prefix' => prefix,
          }
        }
      },
      "GDC_LOGGER" => Logger.new(STDOUT)
    }
    if historization
      pars['config']['storage']['dss']['historized_objects'] = {
        "Bike" => {
          "merge_from" => {
            "name" => "Bike",
            "column_mapping" => {
              "_VALID_FROM" => "_LOAD_AT"
            }
          }
        }
      }
    end
    return pars
  end

  def get_load_params(source, csv, meta=false)
    pars = {
      'local_files' => {
        source => {
          "objects" => {
            "Bike" => {
              "fields" => [{
                "name" => "Id",
                "type" => "identifier"
              },{
                "name" => "WheelSize",
                "type" => "double"
              },{
                "name" => "Manufacturer",
                "type" => "string"
              }],
              "filenames" => [csv]
            }
          }
        }
      }
    }

    if meta
      pars['local_files'][source]['meta'] = {
        "market" => "US"
      }
    end
    return pars
  end

  def get_extract_params(source, custom_sql=false)
    pars = {
      "config" => {
        "storage" => {
          "dss" => {
            "dataset_mapping" =>  {
              source => {
                "bike" => nil
              }
            },
            'output_dirname' => OUT_DIRNAME
          }
        }
      }
    }

    pars['config']['storage']['dss']["dataset_mapping"][source]["bike"] = custom_sql ? {
      "extract_sql" => SQL_FILE
    } : {
      "source_object" => "Bike",
      "columns" => {
        "id" => {
          "source_column" => "Id"
        },
        "wheel_size" => {
          "source_column" => "WheelSize"
        },
        "manufacturer" => {
          "source_column" => "Manufacturer"
        },
        "manufacturer_wheelsize" => {
          "source_column_concat" => [
            "Manufacturer",
            "WheelSize"
          ]
        },
        "distributor" => {
          "source_column_concat" => [
            ":market",
            "Manufacturer"
          ]
        }
      }
    }
    return pars
  end

  describe "save_full" do
    it "saves all the stuff to dss with no history" do
      # create a dss instance
      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix))

      # do the load params and load the data
      load_params = get_load_params(source, FILE1_PATH)
      dss.save_full(load_params)

      # check that it's there
      Sequel.connect ENV["jdbc_url"], :username => ENV["gdc_username"], :password => ENV["gdc_password"] do |conn|
        table_name = "#{prefix}_#{source}_Bike_in"
        conn.fetch "SELECT * FROM #{table_name}" do |row|
          # check there's something in each row
          row[:id].should_not be_nil
          row[:wheelsize].should_not be_nil
          row[:manufacturer].should_not be_nil
          row[:_diff_hash].should_not be_nil
          row[:_load_id].should_not be_nil
          # check it has all the cols it should have
        end

        # and that the row count is right
        f = conn.fetch "SELECT COUNT(*) FROM #{table_name}"
        f.first[:count].should be(4)
      end
    end
    it "saves all the stuff to dss with history" do
      # create a dss instance
      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix, true))

      # do the load params and load the data
      load_params = get_load_params(source, FILE1_PATH)
      dss.save_full(load_params)

      Sequel.connect ENV["jdbc_url"], :username => ENV["gdc_username"], :password => ENV["gdc_password"] do |conn|
        table_name = "#{prefix}_#{source}_Bike"
        conn.fetch "SELECT * FROM #{table_name}" do |row|
          # valid from should be the load time
          row[:_valid_from].should_not be_nil
          row[:_valid_from].should eql(row[:_load_at])
        end
      end

      # save another load
      dss.save_full(get_load_params(source, FILE2_PATH))
      Sequel.connect ENV["jdbc_url"], :username => ENV["gdc_username"], :password => ENV["gdc_password"] do |conn|
        table_name = "#{prefix}_#{source}_Bike"

        # there's one extra line and one changed line => 6
        f = conn.fetch "SELECT COUNT(*) FROM #{table_name}"
        f.first[:count].should be(6)

        f = conn.fetch "SELECT * FROM #{table_name} WHERE Id='3'"
        h = f.all
        # the line that changed should be changed
        h[0][:manufacturer].should_not eql(h[1][:manufacturer])

        # valid from should raise
        h[0][:_valid_from].should_not eql(h[1][:_valid_from])

      end
    end
    it "saves the metadata to a loads table" do
      # create a dss instance
      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix))

      # do the load params and load the data
      load_params = get_load_params(source, FILE1_PATH, true)
      dss.save_full(load_params)
      table_name = "#{prefix}_#{source}_meta_loads"

      # check that it's there
      Sequel.connect ENV["jdbc_url"], :username => ENV["gdc_username"], :password => ENV["gdc_password"] do |conn|
        f = conn.fetch "SELECT * FROM #{table_name}"
        row = f.first

        # metadata should be there
        row[:market].should eql("US")
        # load id as well
        row[:_load_id].should_not be_nil
      end
    end
  end

  describe "extract" do
    it "extracts using generated sqls" do
      # load something there

      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix))

      # do the load params and load the data, with meta
      load_params = get_load_params(source, FILE1_PATH, true)
      dss.save_full(load_params)

      # unload it
      extract_params = get_extract_params(source).deep_merge(get_params(prefix))
      dss2 = GoodData::Connectors::Storage::Dss.new(nil, extract_params)
      ext_out = dss2.extract

      # see what's there
      arr_data = CSV.read(ext_out['test_source']['bike']['csv_filename'])

      # 5 cols
      arr_data[0].length.should eql(5)

      # choose a record
      r1 = arr_data.select{|a| a[0] == "1"}[0]

      # the number
      r1[1].to_f.should eql(29.0)

      # the strings and the concats
      r1[2].should eql('Specialized')
      r1[3].should eql('Specialized29')
      r1[4].should eql('USSpecialized')
    end


    SQL = "SELECT COUNT(*), Manufacturer FROM <%= table_name %> GROUP BY Manufacturer"

    it "extracts something using custom sql" do
      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix))

      # do the load params and load the data, with meta
      load_params = get_load_params(source, FILE1_PATH, true)
      dss.save_full(load_params)

      # process the sql file
      table_name = "#{prefix}_#{source}_Bike_in"
      renderer = ERB.new(SQL)
      File.open(SQL_FILE, 'w') {|f| f.write(renderer.result(binding))}

      extract_params = get_extract_params(source, true).deep_merge(get_params(prefix))
      dss2 = GoodData::Connectors::Storage::Dss.new(nil, extract_params)

      # unload it
      ext_out = dss2.extract

      # see what's there
      arr_data = CSV.read(ext_out['test_source']['bike']['csv_filename'])
      arr_data[0].length.should eql(2)

      # scott should have 2 lines
      scott = arr_data.select{|a| a[1] == "Scott"}[0]
      scott[0].should eql("2")

      # GT 1 line
      gt = arr_data.select{|a| a[1] == "GT"}[0]
      gt[0].should eql("1")

    end

  end
end