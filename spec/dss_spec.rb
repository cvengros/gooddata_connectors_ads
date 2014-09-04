require 'spec_helper'
require 'sequel'

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

  def get_load_params(source, csv)
    {
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
  end
  describe "save_full" do
    it "saves all the stuff to dss with no history" do
      # create a dss instance
      prefix = "testing#{rand(999)}"
      source = "test_source"
      dss = GoodData::Connectors::Storage::Dss.new(nil, get_params(prefix))

      # do the load params and load the data
      load_params = get_load_params(source, 'spec/Bike.csv')
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
      load_params = get_load_params(source, 'spec/Bike.csv')
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
      dss.save_full(get_load_params(source,'spec/Bike2.csv'))
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
  end
end