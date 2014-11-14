module GoodData
  module Connectors
    module Ads


        class Connection

        class << self

          Jdbc::DSS.load_driver
          Java.com.gooddata.dss.jdbc.driver.DssDriver

          def set_up(instance_id,username,password,server)
            @instance_id = instance_id
            @username = username
            @password = password
            @status = "ready"
            @server = server
          end

          def connect
            dss_jdbc_url = "jdbc:dss://#{@server}/gdc/dss/instances/#{@instance_id}"
            $log.info "Connecting to #{dss_jdbc_url}"
            @db = Sequel.connect(dss_jdbc_url, :username=> @username, :password=> @password)
            @status = "connected"
          end

          def disconnect
            @status = "ready"
            @db.disconnect
          end


          def db
            connect if (@status != "connected")
            @db
          end

          def fetch(sql)
            connect if (@status != "connected")
            @db.fetch(sql)
          end

        end



      end


    end
  end
end