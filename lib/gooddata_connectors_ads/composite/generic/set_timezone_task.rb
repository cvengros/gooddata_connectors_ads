module GoodData
  module Connectors
    module Ads
      class SetTimezoneTask < Task

        def initialize(entity)
          super("Set Timezone Task",entity)

        end

        def create_command
          input = {}
          @sql = Base::Templates.make("set_timezone",input)
        end
      end
    end
  end
end
