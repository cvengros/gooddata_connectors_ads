module GoodData
  module Connectors
    module Ads
      class ImportDataTask < Task

        def initialize(entity,files_to_process,history)
          super('Import Data',entity)
          self << DropTableForInputTask.new(@entity)
          self << CreateTableForInputTask.new(@entity)
          files_to_process.each_with_index do |file,index|
            self << CopyFromLocalForInputTask.new(@entity,file,index)
          end
          set_history(history)
        end

      end
    end
  end
end