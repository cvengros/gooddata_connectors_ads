module GoodData
  module Connectors
    module Ads
      class ImportDataTask < Task

        def initialize(entity,files_to_process,history)
          super('Import Data',entity)
          self << DropTableForInputTask.new(@entity)
          self << CreateTableForInputTask.new(@entity)
          self << SetTimezoneTask.new(@entity)
          paraller_task = ParallerTask.new(@entity)
          files_to_process.each_with_index do |file,index|
            paraller_task << CopyFromLocalForInputTask.new(@entity,file,index)
          end
          self << paraller_task
          set_history(history)
        end
      end
    end
  end
end