module GoodData
  module Connectors
    module Ads
      class HistoryWrapperTask < Task

        def initialize(entity)
          super('History Wrapper Task',entity)
          self << DropStageTableTask.new(@entity)
          self << CreateStageTableTask.new(@entity)
          self << ImportDataFromStageToTempStageTask.new(@entity)
          self << ImportDataFromHistoryToTempStageTask.new(@entity)
          self << ImportDataFromEntityToTempStageTask.new(@entity)
          self << ImportDataFromDeletedToTempStageTask.new(@entity)

          transactional_task = Task.new("Integration Transaction Task",@entity)
          transactional_task << DropTemporaryTableTask.new(@entity)
          transactional_task << CreateTemporaryTableTask.new(@entity)
          transactional_task << IntegrateHistoricalDataTask.new(@entity)
          transactional_task << MergeHistoricalDataTask.new(@entity)
          transactional_task << DropStageTableTask.new(@entity)
          transactional_task << DropHistoryForInputTask.new(@entity)
          transactional_task << DropTableForInputTask.new(@entity)
          transactional_task.set_one_command(true)
          self << transactional_task
        end

      end
    end
  end
end