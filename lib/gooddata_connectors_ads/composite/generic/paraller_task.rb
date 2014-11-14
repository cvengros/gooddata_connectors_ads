module GoodData
  module Connectors
    module Ads
      class ParallerTask < Task

        def initialize(entity, number_of_paraller_queries = 4)
          super("Paraller task",entity)
          @number_of_paraller_queries = number_of_paraller_queries

        end



        def run_sql
          if leaf?
            fail "Paraller task cannot be leaf"
          else
            if (@one_command)
              fail "Paraller task cannot be used with one_command option"
            else
              queue = Queue.new
              children.each { |child_task| queue <<  child_task.get_sql }
              threads = []
              @number_of_paraller_queries.times do
                threads << Thread.new do |i|
                  # loop until there are no more things to do
                  until queue.empty?
                    # pop with the non-blocking flag set, this raises
                    # an exception if the queue is empty, in which case
                    # work_unit will be set to nil
                    work_unit = queue.pop(true) rescue nil
                    if work_unit
                      Helper.logger.info "Executing SQL from command #{@name} in thread #{i}"
                      Helper.logger.debug work_unit
                      Helper.logger.info "Result for command #{@name} in thread #{i}:" + Benchmark.measure { Connection.db.run(work_unit) }.to_s
                    end
                  end
                  # when there is no more work, the thread will stop
                end
              end
              threads.each { |t| t.join }
            end
          end
        end




      end
    end
  end
end
