module GoodData
  module Connectors
    module Ads
      class Task < Node
        attr_accessor :sql,:entity
        attr_reader :one_command,:history,:default


        def initialize(name,entity)
          super(name)
          @entity = entity
          @history = false
          @one_command = false
          @default = false
        end


        def set_history(value)
          @history = value
          if !leaf?
            children.each { |child_task| child_task.set_history(value) }
          end
        end

        def set_one_command(value)
          @one_command = value
          if !leaf?
            children.each { |child_task| child_task.set_one_command(value) }
          end
        end

        def set_default(value)
          @default = value
          if !leaf?
            children.each { |child_task| child_task.set_default(value) }
          end
        end


        def create_command
          raise "Called empty create command"
        end



        def get_sql
          if leaf?
            create_command
            @sql
          else
            merged_sql = ""
            children.each { |child_task| merged_sql += child_task.get_sql }
            merged_sql
          end
        end

        def run_sql
          if leaf?
            create_command
            Helper.logger.info "Executing SQL from command #{@name}"
            Helper.logger.debug @sql
            Helper.logger.info "Result for command #{@name}:" + Benchmark.measure { Connection.db.run(@sql) }.to_s
          else
            if (@one_command)
              Helper.logger.info "Executing nested SQL from command #{@name}"
              Helper.logger.debug get_sql
              Helper.logger.info "Result for command #{@name}:" + Benchmark.measure { Connection.db.run(get_sql) }.to_s
            else
              children.each { |child_task| child_task.run_sql }
            end
          end
        end

      end
    end
  end
end