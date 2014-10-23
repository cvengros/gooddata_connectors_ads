
module GoodData
  module Connectors
    module Ads
      class TypeConverter


        class << self

          def to_database_type(type)
            output = ""
            case type
              when Metadata::BooleanType
                  output = "BOOLEAN"
              when Metadata::DateType
                if (type.with_time?)
                  output = "DATETIME"
                else
                  output = "DATE"
                end
              when Metadata::DecimalType
                  output = "DECIMAL(#{type.size},#{type.size_after_comma})"
              when Metadata::IntegerType
                  output = "INTEGER"
              when Metadata::StringType
                  output = "VARCHAR(#{type.size})"
              when Metadata::BigIntegerType
                output = "BIGINTEGER"
            end
            output
          end

          def from_database_type(db_column_row)
            case db_column_row[:data_type_id]
              # Boolean
              when 5
                type = "boolean"
              # Integer
              when 6
                type = "integer"
              # Varchar
              when 9
                type = "string-#{db_column_row[:data_type_length]}"
              # Date
              when 10
                type = "date-false"
              #Timestamp
              when 12
                type = "date-true"
              #Numeric - Decimal
              when 16
                type = "decimal-#{db_column_row[:numeric_precision]}-#{db_column_row[:numeric_scale]}"
              else
                $log.info "Unsupported database type #{db_column_row[:data_type_id]} (#{db_column_row[:data_type]}) - using string(255) as default value"
                type = "string-255"
            end

          end



        end
      end

    end
  end
end
