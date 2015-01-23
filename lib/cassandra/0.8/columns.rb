class CCassandra
  module Columns #:nodoc:
    def _standard_counter_mutation(column_family, column_name, value)
      CCassandraThrift::Mutation.new(
        :column_or_supercolumn => CCassandraThrift::ColumnOrSuperColumn.new(
          :counter_column => CCassandraThrift::CounterColumn.new(
            :name      => column_name_class(column_family).new(column_name).to_s,
            :value     => value
          )
        )
      )
    end

    def _super_counter_mutation(column_family, super_column_name, sub_column, value)
      CCassandraThrift::Mutation.new(:column_or_supercolumn =>
        CCassandraThrift::ColumnOrSuperColumn.new(
          :counter_super_column => CCassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(super_column_name).to_s,
            :columns => [CCassandraThrift::CounterColumn.new(
              :name      => sub_column_name_class(column_family).new(sub_column).to_s,
              :value     => value
            )]
          )
        )
      )
    end
  end
end
