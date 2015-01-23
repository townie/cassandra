class CCassandra
  # A bunch of crap, mostly related to introspecting on column types
  module Columns #:nodoc:
    private

    def is_super(column_family)
      @is_super[column_family] ||= column_family_property(column_family, 'Type') == "Super"
    end

    def column_name_class(column_family)
      @column_name_class[column_family] ||= column_name_class_for_key(column_family, "CompareWith")
    end

    def sub_column_name_class(column_family)
      @sub_column_name_class[column_family] ||= column_name_class_for_key(column_family, "CompareSubcolumnsWith")
    end

    def column_family_property(column_family, key)
      unless schema[column_family]
        raise AccessError, "Invalid column family \"#{column_family}\""
      end
      schema[column_family][key]
    end

    def _standard_insert_mutation(column_family, column_name, value, timestamp, _=nil)
      CCassandraThrift::Mutation.new(
        :column_or_supercolumn => CCassandraThrift::ColumnOrSuperColumn.new(
          :column => CCassandraThrift::Column.new(
            :name      => column_name_class(column_family).new(column_name).to_s,
            :value     => value,
            :timestamp => timestamp
          )
        )
      )
    end

    def _super_insert_mutation(column_family, super_column_name, sub_columns, timestamp, _=nil)
      CCassandraThrift::Mutation.new(:column_or_supercolumn =>
        CCassandraThrift::ColumnOrSuperColumn.new(
          :super_column => CCassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(super_column_name).to_s,
            :columns => sub_columns.collect { |sub_column_name, sub_column_value|
              CCassandraThrift::Column.new(
                :name      => sub_column_name_class(column_family).new(sub_column_name).to_s,
                :value     => sub_column_value.to_s,
                :timestamp => timestamp
              )
            }
          )
        )
      )
    end

    # General info about a deletion object within a mutation
    # timestamp - required. If this is the only param, it will cause deletion of the whole key at that TS
    # supercolumn - opt. If passed, the deletes will only occur within that supercolumn (only subcolumns
    #               will be deleted). Otherwise the normal columns will be deleted.
    # predicate - opt. Defines how to match the columns to delete. if supercolumn passed, the slice will
    #               be scoped to subcolumns of that supercolumn.

    # Deletes a single column from the containing key/CF (and possibly supercolumn), at a given timestamp.
    # Although mutations (as opposed to 'remove' calls) support deleting slices and lists of columns in one shot, this is not implemented here.
    # The main reason being that the batch function takes removes, but removes don't have that capability...so we'd need to change the remove
    # methods to use delete mutation calls...although that might have performance implications. We'll leave that refactoring for later.
    def _delete_mutation(cf, column, subcolumn, timestamp, options={})
      deletion_hash = {:timestamp => timestamp}
      if is_super(cf)
        deletion_hash[:super_column] = column if column
        deletion_hash[:predicate] = CCassandraThrift::SlicePredicate.new(:column_names => [subcolumn]) if subcolumn
      else
        deletion_hash[:predicate] = CCassandraThrift::SlicePredicate.new(:column_names => [column]) if column
      end
      CCassandraThrift::Mutation.new(
        :deletion => CCassandraThrift::Deletion.new(deletion_hash)
      )
    end
  end
end
