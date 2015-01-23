
class CCassandra
  # A helper module you can include in your own class. Makes it easier
  # to work with CCassandra subclasses.
  module Constants
    include CCassandra::Consistency

    Long = CCassandra::Long
    OrderedHash = CCassandra::OrderedHash
  end
end
