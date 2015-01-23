
require 'pp'

class CCassandraThrift::CCassandra::Client
  def send_message(*args)
    pp args
    super
  end
end
