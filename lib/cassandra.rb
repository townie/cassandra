require 'rubygems'
gem 'thrift_client', '~> 0.7'
require 'thrift_client'
gem 'simple_uuid' , '~> 0.3'
require 'simple_uuid'

require 'json' unless defined?(JSON)

here = File.expand_path(File.dirname(__FILE__))

class CCassandra ; end
unless CCassandra.respond_to?(:VERSION)
  require "#{here}/cassandra/0.8"
end

$LOAD_PATH << "#{here}/../vendor/#{CCassandra.VERSION}/gen-rb"
require "#{here}/../vendor/#{CCassandra.VERSION}/gen-rb/cassandra"

$LOAD_PATH << "#{here}"

require 'cassandra/helpers'
require 'cassandra/array'
require 'cassandra/time'
require 'cassandra/comparable'
require 'cassandra/long'
require 'cassandra/composite'
require 'cassandra/dynamic_composite'
require 'cassandra/ordered_hash'
require 'cassandra/columns'
require 'cassandra/protocol'
require 'cassandra/batch'
require "cassandra/#{CCassandra.VERSION}/columns"
require "cassandra/#{CCassandra.VERSION}/protocol"
require "cassandra/cassandra"
require "cassandra/#{CCassandra.VERSION}/cassandra"
unless CCassandra.VERSION.eql?("0.6")
  require "cassandra/column_family"
  require "cassandra/keyspace"
end
require 'cassandra/constants'
require 'cassandra/debug' if ENV['DEBUG']

begin
  require "cassandra_native"
rescue LoadError
  puts "Unable to load cassandra_native extension. Defaulting to pure Ruby libraries."
end
