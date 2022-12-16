# Copyright (C) 2012.  Nathan Farrington <nfarring@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require File.expand_path('../redis-rpc/version',__FILE__)
require 'multi_json'

require 'redis'

module RedisRpc

  class RemoteException < StandardError
    attr_reader :remote_backtrace
    def initialize(message, remote_backtrace)
      super(message)
      @remote_backtrace = remote_backtrace
    end
  end

  class TimeoutException < StandardError; end

  class MalformedResponseException < RemoteException
    def initialize(response)
      super "Malformed RPC Response message: #{response.inspect}", []
    end
  end
  class MalformedRequestException < ArgumentError
    def initialize(reason)
      super "Malformed RPC Request: #{reason.inspect}"
    end
  end

  class Client
    def initialize( redis_server, message_queue, timeout=0 )
      @redis_server = redis_server
      @message_queue = message_queue
      @timeout = timeout
    end

    alias :send! :send

    def get_timeout_at
      # allow mock to manipulate timeout to verify safety behavior
      Time.now.to_i + @timeout + 60
    end

    def send( method_name, *args)
      raise MalformedRequestException, 'block not allowed over RPC' if block_given?

      # request setup
      function_call = {'name' => method_name.to_s, 'args' => args}
      response_queue = @message_queue + ':rpc:' + rand_string
      rpc_request = {
        'function_call' => function_call, 'response_queue' => response_queue,
        'timeout_at' => get_timeout_at,
      }

      rpc_raw_request = MultiJson.dump rpc_request

      # transport
      @redis_server.rpush @message_queue, rpc_raw_request
      message_queue, rpc_raw_response = @redis_server.blpop response_queue, @timeout
      raise TimeoutException if rpc_raw_response.nil?

      # response handling
      rpc_response = MultiJson.load rpc_raw_response
      raise RemoteException.new(rpc_response['exception'], rpc_response['backtrace']) if rpc_response.has_key? 'exception'
      raise MalformedResponseException, rpc_response unless rpc_response.has_key? 'return_value'
      return rpc_response['return_value']

    rescue TimeoutException, SignalException
      # stale request cleanup
      @redis_server.lrem @message_queue, 0, rpc_raw_request
      raise $!
    end

    alias :method_missing :send

    def respond_to?( method_name )
      send( :respond_to?, method_name )
    end

    private

    def rand_string(size=8)
      return rand(36**size).to_s(36).upcase.rjust(size,'0')
    end
  end

  class Server
    def initialize( redis_server, message_queue, local_object, timeout=nil, verbose: false)
      @redis_server = redis_server
      @message_queue = message_queue
      @local_object = local_object
      @timeout = timeout
      @verbose = verbose
    end

    def run
      loop{ run_one }
    end

    def run!
      flush_queue!
      run
    end

    def flush_queue!
      @redis_server.del @message_queue
    end

    private

    def run_one
      # request setup
      message_queue, rpc_raw_request = @redis_server.blpop @message_queue, timeout
      return nil if rpc_raw_request.nil?
      rpc_request = MultiJson.load rpc_raw_request
      response_queue = rpc_request['response_queue']
      function_call = rpc_request['function_call']

      # request execution
      begin
        if rpc_request['timeout_at'].nil?
          raise "Unsafe RPC call: timeout_at not specified"
        end

        if rpc_request['timeout_at'] < Time.now.to_i
          raise "Expired RPC call. timeout_at = #{rpc_request['timeout_at']}. Time.now = #{Time.now.to_i}"
        end

        return_value = @local_object.send( function_call['name'].to_sym, *function_call['args'] )
        rpc_response = {'return_value' => return_value}
      rescue Exception => err
        rpc_response = {'exception' => err.to_s, 'backtrace' => err.backtrace}
      end

      if @verbose
        p rpc_response
      end

      # response tansport
      rpc_raw_response = MultiJson.dump rpc_response
      @redis_server.multi do
        @redis_server.rpush response_queue, rpc_raw_response
        @redis_server.expire response_queue, 1
      end
      true
    end

    def timeout
      @timeout or
        $REDISRPC_SERVER_TIMEOUT or
      0
    end
  end
end
