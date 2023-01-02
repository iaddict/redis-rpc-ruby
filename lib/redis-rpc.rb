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

require_relative 'redis-rpc/version'
require 'json'

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
    def initialize(redis_server, message_queue, timeout: 0)
      @redis_server = redis_server
      @message_queue = message_queue
      @timeout = timeout
    end

    alias :send! :send

    def get_timeout_at
      # allow mock to manipulate timeout to verify safety behavior
      Time.now.to_i + @timeout + 60
    end

    def send(method_name, *args, **kwargs)
      raise MalformedRequestException, 'block not allowed over RPC' if block_given?

      # request setup
      function_call = { 'name' => method_name.to_s, 'args' => args, 'kwargs' => kwargs }
      response_queue = @message_queue + ':rpc:' + rand_string
      rpc_request = {
        'function_call' => function_call,
        'response_queue' => response_queue,
        'timeout_at' => get_timeout_at,
      }

      rpc_raw_request = JSON.dump rpc_request

      # transport
      @redis_server.rpush @message_queue, rpc_raw_request
      _message_queue, rpc_raw_response = @redis_server.blpop response_queue, timeout: @timeout
      raise TimeoutException if rpc_raw_response.nil?

      # response handling
      rpc_response = JSON.parse(rpc_raw_response)
      raise RemoteException.new(rpc_response['exception'], rpc_response['backtrace']) if rpc_response.has_key? 'exception'
      raise MalformedResponseException, rpc_response unless rpc_response.has_key? 'return_value'

      # noinspection RubyUnnecessaryReturnStatement
      return rpc_response['return_value']
    rescue TimeoutException, SignalException
      # stale request cleanup
      @redis_server.lrem @message_queue, 0, rpc_raw_request
      raise
    end

    alias :method_missing :send

    def respond_to?(method_name)
      send(:respond_to?, method_name)
    end

    private

    def rand_string(size = 8)
      rand(36 ** size).to_s(36).upcase.rjust(size, '0')
    end
  end

  class Server
    attr_reader :timeout
    attr_accessor :logger

    # @param [Redis] redis_server
    # @param [String] message_queue
    # @param [Object] local_object
    # @param [nil,Numeric] timeout
    # @param [Integer] response_expiry
    # @param [Boolean] verbose
    # @param [nil,Logger] logger
    def initialize(redis_server, message_queue, local_object, timeout: nil, response_expiry: 1, verbose: false, logger: nil)
      @redis_server = redis_server
      @message_queue = message_queue
      @local_object = local_object
      @timeout = timeout || 0
      @response_expiry = response_expiry
      @verbose = verbose
      @logger = logger
    end

    def run
      catch(:stop!) do
        loop { run_one }
        logger&.info("[#{Time.now}] #{self.class.name} : action=run stopped")
      end
    end

    def run!
      flush_queue!
      run
    end

    def stop!
      client = Client.new(@redis_server.dup, @message_queue)
      client.send(stop_message)
    end

    def flush_queue!
      @redis_server.del @message_queue
    end

    private

    def stop_message
      @stop_message ||= "stop-#{@message_queue}"
    end

    def run_one
      # request setup
      _message_queue, rpc_raw_request = @redis_server.blpop @message_queue, timeout: timeout
      return nil if rpc_raw_request.nil?

      rpc_request = JSON.parse(rpc_raw_request)
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

        if stop_message == function_call['name']
          send_response(response_queue, { 'return_value' => true })
          throw :stop!
        end

        logger&.info("[#{Time.now}] #{self.class.name} : action=run_one rpc_call=#{@local_object.class.name}##{function_call['name']}(#{function_call['args']})")

        function_call['kwargs'].transform_keys!(&:to_sym) if function_call['kwargs']&.kind_of?(Hash)
        return_value = @local_object.send(function_call['name'].to_sym, *function_call['args'], **function_call['kwargs'])
        rpc_response = { 'return_value' => return_value }
      rescue StandardError => err
        rpc_response = { 'exception' => err.to_s, 'backtrace' => err.backtrace }
      end

      send_response(response_queue, rpc_response)
    end

    def send_response(response_queue, rpc_response)
      if @verbose
        p rpc_response
      end

      # response transport
      rpc_raw_response = JSON.dump rpc_response
      @redis_server.multi do |pipeline|
        pipeline.rpush response_queue, rpc_raw_response
        pipeline.expire response_queue, @response_expiry
      end

      true
    end
  end
end
