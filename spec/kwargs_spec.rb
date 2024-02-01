require_relative 'spec_helper'

class KwargsEcho
  def echo(args1, kwarg1:, kwarg2: :kwarg2)
    {args1: args1, kwarg1: kwarg1, kwarg2: kwarg2}
  end
end

describe KwargsEcho do
  context "locally" do
    let(:hall) { KwargsEcho.new }

    it 'should echo' do
      expect(hall.public_methods(true).include?(:echo)).to eq(true)
      expect(hall.echo(1, kwarg1: :kw1)).to be == {args1: 1, kwarg1: :kw1, kwarg2: :kwarg2}
    end
  end

  context "over rpc" do
    let(:rpc_server_builder) { lambda { RedisRpc::Server.new(Redis.new($REDIS_CONFIG), 'hall', KwargsEcho.new, logger: Logger.new(STDERR)) } }
    before(:each) do
      @rpc_server = Thread.start {
        rpc_server_builder.call.run
      }
    end
    after(:each) { rpc_server_builder.call.stop! && @rpc_server.kill; rpc_server_builder.call.flush_queue! }
    let(:hall) { RedisRpc::Client.new($REDIS, 'hall', timeout: 2) }

    it 'should respond_to?(:echo)' do
      expect(hall.respond_to?(:echo)).to eq(true)
    end

    it 'should not respond_to?(:sleep)' do
      expect(hall.respond_to?(:sleep)).to eq(false)
    end

    it 'should not respond_to?(:instance_eval)' do
      expect(hall.respond_to?(:instance_eval)).to eq(false)
    end

    it 'should echo' do
      expect(hall.echo(1, kwarg1: :kw1)).to be == { "args1" => 1, "kwarg1" => "kw1", "kwarg2" => "kwarg2" }
    end
  end
end
