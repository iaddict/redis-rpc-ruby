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

    it 'should echo' do
      expect(hall.echo(1, kwarg1: :kw1)).to be == { "args1" => 1, "kwarg1" => "kw1", "kwarg2" => "kwarg2" }
    end
  end
end
