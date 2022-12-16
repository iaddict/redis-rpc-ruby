# -*- encoding: utf-8 -*-
require File.expand_path('../lib/redis-rpc/version', __FILE__)

Gem::Specification.new do |s|
  s.name = 'redis-rpc'
  s.version = RedisRpc::VERSION
  s.license = 'GPLv3'
  s.authors = ['Phuong Nguyen', 'Nathan Farrington']
  s.email = ['nathan@nathanfarrington.com']

  s.homepage = 'http://github.com/phuongnd08/redis-rpc-ruby'
  s.summary = 'Lightweight RPC for Redis'
  s.description = <<-DESCRIPTION
    RedisRpc is the easiest to use RPC library in the world. (No small claim!).
    This version is a repackage that only has Ruby implementation.

    Redis is a powerful in-memory data structure server that is useful for building
    fast distributed systems. Redis implements message queue functionality with its
    use of list data structures and the `LPOP`, `BLPOP`, and `RPUSH` commands.
    RedisRpc implements a lightweight RPC mechanism using Redis message queues to
    temporarily hold RPC request and response messages. These messages are encoded
    as JSON strings for portability.

    Many other RPC mechanisms are either programming language specific (e.g.
    Java RMI) or require boiler-plate code for explicit typing (e.g. Thrift).
    RedisRpc was designed to be extremely easy to use by eliminating boiler-plate
    code while also being programming language neutral.  High performance was not
    an initial goal of RedisRpc and other RPC libraries are likely to have better
    performance. Instead, RedisRpc has better programmer performance; it lets you
    get something working immediately.
  DESCRIPTION

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_runtime_dependency 'redis'

  s.add_development_dependency 'bundler'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'byebug'
end
