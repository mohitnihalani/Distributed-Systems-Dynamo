defmodule ReplicationTest do
  use ExUnit.Case
  doctest Replication

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  test "greets the world" do
    assert Replication.hello() == :world
  end

  test "Test Replication Manager Working" do
    Emulation.init()
    spawn(:rep, fn -> Replication.init() end)

    send(:rep, :ok)

    # Wait for election
    receive do
    after
      3000 -> :ok
    end

    receive do
      {sender, value} ->
        # code
    end
  after
    Emulation.terminate()
  end
end
