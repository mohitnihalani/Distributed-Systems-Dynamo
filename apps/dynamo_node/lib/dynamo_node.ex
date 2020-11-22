defmodule DynamoNode do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  import Ring

  defstruct(
    state: nil,
    version: nil,
    pid: nil,
    heartbeat_timer: nil,
    seed_node: nil,
    heartbeat_timeout: 1000
  )


  def init() do
    hash_ring = Ring.new()
    node = %DynamoNode{state: hash_ring, version: nil, pid: whoami()}
  end

  """
  With Seed Node
  """
  def init(seed_node) do
    hash_ring = Ring.new()
    node = %DynamoNode{state: hash_ring, version: nil, pid: whoami(), seed_node: seed_node}
  end

  defp synch_with_seed_node(node) do
    if node.seed_node == nil do
      node
    else

    end
  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 5000
  end

  defp save_heartbeat_timer(node, timer) do
    %{node | heartbeat_timer: timer}
  end

  defp set_gossip_timeout(node) do
    if node.heartbeat_timer, do: Emulation.cancel_timer(node.heartbeat_timer)
    gossip_timeout = get_gossip_timeout(node)
    save_heartbeat_timer(node, Emulation.timer(node.heartbeat_timeout))
  end

  def join_ring(state, otherstate) do
    Ring.sync_rings(state, otherstate)
  end
end
