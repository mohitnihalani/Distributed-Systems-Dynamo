defmodule DynamoNode do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  import Ring
  """
  Dynamo Node
  Each node has it's view of the state, which basically means each node has its own ring
  Node can be initiated using the seed node or without the seed node
  Seed node is used so that node can connect to the ring when joined initially
  If seed node is provided in the confguration, it will try to connect to the seed node using gossip protocol
  and join the states.

  """
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

  defp sync_state(ring1, ring2) do
    Ring.sync_rings(ring1, ring2)
  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 5000
  end

  defp save_heartbeat_timer(node, timer) do
    %{node | heartbeat_timer: timer}
  end

  defp reset_gossip_timeout(node) do
    if node.heartbeat_timer, do: Emulation.cancel_timer(node.heartbeat_timer)
    gossip_timeout = get_gossip_timeout(node)
    save_heartbeat_timer(node, Emulation.timer(node.heartbeat_timeout))
  end

  def join_ring(state, otherstate) do
    Ring.sync_rings(state, otherstate)
  end

  def run_gossip_protocol(node) do
    :rand.seed(:exrop, {101, 102, 103})
    random_node = Enum.random(node.state.nodes)
    send(random_node, {:share_state, node.state})
    node = reset_gossip_timeout(node)

    receive do
      {_sender, {:share_state_reply, %Ring{} = joined_state}} ->
        node = %{node | state: sync_state(node.state, joined_state)}
        run_node(node)
      :timer ->
        run_node(node)
    end
  end

  @spec lauch_node(%DynamoNode{}) :: no_return()
  def lauch_node(node) do
    if node.seed_node != nil do

    end
  end

  """
  This is where most recursion happend maintaing state
  """
  defp run_node(node) do

  end
end
