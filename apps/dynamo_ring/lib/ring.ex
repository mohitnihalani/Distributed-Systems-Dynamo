defmodule Ring do
  @moduledoc """
  Documentation for `Ring`.
  """
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  defstruct(
    ring: nil,
    nodes: nil,
    virtual_nodes: 128  # Default Virutal Nodes of the server
  )

  @hash_range trunc(:math.pow(2, 32) - 1)

  def new() do
    %Ring{ring: :gb_trees.empty , nodes: MapSet.new()}
  end

  def create_ring_node(node) do
    Ring.RingNode.new(node)
  end

  @spec add_node(%Ring{}, %Ring.RingNode{}) :: %Ring{}
  defp add_node(ring, %Ring.RingNode{key: node_key} = node) do

    cond do
      MapSet.member?(ring.nodes, node_key) ->
        ring
      :else ->
        ring = %{ring | nodes: MapSet.put(ring.nodes, node_key)}
        Enum.reduce(1..ring.virtual_nodes, ring, fn i, %Ring{ring: r} = acc ->
          hash_key = :erlang.phash2({node_key, i}, @hash_range)
          try do
            %{acc | ring: :gb_trees.insert(hash_key, node_key, r)}
          catch
            :error, {:key_exists, _} ->
              acc
          end
        end)
    end
  end

  @spec add_node(%Ring{}, %Ring.RingNode{}) :: %Ring{}
  def add_nodes(ring, nodes) do
    ring = Enum.reduce(nodes, ring, fn node, acc -> add_node(acc, node) end)
    ring
  end

  def get_node_count(%Ring{nodes: nodes} = ring) do
    Enum.count(nodes)
  end

  def nodes_for_key(%Ring{ring: r} = ring, key, count) do
    hash = :erlang.phash2(key, @hash_range)
    count = min(get_node_count(ring), count)
    case :gb_trees.iterator_from(hash, r) do
      [{_key, node, _, _} | _] = iter ->
        get_nodes_from_iter(iter, count - 1, [node])
      _ ->
        {_key, node} = :gb_trees.smallest(r)
        [node]
    end
  end

  @spec remove_node(%Ring{}, any()) :: %Ring{}
  def remove_node(%Ring{nodes: nodes, ring: ring} = hash_ring, key) do

    if MapSet.member?(nodes, key) do
      tree2 = :gb_trees.to_list(ring)
      |> Enum.filter(fn {_key, ^key} -> false; _ -> true end)
      |> :gb_trees.from_orddict()
      %{hash_ring | nodes: MapSet.delete(nodes, key), ring: tree2}
    else
      hash_ring
    end
  end

  defp get_nodes_from_iter(iter, count, results) do
    if count == 0 do
      Enum.reverse(results)
    else
      case :gb_trees.next(iter) do
        {_key, node, iter} ->
          if node in results do
            get_nodes_from_iter(iter, count, results)
          else
            [node | results]
            get_nodes_from_iter(iter, count - 1, [node | results])
          end
        _ ->
          results
      end
    end
  end

  def hello() do
    :world
  end

end
