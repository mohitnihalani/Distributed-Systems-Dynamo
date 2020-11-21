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
    %Ring{ring: :gb_trees.empty , nodes: %{}}
  end

  defp add_node(ring, node) do

    cond do
      Map.has_key?(ring.nodes, node) ->
        ring
      :else ->
        ring_node = Ring.RingNode.new(node)
        ring = %{ring | nodes: Map.put_new(ring.nodes, node, ring_node)}
        Enum.reduce(1..ring.virtual_nodes, ring, fn i, %Ring{ring: r} = acc ->

          hash_key = :erlang.phash2({node, i}, @hash_range)
          try do
            %{acc | ring: :gb_trees.insert(hash_key , node, r)}
          catch
            :error, {:key_exists, _} ->
              acc
          end
        end)
    end
  end

  @spec add_node(%Ring{}, any()) :: %Ring{}
  def add_nodes(ring, nodes) do
    ring = Enum.reduce(nodes, ring, fn node, acc -> add_node(acc, node) end)
    ring
  end

  def get_node_count(%Ring{nodes: nodes} = ring) do
    Enum.count(nodes)
  end

  def hello() do
    :world
  end

end
