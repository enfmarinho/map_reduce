defmodule MapReduce do
  # Chamada para o caso geral (com lista)
  def main(list, map_func, reduce_func, acc) when is_list(list) do
    list
    |> split_lista()
    |> master(map_func, reduce_func, acc)
  end
  # Chamada recebendo o caminho de um arquivo (para o caso de fazer a contagem de palavras)
  def main(file_path \\ "test.txt", map_func \\ &map_words/1, reduce_func \\ &reduce_words/2, acc \\ %{}) do
    dividir_dataset(file_path)
    |> main(map_func, reduce_func, acc)
  end

  defp master(list, fun_map, fun_reduce,acc) do
    map_manager(list, fun_map)
    list = receber_msgs(length(list))
    |> concatena()
    list = shuffle_sort(list, :id)
    particoes = particionar_shuffle(list, [])
    reduce_manager(particoes, fun_reduce, acc)
    receber_msgs(length(particoes))
    |> shuffle_sort(:count)
  end

  defp number_of_native_threads(), do: System.schedulers()

  defp receber_msgs(msgs \\ [], num_msgs) when num_msgs > 0 do
    receive do
       msg -> receber_msgs([msg | msgs], num_msgs - 1)
    end
  end
  defp receber_msgs(msgs, num_msgs), do: msgs

  defp concatena([], curr), do: curr
  defp concatena([head | tail], curr \\ []), do: concatena(tail, head ++ curr)

  defp formar_listas(list, num) do
    nova_list =
      list
      |> Enum.split(num)
      if nova_list |> elem(1) |> length >= num do
        [nova_list |> elem(0)] ++ (nova_list |> elem(1) |> formar_listas(num))
      else 
        [List.flatten(nova_list |> elem(0), nova_list |> elem(1))]
      end
  end

  defp split_lista(list) do
    multi = IO.gets("Digite o multiplicador: ")
     |> String.replace("\n", "")   # Remove o '\n'
     |> String.to_integer
    num = number_of_native_threads() * multi
    formar_listas(list, num)
  end



  # Essa funcao é usada para o caso de contagem de palavras
  defp dividir_dataset(file_path)  do
    file_path
    |> File.read()
    |> elem(1)
    |> String.replace([",", ";", ".", ":", "(", ")", "\n"], "") # Tira pontuação
    |> String.split(" ")
  end
  # 'pid_list' é uma lista contendo os PIDs das threads utlizadas (será necesssario para receber as mensagens)
  defp map_manager([], _) do
    :ok
  end
  defp map_manager([h|t], fun_map) do
    # Adiciona o novo PID à lista  
    spawn(MapReduce, :concurrent_map, [h, fun_map, self()])
    map_manager(t, fun_map)
  end
  def concurrent_map(list ,fun_map, pid) do
    send pid, recebe_map(list, fun_map)
  end

  defp recebe_map([], _) do
    []
  end
  defp recebe_map([h|t], fun) do
    [fun.(h)] ++ recebe_map(t, fun)
  end


  defp reduce_manager([], _, _) do
    :ok
  end
  defp reduce_manager([h|t], fun_red, acc) do
    spawn(MapReduce, :concurrent_reduce, [h, fun_red, acc, self()])
    reduce_manager(t, fun_red, acc)
  end

  def concurrent_reduce(list, fun_red, acc,pid) do
    send pid, Enum.reduce(list,acc,fun_red)
  end

  defp shuffle_sort(maps , keys) do
    maps |> Enum.sort_by(&Map.get(&1, keys))
  end

  defp particionar_shuffle([], listaLista), do: listaLista
  defp particionar_shuffle(dataset, listaLista) do
    {current, rest} = Enum.split_while(dataset, fn x -> x[:id] == hd(dataset)[:id] end)
    particionar_shuffle(rest, listaLista ++ [current])
    # current
  end

  defp map_words(word) do
    %{
       :id => word,
       :count => 1
     }
  end

  defp reduce_words(word1, word2) when word1 == %{} do
    word2
  end
  defp reduce_words(word1, word2) when word2 == %{} do
    word1
  end
  defp reduce_words(word1, word2) do
    # Já vamos considerar que as duas palavras tem mesmo 'id' (já foi feito o shuffle)
    %{
      :id => word1[:id],
      :count => word1[:count] + word2[:count]
     }
  end
end
