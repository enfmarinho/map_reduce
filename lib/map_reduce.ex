defmodule MapReduce do
  # Chamada para o caso geral (com lista)
  def main(list, map_func, reduce_func, acc) when is_list(list) do
    list
    |> split_lista()
    |> master(map_func, reduce_func, acc)
  # |> saida()
  end
  # Chamada recebendo o caminho de um arquivo (para o caso de fazer a contagem de palavras)
  def main(file_path \\ "test.txt", map_func, reduce_func, acc) do
    dividir_dataset(file_path)
    |> main(map_func, reduce_func, acc)
  end

  defp master(list, fun_map, fun_reduce,acc) do
    map_manager(list, fun_map)
    receber_msgs(length(list))
    |> concatena()

    list = shuffle_sort(list, :id)
    {first, second} = Enum.split(1, list)
    particoes = dividir_dataset(second, first, [], [first])

    # TODO terminar a parte do map
    reduce_manager(particoes, fun_reduce, acc)
    receber_msgs(length(list))
    # TODO retornar resultado
  end

  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end
 
  defp receber_msgs(msgs \\ [], num_msgs) when num_msgs > 0 do
    receive do
       msg -> receber_msgs([msg | msgs], num_msgs - 1)
    end
  end
  defp receber_msgs(msgs, num_msgs)  do
    msgs
  end

  defp concatena([], curr) do
     curr
  end 
  defp concatena([head | tail], curr \\ []) do
    concatena(tail, head ++ curr)
  end

  # Divide a lista de termos em uma lista de listas
  defp formar_listas(list, num) when num >= length(list) do
    [list]
  end
  defp formar_listas(list, num) do
    nova_list =
      list
      |> Enum.split(num)
    [nova_list |> elem(0)] ++ (nova_list |> elem(1) |> formar_listas(num))
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
    |> String.replace([",", ";", ".", ":", "(", ")"], "") # Tira pontuação
    |> String.split(" ")
  end

  # 'pid_list' é uma lista contendo os PIDs das threads utlizadas (será necesssario para receber as mensagens)
  defp map_manager([], _) do
    :ok
  end
  defp map_manager([h|t], fun_map) do
    # Adiciona o novo PID à lista  
    spawn(MapReduce, :concurrent_map, [h, fun_map, self])
    map_manager(t, fun_map)
  end

  defp concurrent_map(list ,fun_map, pid) do
    send pid, recebe_map(list, fun_map)
  end

  defp recebe_map([], fun) do
    []
  end
  defp recebe_map([h|t], fun) do
    [fun.(h)] ++ recebe_map(t, fun)
  end

  defp shuffle_sort(maps , keys) do
    maps 
    |> Enum.shuffle()
    |> Enum.sort_by(&Map.get(&1, keys))
  end

  defp dividir_dataset([], _, listaLista, lista), do: listaLista ++ lista
  defp dividir_dataset(data, anterior, listaLista, lista) do
    {first, second} = Enum.split(data, 1)
    if anterior == first do
      dividir_dataset(second, first, listaLista, lista ++ first)
    else
      dividir_dataset(second, first, listaLista ++ lista, [first])
    end
  end

  defp reduce_manager([], _, _) do
    :ok
  end
  defp reduce_manager([h|t], fun_red, acc) do
    spawn(MapReduce, :concurrent_reduce, [h, fun_red, acc, self])
    reduce_manager(t, fun_red, acc)
  end
  defp concurrent_reduce(list, fun_red, acc,pid) do
    send pid, recebe_reduce(list, fun_red, acc)
  end

  # 'acc' é o valor neutro da operacao (Deve ser alterado)
  defp recebe_reduce([], _) do
    # acc
  end
  defp recebe_reduce(list,fun, _) when length(list) == 1 do
    hd(list)
  end
  # Essa é chamada normal utilizada a partir da segunda iteração, em que será utilizaodo o valor "acc"
  defp recebe_reduce(list, fun) when length(list) >= 2 do
    # Separa os dois primeiros elementos da lista
    list1 = list |> Enum.split(2) |> elem(0)
    list2 = list |> Enum.split(2) |> elem(1)
    fun.(Enum.at(list, 0), Enum.at(list, 1)) |> fun.(recebe_reduce(list2, fun))
  end
  # Esaa é a chamada equivalnete a primeira chamada do reduce, em que teremos que calcular
  # o primeiro temro da lista com o 'acc'
  defp recebe_reduce([h|t], fun, acc) do
    # Separa os dois primeiros elementos da lista
    fun.(acc, h) |> fun.(recebe_reduce(t, fun))
  end
end
