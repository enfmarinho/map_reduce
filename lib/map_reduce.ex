defmodule MapReduce do
  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end

  #DIVIDIR DATASET

  #todo
  # defp formar_listas(list, f, num) do


  # end
  # defp formar_listas(list, f, num) when f + num >   do

  # end

  defp split_lista(list) do
    # criterio de parada decidido pelo cliente
    # a cada "num" elementos da lista, manda os elementos para a proxima lista
    multi = IO.gets("Digite o multiplicador: ")
    num = number_of_native_threads * multi


    #Enum.map(Enum.split(list , num) , list) #dividir em



    #TODO dividir a lista grande em sub listas com o tamanho num e salvar em outra lista

  end
  def dividir_dataset(file_path) do
    list = file_path
    |> File.read
    |> elem(1)
    |> String.split(" ")

    #dividir com base em um múltiplo das cabeças por 6

    num = number_of_native_threads * 6


  end

  defp master() do
    #p1 formar a lista (done)
    #p2 dividir em um multiplo com base nas cabeças threads
    url_caminho = IO.gets("Digite a url....: ")
    dividir_dataset(url_caminho)


    #JOGAR NOS MAPS

    # ENviar para os escravos

    #TERMINOU?

    # Receber da fila de mensagens dos escravos

    #SHUFFLE
    # Ordenar pela chaves

    #TERMINOU O SHUFFLE?
    #REDUCE
    # Manda os escravos realizarem os reduces
  end

  defp recebe_map() do

    #Map here
    # EX: doubled_list = Enum.map(list, fn x -> x * 2 end)

    end

  defp recebe_reduce() do
    #Reduce here
    #EX: sum = Enum.reduce(list, fn x, acc -> x + acc end)
    end

  defp recebe_threads_map() do

    #recebe threads da map
    end

  defp shuffle(list) when is_list(list) do


    end



end
