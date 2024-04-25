// main.go
// Este es un programa en Go que define una estructura Productor que interactúa con un Broker de mensajes mediante RPC
// para publicar mensajes en una cola de mensajes.
package main

import (
	"bufio"
	"fmt"
	"os"
	"net/rpc"
)

// Productor representa a un productor de mensajes que interactúa con un Broker de mensajes.
type Productor struct{
	nombre string
	broker *rpc.Client
}



// func (p *Productor) Productor(nombre string, broker *rpc.Client){
// 	p.nombre = nombre
// 	p.broker = broker
// }

// NuevoProductor crea y devuelve una nueva instancia de Productor con el nombre y broker especificados.
//
// Parámetros:
// - nombre: El nombre del productor.
// - broker: Un puntero al cliente RPC conectado al broker.
//
// Retorna:
// - Un puntero a una nueva instancia de Productor.
func NuevoProductor(nombre string, broker *rpc.Client) *Productor{

	return &Productor{
		nombre: nombre,
		broker: broker,
	}

}

// ArgsDeclararCola representa los argumentos necesarios para declarar una nueva cola en el Broker de mensajes.
type ArgsDeclararCola struct{
	Nombre string
}

// ArgsPublicar representa los argumentos necesarios para publicar un mensaje en una cola del Broker de mensajes.
type ArgsPublicar struct{
	Nombre string
	Mensaje string
}


// Reply representa la respuesta recibida del Broker de mensajes.
type Reply struct{
	Mensaje string
}


// Publicar publica un mensaje en la cola especificada en el Broker mediante RPC.
//
// Parámetros:
// - nombreCola: El nombre de la cola en la que se desea publicar el mensaje.
// - mensaje: El mensaje que se desea publicar en la cola.
func (p *Productor) Publicar(nombreCola string, mensaje string){
	// fmt.Println("Escribiendo")
	// p.broker.Declarar_cola(nombreCola)
	// p.broker.Publicar(nombreCola, mensaje)
    var reply Reply
	args := &ArgsDeclararCola{Nombre: nombreCola}
    err := p.broker.Call("Broker.Declarar_cola", args, &reply)
	if err != nil {
        fmt.Println("Error al llamar al método Multiply:", err)
        return
    }
	args2 := &ArgsPublicar{Nombre: nombreCola, Mensaje: mensaje}
    err = p.broker.Call("Broker.Publicar", args2, &reply)
	fmt.Println("terminar")
	if err != nil {
        fmt.Println("Error al llamar al método Multiply:", err)
        return
    }
}




// main es la función principal del programa.
//
// Esta función se encarga de leer los argumentos de la línea de comandos para obtener el nombre del productor.
// Luego, establece una conexión con el Broker de mensajes y entra en un bucle donde solicita al usuario que ingrese
// el nombre de la cola y el mensaje que desea publicar en ella. Finalmente, llama al método Publicar del productor
// para publicar el mensaje en la cola especificada.
func main(){

	// Obtener los argumentos de la línea de comandos
	args := os.Args

	//Verifica número correcto de argumentos
	if len(args) < 2 {
        fmt.Println("No se ha proporcionado ningún argumento. Ejemplo de uso:")
        fmt.Println("  go run productor nombreProductor")
        return
    }

	//Realizar conexión
	broker, err := rpc.Dial("tcp", "127.0.0.1:8080")
    if err != nil {
        fmt.Println("Error al conectar al servidor:", err)
    }
    defer broker.Close()

	
	reader := bufio.NewReader(os.Stdin)

	productor := NuevoProductor(args[1], broker)

	//Leer de entrada estandar
	for {
        fmt.Print("Ingresa el nombre de la cola: ")
        // Leer una línea de entrada
        input1, err := reader.ReadString('\n')
        if err != nil {
            fmt.Println("Error al leer la entrada:", err)
            continue
        }
		fmt.Print("Ingresa el mensaje: ")
        // Leer una línea de entrada
        input2, err := reader.ReadString('\n')
        if err != nil {
            fmt.Println("Error al leer la entrada:", err)
            continue
        }
		

		go productor.Publicar(input1,input2)
	}
}