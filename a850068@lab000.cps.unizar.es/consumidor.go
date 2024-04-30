package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type Consumidor struct {
	nombre string
	broker *rpc.Client
}

// type consumidor interface{
// 	Consumir(nombre string, callback func(string))
// }

func NuevoConsumidor(nombre string, broker *rpc.Client) *Consumidor {
	// fmt.Println("Creando ", nombre)

	return &Consumidor{
		nombre: nombre,
		broker: broker,
	}

}

type ArgsCallback struct {
	Mensaje string
}

func (c *Consumidor) Callback(args *ArgsCallback, reply *Reply) error {
	fmt.Println("Consumidor " + c.nombre + " " + args.Mensaje)
	fmt.Println("Ingresa el nombre de la cola: ")
	return nil
}

// ArgsDeclararCola representa los argumentos para declarar una nueva cola.
// Contiene el nombre de la cola que se va a declarar.
type ArgsDeclararCola struct {
	Nombre     string
	Durability bool
}

// ArgsConsumir representa los argumentos para consumir mensajes de una cola.
// Contiene el nombre de la cola y una función de callback que se llamará para cada mensaje consumido.
type ArgsConsumir struct {
	Nombre   string
	Callback func(*ArgsCallback, *Reply) error
	Ip       string
}
type Reply struct {
	Mensaje string
}

// Método Leer inicia el proceso de consumo de mensajes de una cola.
// Declara la cola especificada, luego se suscribe para consumir mensajes de esa cola.

func (c *Consumidor) Leer(nombreCola string, durability string, ip string) {
	var reply Reply

	durabilityBool, err := strconv.ParseBool(durability)
	if err != nil {
		fmt.Println("Error al convertir la durabilidad:", err)
		return
	}

	args := &ArgsDeclararCola{Nombre: nombreCola, Durability: durabilityBool}
	err = c.broker.Call("Broker.Declarar_cola", args, &reply)
	if err != nil {
		fmt.Println("Error al llamar al método Multiply:", err)
		return
	}

	args2 := &ArgsConsumir{Nombre: nombreCola, Callback: c.Callback, Ip: ip}
	err = c.broker.Call("Broker.Consumir", args2, &reply)
	if err != nil {
		fmt.Println("Error al llamar al método Multiply:", err)
		return
	}
}

func main() {
	// Verificar si se proporcionan los argumentos necesarios
	args := os.Args

	if len(args) < 4 {
		fmt.Println("No se ha proporcionado ningún argumento. Ejemplo de uso:")
		fmt.Println("  go run productor nombreConsumidor direccionIPBroker:puerto direccionIP:puerto")
		return
	}
	// Conectar al servidor Broker RPC
	broker, err := rpc.Dial("tcp", args[2])
	if err != nil {
		fmt.Println("Error al conectar al servidor:", err)
	}
	defer broker.Close()

	consumidor1 := NuevoConsumidor(args[1], broker)

	rpc.Register(consumidor1)

	// Iniciar el servidor RPC del consumidor
	listener, err := net.Listen("tcp", args[3])
	if err != nil {
		fmt.Println("ListenTCP error:", err)
	}
	go func() {
		rpc.Accept(listener)
	}()

	// Leer el nombre de la cola desde la entrada estándar y comenzar a consumir mensajes
	reader := bufio.NewReader(os.Stdin)

	var input string
	for {
		fmt.Println("Ingresa el nombre de la cola: ")
		// Leer una línea de entrada
		input, err = reader.ReadString('\n')
		fmt.Print("Si es el primer mensaje de la cola, ¿desea que la cola sea durable? (true/false):")
		// Leer una línea de entrada
		input2, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error al leer la entrada:", err)
			continue
		}
		consumidor1.Leer(input, input2, args[3])

	}

}
