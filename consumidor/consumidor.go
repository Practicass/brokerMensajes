package main

import (
	"bufio"
	"fmt"
	"os"
	"net"
	"net/rpc"
)

type Consumidor struct{
	nombre string
	broker *rpc.Client
}

// type consumidor interface{
// 	Consumir(nombre string, callback func(string))
// }

func NuevoConsumidor(nombre string, broker *rpc.Client) *Consumidor{
	// fmt.Println("Creando ", nombre)

	return &Consumidor{
		nombre: nombre,
		broker: broker,
	}

}

type ArgsCallback struct{
	Mensaje string
}


func (c *Consumidor) Callback(args *ArgsCallback, reply *Reply) error{
	fmt.Println("Consumidor " + c.nombre + " " + args.Mensaje)
	fmt.Println("Ingresa el nombre de la cola: ")
	return nil
}

// func (c *Consumidor) CrearCola(nombreCola string){
// 	c.broker.Declarar_cola(nombreCola)
// }

type ArgsDeclararCola struct{
	Nombre string
}
type ArgsConsumir struct{
	Nombre string
	Callback func(*ArgsCallback, *Reply) error
	Ip string
}
type Reply struct{
	Mensaje string
}

func (c *Consumidor) Leer (nombreCola string, ip string) {
	// fmt.Println("Consumidor " + c.nombre + " leyendo")
	// c.broker.Declarar_cola(nombreCola)
	// c.broker.Consumir(nombreCola, c.Callback)
	var reply Reply
	args := &ArgsDeclararCola{Nombre: nombreCola}
    err := c.broker.Call("Broker.Declarar_cola", args, &reply)
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



func main(){

	args := os.Args


	if len(args) < 4 {
        fmt.Println("No se ha proporcionado ningún argumento. Ejemplo de uso:")
        fmt.Println("  go run productor nombreConsumidor direccionIPBroker:puerto direccionIP:puerto")
        return
    }
	broker, err := rpc.Dial("tcp", args[2])
    if err != nil {
        fmt.Println("Error al conectar al servidor:", err)
    }
    defer broker.Close()
	
	consumidor1 := NuevoConsumidor(args[1], broker)

	rpc.Register(consumidor1)


	listener, err := net.Listen("tcp", args[3])
	if err != nil {
		fmt.Println("ListenTCP error:", err)
	}
	go func() {
		rpc.Accept(listener)
	} ()


	reader := bufio.NewReader(os.Stdin)

	var input string;
	for {
        fmt.Println("Ingresa el nombre de la cola: ")
        // Leer una línea de entrada
        input, err = reader.ReadString('\n')
        if err != nil {
            fmt.Println("Error al leer la entrada:", err)
			continue
        }
		consumidor1.Leer(input, args[3])

	}
	

}