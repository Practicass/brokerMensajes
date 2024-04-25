package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Cola representa una cola de mensajes.
// Tiene un canal de mensajes (`mensajes`) y un mutex (`mux`) para sincronización.
type Cola struct {
	mensajes chan string
	mux *sync.Mutex
}

//Estructura que representa el broker.
type Broker struct {
    // colas es un mapa que asocia nombres de cola con canales de tipo string.
    // Cada canal representa una cola donde se pueden enviar y recibir mensajes de tipo string.
    colas map[string]Cola

    // consumidores es un mapa que asocia nombres de cola con listas de consumidores.
    // Cada consumidor está representado por una cadena (string).
    consumidores map[string][]string

	mensajeConsumido chan bool

	mensajeRechazado chan string

}


// ArgsDeclararCola representa los argumentos para declarar una nueva cola.
// Contiene el nombre de la cola que se va a declarar.
type ArgsDeclararCola struct{
	Nombre string
}

// ArgsPublicar representa los argumentos para publicar un mensaje en una cola.
// Contiene el nombre de la cola y el mensaje que se va a publicar.
type ArgsPublicar struct{
	Nombre string
	Mensaje string
}

// ArgsConsumir representa los argumentos para consumir mensajes de una cola.
// Contiene el nombre de la cola y una función de callback que se llamará para cada mensaje consumido.
type ArgsConsumir struct{
	Nombre string
	Callback func(*ArgsCallback, *Reply) error
}

// Reply representa la respuesta de una llamada RPC.
// Contiene un mensaje.
type Reply struct{
	Mensaje string
}

type ArgsCallback struct{
	Mensaje string
}


// NuevoBroker crea y devuelve una nueva instancia de `Broker`.
// Inicializa los mapas `colas` y `consumidores` vacíos.
//
// Retorna:
// - Un puntero a una nueva instancia de `Broker`.
func NuevoBroker() *Broker {
	// fmt.Println("Broker")
	return &Broker{
		colas : make(map[string]Cola),
		consumidores : make(map[string][]string),
		mensajeConsumido: make(chan bool),
		mensajeRechazado: make(chan string, 1),
	}
	
}


// Declarar_cola es un método RPC que declara una nueva cola si no existe.
// Toma un argumento `ArgsDeclararCola` que contiene el nombre de la cola a declarar y una respuesta `Reply`.
//
// Parámetros:
// - args: Un puntero a una estructura `ArgsDeclararCola` que contiene el nombre de la cola.
// - reply: Un puntero a una estructura `Reply` que puede contener la respuesta del servidor RPC.
//
// Retorna:
// - Un valor de tipo `error` que es `nil` si la operación es exitosa, o un error si ocurre un problema.
func (l *Broker) Declarar_cola(args *ArgsDeclararCola, reply *Reply) error{
	if(l.colas == nil){
		l.colas = make(map[string]Cola)
	}
	if _, ok := l.colas[args.Nombre]; !ok {
		l.colas[args.Nombre] = Cola{make(chan string, 10), &sync.Mutex{}}
		l.consumidores[args.Nombre] = []string{}
		fmt.Println("Cola declarada")
		l.mensajeRechazado <- "ok"

	}
	return nil
	
}

func (l *Broker) mensajeCaducado( nombre string){
	timer := time.NewTimer(100 * time.Second)
    
	select {	
	case <-timer.C:
		fmt.Println("Mensaje caducado")
		<- l.colas[nombre].mensajes
	case <-l.mensajeConsumido:
		fmt.Println("Mensaje consumido")
	}


}

// Publicar es un método RPC que publica un mensaje en una cola específica.
// Toma argumentos `ArgsPublicar` que contienen el nombre de la cola y el mensaje a publicar, y una respuesta `Reply`.
//
// Parámetros:
// - args: Un puntero a una estructura `ArgsPublicar` que contiene el nombre de la cola y el mensaje a publicar.
// - reply: Un puntero a una estructura `Reply` que puede contener la respuesta del servidor RPC.
//
// Retorna:
// - Un valor de tipo `error` que es `nil` si la operación es exitosa, o un error si ocurre un problema.
func (l *Broker) Publicar(args *ArgsPublicar, reply *Reply) error{
	if _, ok := l.colas[args.Nombre]; ok {
		fmt.Println("Publicando", args.Nombre," ", args.Mensaje)
		l.colas[args.Nombre].mensajes <- args.Mensaje

		go l.mensajeCaducado(args.Nombre)
	}
	return nil
}


// Leer es una función que se ejecuta como una goroutine para leer mensajes de una cola.
// Consume mensajes de la cola con el nombre especificado y llama al callback para cada mensaje leído.
//
// Parámetros:
// - nombre: El nombre de la cola de la que se desean consumir mensajes.
// - callback: Una función de callback que se ejecutará con cada mensaje consumido.
func (l *Broker) Leer(nombre string, client *rpc.Client){
	for {
		
		//tener un seguna canal / cola / vector dinamico  y antes de leer de el canal l.colas[nombre].mensajes comporbar si esta vacia el canal / cola / vector dinamico prioritario
		// hacer que sea atómico entre las go rutinas
		var mensaje string
		mensaje = <- l.mensajeRechazado
		if(mensaje == "ok"){
			mensaje = <- l.colas[nombre].mensajes
		}
		
		// fmt.Println("Consumiendo")
		// (callback)(mensaje)
		args := &ArgsCallback{Mensaje: mensaje}
		var reply Reply
		err := client.Call("Consumidor.Callback", args, &reply)
		if err != nil {
			fmt.Println("Error al llamar a la función callback:", err)
			// Decide qué hacer en caso de error.
			l.mensajeRechazado <- mensaje
			client.Close()
			break;
		}else{
			l.mensajeRechazado <- "ok"
			l.mensajeConsumido <- true
		}
		time.Sleep(1000*time.Millisecond)
	}
}


// Consumir es un método RPC que inicia una goroutine para consumir mensajes de una cola específica.
// Toma argumentos `ArgsConsumir` que contienen el nombre de la cola y la función de callback, y una respuesta `Reply`.
//
// Parámetros:
// - args: Un puntero a una estructura `ArgsConsumir` que contiene el nombre de la cola y la función de callback para consumir mensajes.
// - reply: Un puntero a una estructura `Reply` que puede contener la respuesta del servidor RPC.
//
// Retorna:
// - Un valor de tipo `error` que es `nil` si la operación es exitosa, o un error si ocurre un problema.
func (l *Broker) Consumir(args *ArgsConsumir, reply *Reply) error{
	if _, ok := l.colas[args.Nombre]; ok {
		client, err := rpc.Dial("tcp", "127.0.0.1:8081")
		if err != nil {
			fmt.Println("Dialing:", err)
		}
		go l.Leer(args.Nombre, client)
		return nil

	}else{
		//poner error
		return nil
	}
}


// main es la función principal que inicia el servidor RPC y espera conexiones.
// Crea una instancia de `Broker`, la registra en RPC y comienza a escuchar en el puerto 8080.
func main(){

	l := NuevoBroker()
	rpc.Register(l)

	ln, err := net.Listen("tcp", "127.0.0.1:8080")
    if err != nil {
        fmt.Println("Error al iniciar el servidor:", err)
        return
    }
    defer ln.Close()

    fmt.Println("Servidor escuchando en el puerto 8080")

	for{
		// Aceptar conexiones entrantes
		rpc.Accept(ln)
		if err != nil {
			fmt.Println("Error al aceptar la conexión:", err)
			return
		}
		fmt.Println("Cliente conectado")
	}

}




