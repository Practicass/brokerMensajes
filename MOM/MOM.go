package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Cola representa una cola de mensajes.
// Tiene un canal de mensajes (`mensajes`) y un mutex (`mux`) para sincronización.
type Cola struct {
	mensajes chan string
	durability bool
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
	Durability bool
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
	Ip string
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
		l.colas[args.Nombre] = Cola{make(chan string, 100), args.Durability}
		l.consumidores[args.Nombre] = []string{}
		fmt.Println("Cola declarada")
		l.mensajeRechazado <- "ok"

	}
	return nil
}

// mensajeCaducado es una función que maneja la expiración de un mensaje en una cola específica.
// Configura un temporizador de caducidad para los mensajes en la cola y elimina el mensaje
// si no se consume dentro del período especificado.
//
// Parámetros:
// - nombre: El nombre de la cola en la que se está manejando la caducidad de los mensajes.
//
// Comportamiento:
// - Configura un temporizador (`timer`) con una duración de 300 segundos (5 minutos).
// - Utiliza una instrucción `select` para esperar a que se produzca una de dos condiciones:
//     - El temporizador expira (`<-timer.C`), lo que indica que el mensaje ha caducado.
//     - Un mensaje ha sido consumido (`<-l.mensajeConsumido`), lo que indica que el mensaje ha sido procesado.
// - Si el temporizador expira primero, imprime un mensaje indicando que el mensaje ha caducado y elimina el mensaje de la cola.
// - Si se consume el mensaje antes de que el temporizador expire, imprime un mensaje indicando que el mensaje ha sido consumido.
func (l *Broker) mensajeCaducado( nombre string){
	timer := time.NewTimer(300 * time.Second)
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
		if(l.colas[args.Nombre].durability){
            // Abre el archivo con el nombre args.Nombre.txtx en modo append.
            file, err := os.OpenFile(args.Nombre+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                fmt.Println("Error al abrir el archivo:", err)
                return err
            }
            defer file.Close()
            // Escribe args.Mensaje en el archivo.
            if _, err := file.WriteString(args.Mensaje); err != nil {
                fmt.Println("Error al escribir en el archivo:", err)
                return err
            }
			
		}
		go l.mensajeCaducado(args.Nombre)
	}
	return nil
}

// eliminarPrimeraLinea elimina la primera línea de un archivo especificado por `nombreArchivo`.
//
// Parámetros:
// - nombreArchivo: El nombre del archivo del que se desea eliminar la primera línea.
//
// Retorna:
// - Un valor de tipo `error` que es `nil` si la operación es exitosa, o un error si ocurre un problema al leer o escribir en el archivo.
//
// Comportamiento:
// - Lee todas las líneas del archivo especificado por `nombreArchivo` utilizando `os.ReadFile`.
// - Convierte el contenido leído en una slice de strings utilizando `strings.Split`.
// - Elimina la primera línea de la slice de strings.
// - Si solo queda una línea después de eliminar la primera línea, elimina el archivo completo utilizando `os.Remove`.
// - Si quedan más líneas, las convierte de nuevo en una cadena utilizando `strings.Join`.
// - Escribe las líneas restantes de nuevo en el archivo utilizando `os.WriteFile` con permisos de escritura (0644).
// - Retorna un error si ocurre algún problema al leer, escribir o eliminar el archivo.
func eliminarPrimeraLinea(nombreArchivo string) error {
    // Lee todas las líneas del archivo.
    lineas, err := os.ReadFile(nombreArchivo)
    if err != nil {
        return err
    }
    // Convierte las líneas a una slice de strings.
    todasLasLineas := strings.Split(string(lineas), "\n")
    // Elimina la primera línea.
    lineasRestantes := todasLasLineas[1:]
	if(len(lineasRestantes) == 1 ){
		fmt.Println("Borrando archivo")
		err = os.Remove(nombreArchivo)
		if err != nil {
			fmt.Println("Borrando archivo"+err.Error())
			return err
		}
		return nil
	}else{
		// Convierte las líneas restantes a una cadena.
		nuevoContenido := strings.Join(lineasRestantes, "\n")

		// Escribe las líneas restantes de nuevo en el archivo.
		err = os.WriteFile(nombreArchivo, []byte(nuevoContenido), 0644)
		if err != nil {
			return err
		}
		return nil
	}
}


// leerArchivo es un método del tipo `Broker` que lee todas las líneas de un archivo con el nombre especificado,
// declara una cola con el nombre del archivo, y publica cada línea no vacía como un mensaje en esa cola.
//
// Parámetros:
// - nombreArchivo: El nombre del archivo del que se van a leer las líneas (sin la extensión .txt).
//
// Retorna:
// - Un valor de tipo `error` que es `nil` si la operación es exitosa, o un error si ocurre un problema al leer el archivo o al publicar mensajes.
//
// Comportamiento:
// - Lee todas las líneas del archivo con el nombre especificado y extensión `.txt` utilizando `os.ReadFile`.
// - Convierte el contenido leído en una slice de strings utilizando `strings.Split`.
// - Declara una cola con el nombre del archivo, especificando que la cola es duradera (opcional).
// - Itera sobre las líneas de la slice de strings y publica cada línea no vacía como un mensaje en la cola declarada.
// - Retorna un error si ocurre algún problema al leer el archivo o al publicar mensajes.
func (l *Broker) leerArchivo(nombreArchivo string) error{
	// Lee todas las líneas del archivo.
    lineas, err := os.ReadFile(nombreArchivo+".txt")
    if err != nil {
        return err
    }
    // Convierte las líneas a una slice de strings.
    todasLasLineas := strings.Split(string(lineas), "\n")
	l.Declarar_cola(&ArgsDeclararCola{Nombre: nombreArchivo, Durability: true}, &Reply{Mensaje: ""})
	for i := 0; i < len(todasLasLineas); i++{
		if(todasLasLineas[i] != ""){
			l.Publicar(&ArgsPublicar{Nombre: nombreArchivo, Mensaje: todasLasLineas[i]}, &Reply{Mensaje: ""})
		}
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
		var mensaje string
		mensaje = <- l.mensajeRechazado
		if(mensaje == "ok"){
			mensaje = <- l.colas[nombre].mensajes
		}
		args := &ArgsCallback{Mensaje: mensaje}
		var reply Reply
		err := client.Call("Consumidor.Callback", args, &reply)
		fmt.Println("Mensaje error:", err)
		if err != nil {
			fmt.Println("Error al llamar a la función callback:", err)
			// Decide qué hacer en caso de error.
			l.mensajeRechazado <- mensaje
			client.Close()
			break;
		}else{
			fmt.Println("Else ",l.colas[nombre].durability)
			if(l.colas[nombre].durability){
				fmt.Println("Eliminando mensaje")
				eliminarPrimeraLinea(nombre+".txt")
			}
			l.mensajeRechazado <- "ok"
			l.mensajeConsumido <- true
		}
		time.Sleep(300*time.Millisecond)
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
		client, err := rpc.Dial("tcp", args.Ip)
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

// EjecutarBroker inicia el servidor RPC del broker y escucha conexiones entrantes en la dirección IP especificada.
//
// Parámetros:
// - ip: La dirección IP en la que el servidor debe escuchar las conexiones entrantes.
//
// Comportamiento:
// - Registra el broker (instancia de `Broker`) como un servicio RPC utilizando `rpc.Register`.
// - Inicia un listener TCP en la dirección IP especificada utilizando `net.Listen`.
// - Verifica si hay un error al iniciar el servidor y, de ser así, imprime el error y retorna.
// - Usa `defer` para asegurarse de cerrar el listener cuando la función termine.
// - Imprime un mensaje indicando que el servidor está escuchando en la dirección IP especificada.
// - En un bucle infinito, acepta conexiones entrantes y maneja los clientes conectados utilizando `rpc.Accept`.
func (l * Broker) EjecutarBroker( ip string){
	rpc.Register(l)
	ln, err := net.Listen("tcp", ip)
	if err != nil {
		fmt.Println("Error al iniciar el servidor:", err)
		return
	}
	defer ln.Close()
	fmt.Println("Servidor escuchando en ", ip)
	for{
		// Aceptar conexiones entrantes
		rpc.Accept(ln)
		fmt.Println("Cliente conectado")
	}
}

// ListarColas muestra en la consola una lista de todas las colas disponibles.
//
// Comportamiento:
// - Imprime un encabezado ("Colas:").
// - Verifica si no hay colas disponibles y, de ser así, imprime un mensaje indicando que no hay colas.
// - Si hay colas disponibles, itera sobre las claves (nombres) de las colas y las imprime en la consola.
func (l *Broker) ListarColas(){
	fmt.Println("Colas:")
	if len(l.colas) == 0 {
		fmt.Println("No hay colas disponibles")
	} else {
		for key := range l.colas {
			fmt.Println(key)
		}
	}	
}


// BorrarCola elimina la cola con el nombre especificado del broker.
//
// Parámetros:
// - nombre: El nombre de la cola que se desea eliminar.
//
// Comportamiento:
// - Verifica si la cola con el nombre especificado existe en el broker.
// - Si la cola existe, imprime un mensaje indicando que se va a eliminar la cola y la elimina utilizando `delete`.
func (l *Broker) BorrarCola(nombre string){
	if _, ok := l.colas[nombre]; ok {
		fmt.Println("Borrando cola", nombre)
		delete(l.colas, nombre)
	}
}

// RescatarColasAnteriores lee los archivos en el directorio actual y carga colas a partir de ellos.
//
// Comportamiento:
// - Lee los archivos en el directorio actual utilizando `os.ReadDir`.
// - Verifica si hay un error al leer los archivos y, de ser así, imprime el error y retorna.
// - Itera sobre los archivos en el directorio, omitiendo el primer archivo (índice 0).
// - Por cada archivo, imprime su nombre, extrae el nombre de la cola (sin la extensión) y llama a `leerArchivo` para cargar las colas.
func (l *Broker) RescatarColasAnteriores(){
    archivos, err := os.ReadDir(".")
    if err != nil {
		fmt.Println("Error al rescatar colas antiguas:", err)
        return
    }
    for index, archivo := range archivos {
		if(index != 0){
			fmt.Println(archivo.Name()+";")
			name := strings.Split(archivo.Name(), ".")[0]			 
			l.leerArchivo(name)
		}
    }
}


// main es la función principal que inicia el servidor RPC y espera conexiones.
// Crea una instancia de `Broker`, la registra en RPC y comienza a escuchar en el puerto 8080.
func main(){
	args := os.Args
	//Verifica número correcto de argumentos
	if len(args) < 2 {
        fmt.Println("No se ha proporcionado ningún argumento. Ejemplo de uso:")
        fmt.Println("  go run MOM direccionIP:puerto")
        return
    }
	l := NuevoBroker()
	go l.EjecutarBroker(args[1])
	l.RescatarColasAnteriores()
	reader := bufio.NewReader(os.Stdin)
	for {
        fmt.Println("Ingresa una de las operacions ( listar colas / borrar cola): ")
        // Leer una línea de entrada
        input, err := reader.ReadString('\n')
        if err != nil {
            fmt.Println("Error al leer la entrada:", err)
			continue
        }
		if(strings.Contains(input, "listar colas")){
			l.ListarColas()
		}else if(strings.Contains(input, "borrar cola")){
			fmt.Println("Ingresa el nombre de la cola a borrar: ")
			input, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error al leer la entrada:", err)
				continue
			}
			l.BorrarCola(input)
		}else{
			fmt.Println("Operación no válida")
		}
	}
}




