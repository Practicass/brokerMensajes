package main

import (
	"sync"

)


type Cola struct {
	mensajes chan string
	mux sync.Mutex
}

//Estructura que representa el broker.
type Broker struct {
    // colas es un mapa que asocia nombres de cola con canales de tipo string.
    // Cada canal representa una cola donde se pueden enviar y recibir mensajes de tipo string.
    colas map[string]Cola

    // consumidores es un mapa que asocia nombres de cola con listas de consumidores.
    // Cada consumidor está representado por una cadena (string).
    consumidores map[string][]string
}

func (l *Broker) declarar_cola(nombre string ){
	if(l.colas == nil){
		l.colas = make(map[string]Cola)

	}
	
	if _, ok := l.colas[nombre]; !ok {
		l.colas[nombre] = Cola{make(chan string),sync.Mutex{}}
		l.consumidores[nombre] = []string{}
	}
	
}

func (l *Broker) publicar(nombre string, mensaje string){
	if _, ok := l.colas[nombre]; ok {
		l.colas[nombre].mensajes <- mensaje
	}
}

func (l *Broker) consumir(nombre string, respuesta []string){

	if _, ok := l.colas[nombre]; ok {
		// i = 0
		for true {
			mensaje := <- l.colas[nombre]
			respuesta.append(mensaje)

		}
	}
}




func main() {

	// miListaColas := ListaColas{
	// 	colas: list.New(),
	// }

}
