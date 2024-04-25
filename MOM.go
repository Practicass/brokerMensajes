package main

import (
	"sync"
	"fmt"

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

func  NuevoBroker() *Broker {
	// fmt.Println("Broker")
	return &Broker{
		colas : make(map[string]Cola),
		consumidores : make(map[string][]string),

	}
	
}

func (l *Broker) Declarar_cola(nombre string ){
	if(l.colas == nil){
		l.colas = make(map[string]Cola)

	}
	if _, ok := l.colas[nombre]; !ok {
		l.colas[nombre] = Cola{make(chan string),sync.Mutex{}}
		l.consumidores[nombre] = []string{}
	}
	
}

func (l *Broker) Publicar(nombre string, mensaje string){
	if _, ok := l.colas[nombre]; ok {
		fmt.Println("Publicando", nombre," ", mensaje)
		l.colas[nombre].mensajes <- mensaje
	}
}

func (l *Broker) Consumir(nombre string, callback func(string)){
	if _, ok := l.colas[nombre]; ok {
		// for true {
			mensaje := <- l.colas[nombre].mensajes
			// fmt.Println("Consumiendo")
			callback(mensaje)
			
		// }
	}
}




