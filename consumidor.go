package main

import "fmt"

type Consumidor struct{
	nombre string
	broker *Broker
}

// type consumidor interface{
// 	Consumir(nombre string, callback func(string))
// }

func NuevoConsumidor(nombre string, broker *Broker) *Consumidor{
	// fmt.Println("Creando ", nombre)

	return &Consumidor{
		nombre: nombre,
		broker: broker,
	}

}

func (c *Consumidor) Callback(mensaje string){
	fmt.Println("Consumidor " + c.nombre + " " + mensaje)
}

func (c *Consumidor) CrearCola(nombreCola string){
	c.broker.Declarar_cola(nombreCola)
}


func (c *Consumidor) Leer (nombreCola string) {
	// fmt.Println("Consumidor " + c.nombre + " leyendo")
	c.broker.Consumir(nombreCola, c.Callback)
}