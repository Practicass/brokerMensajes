package main

// import "fmt"

type Productor struct{
	nombre string
	broker *Broker
}

func (p *Productor) Productor(nombre string, broker *Broker){
	p.nombre = nombre
	p.broker = broker
}

func NuevoProductor(nombre string, broker *Broker) *Productor{

	return &Productor{
		nombre: nombre,
		broker: broker,
	}

}

func (p *Productor) CrearCola(nombreCola string){
	p.broker.Declarar_cola(nombreCola)
}


func (p *Productor) Publicar(nombre string, mensaje string){
	// fmt.Println("Escribiendo")
	p.broker.Publicar(nombre, mensaje)
}