package main

import (
	"sync"
)


func main(){
	broker := NuevoBroker()
	// consumidor1 := Consumidor{nombre: "PEPE", broker: broker}
	// consumidor2 := Consumidor{nombre: "JUAN", broker: broker}
	// productor1 := Productor{nombre: "MARIA", broker: broker}

	consumidor1 := NuevoConsumidor("PEPE", broker)
	consumidor2 := NuevoConsumidor("JUAN", broker)
	productor1 := NuevoProductor("MARIA", broker)
	


	consumidor1.CrearCola("COLA1")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumidor1.Leer("COLA1")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumidor2.Leer("COLA1")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		productor1.Publicar("COLA1", "HOLA")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		productor1.Publicar("COLA1", "ADIOS")
	}()
	wg.Wait()
	
}