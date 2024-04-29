# Define el directorio de los paquetes de cada programa
PROGRAM1_DIR=./MOM
PROGRAM2_DIR=./consumidor
PROGRAM3_DIR=./productor

# Define los objetivos (targets) del Makefile
.PHONY: MOM program1 consumidor1 consumidor2 productor

# Objetivo "all" para ejecutar todos los programas
all: MOM consumidor1 consumidor2 productor

# MOM:
# 	@echo "Ejecutando programa 1 en una nueva terminal..."
# 	gnome-terminal -- bash -c "cd $(PROGRAM1_DIR); go run MOM.go 127.0.0.1:8080; exec bash"

# # Objetivo para ejecutar el segundo programa en una nueva terminal
# consumidores:
# 	@echo "Ejecutando consumidores en una nueva terminal..."
# 	gnome-terminal -- bash -c "cd $(PROGRAM2_DIR); go run consumidor.go Juan 127.0.0.1:8080 127.0.0.1:8081 ; exec bash"

# # Objetivo para ejecutar el tercer programa en una nueva terminal
# productor:
# 	@echo "Ejecutando productor en una nueva terminal..."
# 	gnome-terminal -- bash -c "cd $(PROGRAM3_DIR); go run productor.go Pedro 127.0.0.1:8080; exec bash"

MOM:
	@echo "Ejecutando programa 1 en una nueva terminal..."
	cd $(PROGRAM1_DIR) && go run MOM.go 155.210.154.200:8084


# Objetivo para ejecutar el segundo programa en una nueva terminal
consumidor1:
	@echo "Ejecutando consumidores en una nueva terminal..."
	cd $(PROGRAM2_DIR) && go run consumidor.go Juan 155.210.154.200:8084 155.210.154.201:8081

consumidor2:
	@echo "Ejecutando consumidores en una nueva terminal..."
	cd $(PROGRAM2_DIR) && go run consumidor.go Maria 155.210.154.200:8084 155.210.154.202:8082


# Objetivo para ejecutar el tercer programa en una nueva terminal
productor:
	@echo "Ejecutando productor en una nueva terminal..."
	cd $(PROGRAM3_DIR) && go run productor.go Pedro 155.210.154.200:8084