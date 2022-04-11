# Se importa librerias necesarias
import csv
import sys
import signal
import json
import time
import socket


def signal_handler(sig, frame):
    print("\nSe pulsó Ctrl+C!, saliendo del programa!")
    try:
        UDP_Client_Socket.close()
    except:
        pass
    else:
        print("Se cerró el cliente.")
    exit(0)


class Parser:
    def __init__(self):
        pass

    # Encapsulamiento de metodo
    def __obtain_address(self):
        # Obtengo address de archivo de exchange
        with open("./config.txt", "r", encoding="UTF8") as config_file:
            self.address = config_file.read().rstrip("\n")

    def __csv_reader(self):
        # Apertura del archivo para lectura
        with open(self.address, "r", encoding="UTF8") as csv_file:
            # Cargo archivo csv utilizando libreria de lectura modo diccionario
            self.reader = csv.DictReader(csv_file)

            self.__convert_json()

    def __convert_json(self):
        json_list = []

        # Convierto cada fila csv en un diccionario de python
        for row in self.reader:
            # Añado este diccionario de python a lista json
            json_list.append(row)

        # Convierto lista json a JSON String
        self.json_string = json.dumps(json_list)

    def parse(self):
        self.__obtain_address()
        self.__csv_reader()

        return self.json_string


class Main:
    def __init__(self):
        self.data = Parser()

    def main(self):
        signal.signal(signal.SIGINT, signal_handler)

        try:
            port = int(sys.argv[1])
        except:
            print("Puerto incorrecto")
            exit(1)

        server_address_port = ("localhost", port)

        global UDP_Client_Socket

        # Se crea socket UDP del lado del cliente
        UDP_Client_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

        print("Conectado al puerto " + str(port))

        while True:
            # Obtengo datos
            json_string = self.data.parse()

            # Envio datos por el socket
            UDP_Client_Socket.sendto(
                bytearray(json_string, "utf-8"), server_address_port
            )

            # Sleep por 30 segundos
            time.sleep(30)


m = Main()
m.main()
