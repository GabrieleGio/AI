import random
listaNumeri: list[int] = []

for i in range(0,30):
    listaNumeri.append(random.randint(1,99))

print("La lista di numeri è: ")
print(listaNumeri)

numeriSingoli = []
numeriMultipli = []

for n in listaNumeri:
    #Controllo numeri multipli e singoli
    if n not in numeriSingoli:
        numeriSingoli.append(n)
    else:
        numeriMultipli.append(n)

print(f"I numeri che appaiono più volte sono: {numeriMultipli}")


database = {}

class Persona:
    def __init__(self,id, nome, cognome, eta):
        self.id = id
        self.nome = nome
        self.cognome = cognome
        self.eta = eta
        
        try:
            if self.id not in database:
                database[self.id] = self
                print("Utente aggiunto al database")

            else:
                raise ValueError (f"Errore, c'è già un utente con ID {self.id} nel database")
            
        except ValueError as e:
            print(e)
        
    def presentazione(self):
        print(f"Ciao! Sono {self.nome} {self.cognome} e ho {self.eta} anni!")

matteo = Persona(0, "Matteo", "Rossi", 25)

matteo.presentazione()



