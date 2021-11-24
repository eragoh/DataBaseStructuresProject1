NUMBER_OF_RECORDS = 10000 #number of students

def write_record():
    index = NUMBER_OF_RECORDS


file = open("rekordy.txt") #zmienić na plik binarny potem

while NUMBER_OF_RECORDS > 0:
    write_record()
    NUMBER_OF_RECORDS -= 1

file.close()
