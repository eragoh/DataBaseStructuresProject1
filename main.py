import random

NUMBER_OF_RECORDS = 100 #number of students
BYTES_TO_TAKE = 40 # 2kB
GROWING = True

def get_some_records():
    with open("records.txt", "r") as f:
        records = f.read(BYTES_TO_TAKE)
        while records:
            yield records
            records = f.read(BYTES_TO_TAKE)

def get_record():
    gen = get_some_records()
    rest = '' # rest from the records, to be added on the start of the next run
    while(True):
        try:
            records_from_generator = next(gen)
            while records_from_generator[-1] != ';':
                rest += records_from_generator[-1]
                del records_from_generator[-1]

            list = records_from_generator.split(';')
            for r in list:
                yield r
        except:
            return


def draw_grade():
    return random.choice(['2.0', '2.5', '3.0', '3.5', '4.0', '4.5', '5.0'])

def write_record():
    file.write(str(NUMBER_OF_RECORDS)+','+draw_grade()+','+draw_grade()+','+draw_grade()+';')

def get_average_grade():
    return 1

file = open("records.txt", "w") #change later to binary files .code() and .decode()
while NUMBER_OF_RECORDS > 0:
    write_record()
    NUMBER_OF_RECORDS -= 1
file.close()
#records generated, time to sort

for r in get_record():
    print(r)

#get 2kB to generator and split it to create ready records
#care for last record, since it probably won't be complete
#dispose records to tapes 1 and 2
#merge to 3 and 4
#merge to 1 and 2
#....
#check if finished


#split to split records
