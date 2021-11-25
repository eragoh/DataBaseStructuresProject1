# project aims to sort data bases with big number of records using Natural Merge

import random
import timeit

NUMBER_OF_RECORDS = 1000000 #number of students
BYTES_TO_TAKE = 2048 # 2kB
GROWING = True

def get_some_records_from_file(file):
    rest = '' # rest from the records, to be added on the start of the next run
    with open(file, "r") as f:
        records = f.read(BYTES_TO_TAKE)
        while records:
            if rest != '':
                records = rest + records
                rest = ''
            while records[-1] != ';':
                rest += records[-1]
                records = records[:-1] # delete last char
                if records == '':
                    break
            rest = rest[::-1] # reverse string
            list = records.split(';') if records else []
            yield list
            records = f.read(BYTES_TO_TAKE)

def get_record_value():
    pass # get record value

def write_records_to_file(file_name, records_list):
    if records_list:
        with open(file_name, "a") as file:
            file.write(';'.join(records_list)+';')

def draw_grade():
    return random.choice(['2.0', '2.5', '3.0', '3.5', '4.0', '4.5', '5.0'])

def create_record():
    file.write(str(NUMBER_OF_RECORDS)+','+draw_grade()+','+draw_grade()+','+draw_grade()+';')

def get_average_grade():
    return 1

file = open("records.txt", "w") #change later to binary files .code() and .decode()
while NUMBER_OF_RECORDS > 0:
    create_record()
    NUMBER_OF_RECORDS -= 1
file.close()
#records generated, time to sort
tapes = ["tape1.txt", "tape2.txt", "tape3.txt", "tape4.txt"]
#clear tapes
for t in tapes:
    with open(t, "w") as file:
        pass

start = timeit.default_timer()
tape1, tape2, tape3, tape4 = [], [], [], []
which_tape = 1
for records in get_some_records_from_file("records.txt"):
    for record in records:
        if record != '':
            if which_tape == 1:
                tape1.append(record)
                which_tape = 2
            else:
                tape2.append(record)
                which_tape = 1
    write_records_to_file(tapes[0], tape1)
    write_records_to_file(tapes[1], tape2)
    tape1, tape2 = [], []
stop = timeit.default_timer()
print(f'Time::::::{stop-start}')

#get 2kB to generator and split it to create ready records
#care for last record, since it probably won't be complete
#dispose records to tapes 1 and 2
#merge to 3 and 4
#merge to 1 and 2
#....
#check if finished


#split to split records
