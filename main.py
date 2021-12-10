# project aims to sort data bases with big number of records using Natural Merge

import random
import timeit
import sys

NUMBER_OF_RECORDS = 12 #number of students
BYTES_TO_TAKE = 200 # normally 4kB=4096
NUMBER_OF_DISK_SAVES = 0
NUMBER_OF_DISK_READS = -1
GROWING = True

def get_some_records():
    with open("records.txt", "r") as f:
        records = f.read(BYTES_TO_TAKE)
        while records:
            yield records
            records = f.read(BYTES_TO_TAKE)

def get_record():
    global NUMBER_OF_DISK_READS
    gen = get_some_records()
    rest = '' # rest from the records, to be added on the start of the next run
    while True:
        try:
            NUMBER_OF_DISK_READS += 1
            records_from_generator = next(gen)
            if rest != '':
                records_from_generator = rest + records_from_generator
                rest = ''
            while records_from_generator[-1] != ';':
                rest += records_from_generator[-1]
                records_from_generator = records_from_generator[:-1] # delete last char
                if records_from_generator == '':
                    break
            rest = rest[::-1] # reverse string
            list = records_from_generator.split(';')
            for r in list:
                yield r
        except:
            return


# def get_some_records_from_file(file):
#     rest = '' # rest from the records, to be added on the start of the next run
#     with open(file, "r") as f:
#         records = f.read(BYTES_TO_TAKE)
#         while records:
#             if rest != '':
#                 records = rest + records
#                 rest = ''
#             while records[-1] != ';':
#                 rest += records[-1]
#                 records = records[:-1] # delete last char
#                 if records == '':
#                     break
#             rest = rest[::-1] # reverse string
#             list = records.split(';') if records else []
#             yield list
#             records = f.read(BYTES_TO_TAKE)

def write_records_to_file(file_name, records_list):
    if records_list:
        with open(file_name, "a") as file:
            file.write(';'.join(records_list)+';')

def draw_grade():
    return random.choice(['2.0', '3.0', '3.5', '4.0', '4.5', '5.0'])

def create_record_to_file():
    file.write(str(NUMBER_OF_RECORDS)+'/'+draw_grade()+draw_grade()+draw_grade()+';')

def get_record_value(record):
    i = record.find('/')
    value = (int)(record[i+1]) + (int)(record[i+3])/10 + (int)(record[i+4])\
            + (int)(record[i+6])/10 + (int)(record[i+7]) + (int)(record[i+9])/10
    return value

def add_record_to_tape(tape, record):
    global NUMBER_OF_DISK_SAVES
    tape.append(record)
    if sys.getsizeof(tape1 + tape2 + tape3 + tape4) >= BYTES_TO_TAKE * 0.97:
        NUMBER_OF_DISK_SAVES += 1
        if tapes_swapped:
            write_records_to_file(tapes[0], tape2)
            write_records_to_file(tapes[1], tape1)
            tape1.clear()
            tape2.clear()
        else:
            write_records_to_file(tapes[0], tape1)
            write_records_to_file(tapes[1], tape2)
            tape1.clear()
            tape2.clear()


# change later to binary files .code() and .decode()
with open("records.txt", "w") as file:
    while NUMBER_OF_RECORDS > 0:
        create_record_to_file()
        NUMBER_OF_RECORDS -= 1

# records generated, time to sort
tapes = ["tape1.txt", "tape2.txt", "tape3.txt", "tape4.txt"]
# clear tapes
for t in tapes:
    with open(t, "w") as file:
        pass

start = timeit.default_timer()
tape1, tape2, tape3, tape4 = [], [], [], []
tapes_swapped = False
lval = -1 # last record value

# SORTING

for record in get_record(): # can be done also using next(gen)
    if record != '':
        # INITIAL SORT - disposition from main tape to two tapes
        # count value of record
        val = get_record_value(record)
        # compare with last record in the tape
        # if greater or equal, write to the same tape
        # otherwise, write to another tape
        if val >= lval:
            add_record_to_tape(tape1, record)
        else:
            tape2.append(record)
            add_record_to_tape(tape2, record)
            tape1, tape2 = tape2, tape1 # swap tape1 and tape2
            tapes_swapped = not tapes_swapped
        lval = val

stop = timeit.default_timer()
print(f'Time::::::{stop-start}')
print(f'Disk Saves::::::{NUMBER_OF_DISK_SAVES}')
print(f'Disk Reads::::::{NUMBER_OF_DISK_READS}')

#get 2kB to generator and split it to create ready records
#care for last record, since it probably won't be complete
#dispose records to tapes 1 and 2
#merge to 3 and 4
#merge to 1 and 2
#....
#check if finished


#split to split records
