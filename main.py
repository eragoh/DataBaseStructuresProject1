# project aims to sort data bases with big number of records using Natural Merge (2+1)

import random
import timeit
import sys

NUMBER_OF_RECORDS = 10000 #number of students
BYTES_TO_TAKE = 4096 # normally 4kB=4096
NUMBER_OF_DISK_SAVES = 0
NUMBER_OF_SORTING_PHASES = 0
NUMBER_OF_DISK_READS = 0
GROWING = True

def get_some_records(file):
    with open(file, "r") as f:
        records = f.read(BYTES_TO_TAKE)
        while records:
            yield records
            records = f.read(BYTES_TO_TAKE)

def get_record(file):
    global NUMBER_OF_DISK_READS
    gen = get_some_records(file)
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
            #f = filter(None, records_from_generator.split(';'))
            ll = records_from_generator.split(';')
            #ll = list(f)
            pass
            for r in ll:
                yield r
        except:
            return

def write_records_to_file(file_name, records_list):
    if records_list:
        with open(file_name, "a") as file:
            file.write(';'.join(records_list)+';')

def draw_grade():
    return random.choice(['2.0', '3.0', '3.5', '4.0', '4.5', '5.0'])

def create_record_to_file():
    f.write(str(NUMBER_OF_RECORDS)+'/'+draw_grade()+draw_grade()+draw_grade()+';')

def get_record_value(record):
    i = record.find('/')
    value = (int)(record[i+1]) + (int)(record[i+3])/10 + (int)(record[i+4])\
            + (int)(record[i+6])/10 + (int)(record[i+7]) + (int)(record[i+9])/10
    return value

# can be detached to save tape3 and to save tape1 and 2
def add_record_to_tape(tape, record):
    global NUMBER_OF_DISK_SAVES
    tape.append(record)
    if sys.getsizeof(tape1 + tape2 + tape3) >= BYTES_TO_TAKE * 0.97:
        if tapes_swapped:
            if tape2:
                write_records_to_file(tapes[0], tape2)
                tape2.clear()
                NUMBER_OF_DISK_SAVES += 1
            if tape1:
                write_records_to_file(tapes[1], tape1)
                tape1.clear()
                NUMBER_OF_DISK_SAVES += 1

        else:
            if tape1:
                write_records_to_file(tapes[0], tape1)
                tape1.clear()
                NUMBER_OF_DISK_SAVES += 1
            if tape2:
                write_records_to_file(tapes[1], tape2)
                tape2.clear()
                NUMBER_OF_DISK_SAVES += 1
        if tape3:
            write_records_to_file(tapes[2], tape3)
            tape3.clear()
            NUMBER_OF_DISK_SAVES += 1

tapes = ["tape1.txt", "tape2.txt", "tape3.txt"]
# clear tapes
for t in tapes:
    with open(t, "w") as _:
        pass

with open(tapes[2], "w") as f:
    while NUMBER_OF_RECORDS > 0:
        create_record_to_file()
        NUMBER_OF_RECORDS -= 1

# records generated, time to sort

start = timeit.default_timer()
tape1, tape2, tape3 = [], [], []
sorting = True

# SORTING
while sorting:
    tapes_swapped = False
    lval = -1 # last record value
    # clear tapes 1 and 2
    with open(tapes[0], "w") as _:
        pass
    with open(tapes[1], "w") as _:
        pass

    for record in get_record(tapes[2]): # can be done also using next(gen)
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
                add_record_to_tape(tape2, record)
                tape1, tape2 = tape2, tape1 # swap tape1 and tape2
                tapes_swapped = not tapes_swapped
            lval = val

    if tapes_swapped:
        tape1, tape2 = tape2, tape1 # unswap tape1 and tape2
        tapes_swapped = False

    # save whats left in tapes 1 and 2
    NUMBER_OF_DISK_SAVES += 2
    write_records_to_file(tapes[0], tape1)
    write_records_to_file(tapes[1], tape2)
    tape1.clear()
    tape2.clear()

    # clear tape3.txt before writing into it
    with open(tapes[2], "w") as _:
        pass

    # MERGING INTO TAPE 3

    lval = -1
    rec1, rec2 = '', ''
    tape1_gen = get_record(tapes[0])
    tape2_gen = get_record(tapes[1])
    sorting = False # CHANGE TO True IF NOT SORTED (if more than 2 runs to be merged)
    while True:
        # while insted of if because of empty strings because of .split()
        # take new record, only if previous was already used
        while rec1 == '':
            rec1 = next(tape1_gen, -1)
        while rec2 == '':
            rec2 = next(tape2_gen, -1)

        if rec1 == -1 and rec2 == -1:
            #NUMBER_OF_DISK_READS -= 1
            break
        elif rec1 == -1:
            #NUMBER_OF_DISK_READS -= 1
            add_record_to_tape(tape3, rec2)
            if get_record_value(rec2) < lval:
                sorting = True
            lval = get_record_value(rec2)
            rec2 = ''
            continue
        elif rec2 == -1:
            #NUMBER_OF_DISK_READS -= 1
            add_record_to_tape(tape3, rec1)
            if get_record_value(rec1) < lval:
                sorting = True
            lval = get_record_value(rec1)               # lval useless from here actually, can be deleted
            rec1 = ''
            continue

        if get_record_value(rec1) <= get_record_value(rec2):
            lesser, greater = rec1, rec2
        else:
            lesser, greater = rec2, rec1

        if get_record_value(lesser) >= lval:
            add_record_to_tape(tape3, lesser)
            if lesser == rec1:
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''
        elif get_record_value(greater) >= lval:
            sorting = True
            add_record_to_tape(tape3, greater)
            if greater == rec1:                     # delete used record (put '' instead)
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''
        else:
            sorting = True
            add_record_to_tape(tape3, lesser)
            if lesser == rec1:
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''

    # save whats left in tape3
    NUMBER_OF_DISK_SAVES += 1
    write_records_to_file(tapes[2], tape3)
    tape3.clear()
    NUMBER_OF_SORTING_PHASES += 1

stop = timeit.default_timer()
print(f'Time::::::{stop-start}')
print(f'Disk Saves::::::{NUMBER_OF_DISK_SAVES}')
print(f'Disk Reads::::::{NUMBER_OF_DISK_READS}')
print(f'Sorting phases::::::{NUMBER_OF_SORTING_PHASES}')
